/*
Copyright 2025 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

// TestActivatorShutdownWithKeepAliveConnections validates that the activator's
// shutdown sequence properly handles keep-alive connections during graceful
// termination.
//
// This test reproduces the root cause of the flaky TestActivatorHAGraceful e2e
// test (SRVKS-1368). The activator creates multiple h2c servers (http1, h2c,
// profile). When clients maintain persistent keep-alive connections with
// ongoing request/response cycles (as the e2e test's prober goroutine does),
// the current shutdown sequence has two problems:
//
//  1. It does NOT call server.SetKeepAlivesEnabled(false) before Shutdown, so
//     keep-alive clients are not signaled to disconnect before the drain begins.
//  2. It shuts down servers SERIALLY with server.Shutdown(context.Background()),
//     which means each server must fully drain before the next one starts
//     draining. When connections hold servers open, the total drain time is the
//     SUM of all individual drain times.
//
// Together these cause the activator pod's shutdown to exceed the test's
// WaitForPodDeleted timeout (480s < TerminationGracePeriodSeconds 600s).
//
// The fix (PR #1766) adds server.SetKeepAlivesEnabled(false) before
// Shutdown() to signal keep-alive clients to disconnect, and parallelizes
// the shutdown of multiple servers so the total drain time is the MAX
// (not SUM) of individual drain times.
//
// Without the fix: this test FAILS (serial shutdown blocks too long).
// With the fix: this test PASSES (parallel shutdown + keep-alive disable).
func TestActivatorShutdownWithKeepAliveConnections(t *testing.T) {
	const (
		numServers = 3
		// Each server's active connection takes this long to drain after
		// its Shutdown is initiated (simulating a keep-alive client that
		// holds the connection open during the drain period).
		perServerDrainTime = 4 * time.Second
		// With serial shutdown: total = numServers * perServerDrainTime = 12s
		// With parallel shutdown: total = perServerDrainTime = 4s
		// So a 7-second deadline fails serial but passes parallel.
		shutdownDeadline = 7 * time.Second
	)

	type serverInfo struct {
		srv    *http.Server
		ln     net.Listener
		cancel context.CancelFunc
	}
	srvInfos := make([]serverInfo, numServers)

	for i := 0; i < numServers; i++ {
		// Each server gets its own cancel context so it can independently
		// control its long-lived connections.
		streamCtx, cancel := context.WithCancel(context.Background())

		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/stream" {
				// Simulate a persistent keep-alive connection from the
				// prober: the handler sends periodic data, keeping the
				// HTTP connection in StateActive. This prevents
				// server.Shutdown from completing until the stream ends.
				flusher, ok := w.(http.Flusher)
				if !ok {
					http.Error(w, "streaming unsupported", http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Connection", "keep-alive")
				w.WriteHeader(http.StatusOK)
				flusher.Flush()

				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-streamCtx.Done():
						return
					case <-r.Context().Done():
						return
					case <-ticker.C:
						if _, err := fmt.Fprintf(w, "data: keepalive\n\n"); err != nil {
							return
						}
						flusher.Flush()
					}
				}
			}
			w.WriteHeader(http.StatusOK)
			fmt.Fprintln(w, "ok")
		})

		// Create h2c server matching the activator's pkgnet.NewServer setup.
		srv := &http.Server{
			Handler:           h2c.NewHandler(handler, &http2.Server{}),
			ReadHeaderTimeout: time.Minute,
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Server %d: listen failed: %v", i, err)
		}

		// Register an OnShutdown callback that signals the streaming
		// handler to stop after a simulated drain delay. This models the
		// real behavior where keep-alive connections drain gradually
		// after receiving the shutdown signal.
		srv.RegisterOnShutdown(func() {
			// Simulate the time it takes for keep-alive clients to
			// notice the shutdown signal and close their connections.
			time.Sleep(perServerDrainTime)
			cancel()
		})

		go srv.Serve(ln)
		srvInfos[i] = serverInfo{srv: srv, ln: ln, cancel: cancel}
	}

	// Verify all servers work and establish streaming keep-alive connections.
	var clientWg sync.WaitGroup
	for i, si := range srvInfos {
		addr := si.ln.Addr().String()
		// Verify basic connectivity.
		resp, err := http.Get("http://" + addr)
		if err != nil {
			t.Fatalf("Server %d: request failed: %v", i, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		// Start a streaming keep-alive connection (like the e2e prober).
		clientWg.Add(1)
		go func(idx int, a string) {
			defer clientWg.Done()
			resp, err := http.Get("http://" + a + "/stream")
			if err != nil {
				return
			}
			defer resp.Body.Close()
			io.Copy(io.Discard, resp.Body)
		}(i, addr)
	}

	// Let streaming connections stabilize.
	time.Sleep(500 * time.Millisecond)

	// Build the servers map matching the activator's shutdown interface.
	servers := make(map[string]*http.Server, numServers)
	for i, si := range srvInfos {
		servers[fmt.Sprintf("srv%d", i)] = si.srv
	}

	// Test shutdown. Current shutdownServers calls server.Shutdown serially.
	// Each server's OnShutdown callback fires when ITS Shutdown is called,
	// but the drain delay means each takes ~4 seconds to fully close.
	//
	// Serial (current): total ≈ 3 × 4s = 12s > 7s deadline → FAIL
	// Parallel (fix):   total ≈ max(4s, 4s, 4s) = 4s < 7s → PASS
	//
	// Additionally, SetKeepAlivesEnabled(false) from the fix closes idle
	// connections before Shutdown fires, reducing the drain load.
	shutdownDone := make(chan struct{})
	go func() {
		shutdownServers(servers)
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		t.Log("shutdownServers completed within deadline")
	case <-time.After(shutdownDeadline):
		t.Fatal("FAIL: shutdownServers blocked for >" + shutdownDeadline.String() +
			" due to serial shutdown of servers with active keep-alive connections. " +
			"The activator shutdown must: (1) call server.SetKeepAlivesEnabled(false) " +
			"to signal keep-alive clients to disconnect, and (2) shut down servers " +
			"in parallel so total drain time is max(individual) not sum(individual). " +
			"See SRVKS-1368 and PR #1766.")
	}

	// Clean up.
	for _, si := range srvInfos {
		si.cancel()
		si.ln.Close()
	}
	clientWg.Wait()
}
