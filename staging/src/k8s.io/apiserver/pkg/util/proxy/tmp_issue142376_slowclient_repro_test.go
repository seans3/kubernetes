/*
Copyright The Kubernetes Authors.

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

package proxy

// TEMPORARY, HAND-WRITTEN, NOT FOR COMMIT.
// Throwaway harness for researching kubernetes/kubernetes#142376: does the
// WebSocket->SPDY StreamTranslatorHandler lose the tail of stdout by itself
// when the WebSocket client reads slowly? The upstream SPDY server here is
// deliberately well behaved (half-closes its streams and waits for the
// translator to close), so any loss is attributable to the translator hop.
// Delete after use.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	rcconstants "k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/streaming/pkg/httpstream/spdy"
)

// throttledCounter counts bytes and sleeps so that it accepts at most
// bytesPerSec (0 means unthrottled).
type throttledCounter struct {
	bytesPerSec int64
	got         atomic.Int64
}

func (w *throttledCounter) Write(p []byte) (int, error) {
	w.got.Add(int64(len(p)))
	if w.bytesPerSec > 0 {
		time.Sleep(time.Duration(int64(len(p)) * int64(time.Second) / w.bytesPerSec))
	}
	return len(p), nil
}

func runIssue142376(t *testing.T, payloadSize int, readerBytesPerSec int64) (int64, error, time.Duration) {
	t.Helper()
	upstreamSawClientClose := make(chan bool, 1)
	spdyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if _, err := httpstream.Handshake(req, w, []string{rcconstants.StreamProtocolV4Name}); err != nil {
			t.Errorf("handshake: %v", err)
			return
		}
		streamCh := make(chan httpstream.Stream, 4)
		conn := spdy.NewResponseUpgrader().UpgradeResponse(w, req, func(stream httpstream.Stream, replySent <-chan struct{}) error {
			streamCh <- stream
			return nil
		})
		if conn == nil {
			t.Errorf("upgrade failed")
			return
		}
		defer conn.Close()
		var stdoutStream, errorStream httpstream.Stream
		for stdoutStream == nil || errorStream == nil {
			select {
			case s := <-streamCh:
				switch s.Headers().Get(v1.StreamType) {
				case v1.StreamTypeStdout:
					stdoutStream = s
				case v1.StreamTypeError:
					errorStream = s
				}
			case <-time.After(30 * time.Second):
				t.Errorf("timed out waiting for streams")
				return
			}
		}
		chunk := make([]byte, 32*1024)
		for sent := 0; sent < payloadSize; sent += len(chunk) {
			if _, err := stdoutStream.Write(chunk); err != nil {
				t.Errorf("upstream stdout write failed after %d bytes: %v", sent, err)
				return
			}
		}
		if err := v4WriteStatusFunc(errorStream)(&apierrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusSuccess}}); err != nil {
			t.Errorf("upstream writeStatus failed: %v", err)
		}
		// Well-behaved upstream: half-close (FIN) every stream, then wait for the
		// SPDY client (the translator) to close the connection.
		_ = stdoutStream.Close()
		_ = errorStream.Close()
		select {
		case <-conn.CloseChan():
			upstreamSawClientClose <- true
		case <-time.After(5 * time.Minute):
			upstreamSawClientClose <- false
		}
	}))
	defer spdyServer.Close()

	spdyLocation, err := url.Parse(spdyServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	spdyTransport, err := fakeTransport()
	if err != nil {
		t.Fatal(err)
	}
	translatorDone := make(chan time.Time, 1)
	streamTranslator := NewStreamTranslatorHandler(spdyLocation, spdyTransport, 0, Options{Stdout: true})
	streamTranslatorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		streamTranslator.ServeHTTP(w, req)
		translatorDone <- time.Now()
	}))
	defer streamTranslatorServer.Close()

	streamTranslatorLocation, err := url.Parse(streamTranslatorServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	exec, err := remotecommand.NewWebSocketExecutor(&rest.Config{Host: streamTranslatorLocation.Host}, "GET", streamTranslatorServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	stdout := &throttledCounter{bytesPerSec: readerBytesPerSec}
	start := time.Now()
	streamErr := exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{Stdout: stdout})
	elapsed := time.Since(start)

	select {
	case ok := <-upstreamSawClientClose:
		if !ok {
			t.Errorf("upstream never saw the translator close the SPDY connection")
		}
	case <-time.After(30 * time.Second):
		t.Errorf("upstream handler did not finish")
	}
	select {
	case at := <-translatorDone:
		t.Logf("translator handler returned %.1fs after start; client returned after %.1fs", at.Sub(start).Seconds(), elapsed.Seconds())
	case <-time.After(30 * time.Second):
		t.Errorf("translator handler did not finish")
	}
	return stdout.got.Load(), streamErr, elapsed
}

func TestTmpIssue142376TranslatorSlowClient(t *testing.T) {
	payload := 16 * 1024 * 1024
	if v := os.Getenv("REPRO_PAYLOAD"); v != "" {
		payload, _ = strconv.Atoi(v)
	}
	rate := int64(512 * 1024)
	if v := os.Getenv("REPRO_RATE"); v != "" {
		rate, _ = strconv.ParseInt(v, 10, 64)
	}
	runs := 3
	if v := os.Getenv("REPRO_RUNS"); v != "" {
		runs, _ = strconv.Atoi(v)
	}

	got, err, d := runIssue142376(t, payload, 0)
	t.Logf("RESULT fast reader: got=%d/%d err=%v elapsed=%.1fs", got, payload, err, d.Seconds())

	for i := 0; i < runs; i++ {
		got, err, d := runIssue142376(t, payload, rate)
		t.Logf("RESULT slow reader (%d B/s) run %d: got=%d/%d (%.1f%%) truncated=%v err=%v elapsed=%.1fs",
			rate, i, got, payload, 100*float64(got)/float64(payload), got != int64(payload), err, d.Seconds())
	}
}
