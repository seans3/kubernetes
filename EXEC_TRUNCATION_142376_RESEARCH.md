# Exec/attach output truncation with slow clients (issue #142376)

Research notes, written 2026-10-05 against `kubernetes/kubernetes` master at
`205d4973415`. This branch exists to carry the research between machines. It is
not intended as a PR: it contains a temporary test harness.

Line numbers below refer to that commit.

## Summary

- [#142376](https://github.com/kubernetes/kubernetes/issues/142376) is a real
  bug: `kubectl exec`, `kubectl attach`, `kubectl cp` and `crictl exec` lose the
  end of the output when the client reads more slowly than the command writes.
- The bug predates KEP-4006. It originates in the SPDY-era CRI streaming server,
  which writes the exit status and closes the connection while output is still
  in the socket's send buffer.
- KEP-4006 did not cause it, but the WebSocket-to-SPDY translator
  (`StreamTranslatorHandler`) repeats the same close pattern, so it is a second,
  independent loss point. This was confirmed with an isolated test (below).
- On WebSocket the truncation is loud (exit 1, `connection reset by peer`). On
  SPDY it is silent (exit 0, empty stderr).
- PR [#142461](https://github.com/kubernetes/kubernetes/pull/142461) fixes the
  translator hop in the isolated test. It is open and under discussion.

## The reported issue

Reporter's setup: kind v1.37.0, two nodes, kubectl v1.37.0, containerd v2.3.4.
The pod runs `head -c 33554432 /dev/zero` (32 MiB) and the client reads at
1 MiB/s.

| Path | Truncated | Of those, exit 0 and empty stderr |
|---|---|---|
| kubectl exec, WebSocket (default) | 3/5 | 0 |
| kubectl exec, SPDY | 5/5 | 5 |
| crictl exec on the node, same reader | 5/5 | 5 |
| crictl exec on the node, unthrottled reader | 0/5 | |
| kubectl exec, process sleeps 45s after last write | 0/1 | |

Only the tail is missing; the start of the stream is intact.

Independent confirmation in the thread (thc1006, four-node kubeadm v1.37.0,
20 runs per cell):

- SPDY silently short in 55 of 80 runs.
- `crictl exec` silently short in 65 of 80 runs.
- WebSocket lost data in 80 of 80 runs, every one with exit 1.
- All 160 control runs with a 45s sleep after the last write were complete.
- Median share received in truncated runs: 73–94% for SPDY, 89–96% for crictl,
  45–80% for WebSocket.
- Congestion control mattered on one node: bbr 20/20 silently short, cubic 1/20.

herefindalex reproduced the silent SPDY truncation on localhost with only
`cri-streaming`'s `ServeExec` and the client-go SPDY executor, with no
containerd, kubelet or apiserver involved.

## The data path

```
[kubectl (client-go)]
   |  WebSocket (v5.channel.k8s.io) or SPDY (v4.channel.k8s.io)
   v
[kube-apiserver]   upgrade-aware proxy, or WebSocket->SPDY translator
   |  WebSocket (v5, with ExtendWebSocketsToKubelet) or SPDY (v4)
   v
[kubelet]          upgrade-aware proxy, or WebSocket->SPDY translator
   |  SPDY (v4.channel.k8s.io)
   v
[CRI streaming server (containerd / CRI-O, vendoring k8s.io/cri-streaming)]
   |  pipes
   v
[process in the container]
```

Three configurations:

1. **SPDY end to end.** kubectl speaks SPDY; the apiserver and kubelet are raw
   upgrade-aware proxies; the runtime's streaming server terminates SPDY.
2. **WebSocket, translator in the apiserver.** kubectl speaks v5 WebSocket to
   the apiserver, which translates to SPDY toward the kubelet
   (`pkg/registry/core/pod/rest/subresources.go:152` and `:227`).
3. **WebSocket, translator in the kubelet.** With `ExtendWebSocketsToKubelet`
   (beta, default on since 1.36) the apiserver passes the WebSocket upgrade
   through and the kubelet runs the same translator
   (`pkg/kubelet/server/server.go:1161` and `:1198`).

## Root cause

The mechanism is the same at every loss point:

1. The reader is slow, so backpressure fills the socket buffers on every hop.
   Several megabytes of output sit in kernel send and receive buffers.
2. The command exits. The server writes the status and closes the TCP socket
   immediately. The kernel keeps delivering the buffered output in the
   background.
3. The client sends another frame. No stdin is needed: both clients send a ping
   every 5 seconds. The kernel answers data arriving on a closed socket with a
   reset and discards the output it had not yet delivered.

That is why the loss depends on how long the tail takes to drain: if everything
drains before the next client frame arrives, nothing is lost.

### Loss point 1: CRI streaming server (not KEP-4006 code)

`staging/src/k8s.io/cri-streaming/pkg/streaming/remotecommand/exec.go:85`:

```go
defer ctx.conn.Close()

err := executor.ExecInContainer(...)
...
_ = ctx.writeStatus(...)
```

`ServeAttach` does the same (`attach.go:44`). For SPDY, `connection.Close`
(`staging/src/k8s.io/streaming/pkg/httpstream/spdy/connection.go:107`) resets
every stream and closes the underlying connection.

This is why `crictl exec` alone truncates. containerd release/2.2 has its own
copy of this server in `internal/cri/streamingserver` and behaves the same way.

### Loss point 2: silent success in client-go (not KEP-4006 code)

`staging/src/k8s.io/client-go/tools/remotecommand/errorstream.go:49`:

```go
case len(message) > 0:
    errorChan <- d.decode(message)
default:
    errorChan <- nil
```

An error stream that ends with no status is treated as success. When the SPDY
connection is torn down, stdout ends with EOF and the status is missing, so the
client exits 0 with truncated output.

### Loss point 3: the WebSocket translator (KEP-4006 code)

`staging/src/k8s.io/apiserver/pkg/util/proxy/streamtranslator.go:72`:

```go
defer websocketStreams.conn.Close()
```

It runs immediately after the final `writeStatus`. `wsstream.Conn.Close` calls
`golang.org/x/net/websocket`'s `Conn.Close`
(`vendor/golang.org/x/net/websocket/websocket.go:234`), which writes a close
frame and then closes the socket without waiting for the client's close frame.

The client-go WebSocket heartbeat then triggers the reset:
`staging/src/k8s.io/client-go/tools/remotecommand/websocket.go:59` sets
`pingPeriod = 5 * time.Second`.

The same handler runs in the kubelet with `ExtendWebSocketsToKubelet`, so both
placements are affected.

### Other places worth knowing about

- **Upgrade-aware proxy.**
  `staging/src/k8s.io/apimachinery/pkg/util/proxy/upgradeaware.go:450` returns,
  and closes both connections, as soon as either copy direction finishes. This
  was raised in the thread as a possible further loss point. It was not
  isolated in a test here or there.
- **Write deadline on the status.**
  `staging/src/k8s.io/apiserver/pkg/util/proxy/websocket.go:194` sets a 10s
  write deadline before the status write, but `Conn.write`
  (`staging/src/k8s.io/streaming/pkg/httpstream/wsstream/conn.go:393`) calls
  `resetTimeout()`, which overwrites it with the 4h idle timeout. PR
  [#142596](https://github.com/kubernetes/kubernetes/pull/142596) fixes this.

## Did this exist before KEP-4006?

Yes. The underlying bug lives in the SPDY path and has nothing to do with
WebSockets.

Evidence:

- [#60140](https://github.com/kubernetes/kubernetes/issues/60140) reported the
  same symptom in 2018, years before KEP-4006 (alpha in 1.29). It was closed as
  not reproducible; the attempts used fast local readers.
- `crictl exec` on the node truncates, and that path involves no apiserver, no
  kubelet and no KEP-4006 code.
- With WebSockets turned off (`KUBECTL_REMOTE_COMMAND_WEBSOCKETS=false`),
  `kubectl exec` still truncates, silently.
- The reporter reproduced it on k3s 1.30 through 1.36. Which transport each of
  those runs used was not checked.

What KEP-4006 changed:

- **Worse:** it added a second place where the same mistake happens. On the
  WebSocket path there are two independent loss points in series, the runtime
  and the translator. Fixing only the runtime does not fix WebSocket clients.
  The containerd issue says this directly: behind an older kubelet, SPDY is
  fixed but WebSocket is still cut off by the kubelet's translator.
- **Better:** WebSocket truncation is loud (exit 1), where SPDY truncation is
  silent (exit 0).

Nothing was run against a pre-4006 release during this research. The claim that
the bug is old rests on the 2018 issue, the `crictl` result and reading the
code.

## Test evidence for the translator loss point

### Harness

`staging/src/k8s.io/apiserver/pkg/util/proxy/tmp_issue142376_slowclient_repro_test.go`

It has three parts:

- **Upstream:** a fake SPDY server that is deliberately well behaved. It writes
  the payload to stdout, writes a success status, half-closes its streams, and
  waits for the translator to close the connection. Any loss therefore comes
  from the translator hop.
- **Middle:** a real `StreamTranslatorHandler`.
- **Client:** the real client-go `WebSocketExecutor`, with a stdout writer
  throttled to a set rate.

Environment: Linux 7.0, loopback, no TLS, cubic,
`tcp_wmem = 4096 16384 4194304`, `tcp_rmem = 4096 131072 33554432`.

### Commands

```bash
cd staging/src/k8s.io/apiserver
REPRO_PAYLOAD=8388608 REPRO_RATE=131072 REPRO_RUNS=3 \
  go test ./pkg/util/proxy/ -run TestTmpIssue142376TranslatorSlowClient -v -count=1 -timeout 20m
```

`REPRO_PAYLOAD` is bytes written by the upstream, `REPRO_RATE` is the reader's
bytes per second, `REPRO_RUNS` is the number of slow runs. Each invocation also
does one unthrottled control run first. The test only logs results; it does not
fail on truncation.

### Results

| Code | Payload | Reader | Result |
|---|---|---|---|
| master | 8 MiB | unthrottled | complete, no error |
| master | 8 MiB | 128 KiB/s | truncated 3/3: 71.6%, 78.5%, 70.2% received |
| master | 64 MiB | 1 MiB/s | complete 2/2 |
| PR #142461 (`fce8f6619ea`) | 8 MiB | 128 KiB/s | complete 3/3, no error |

Error on the truncated runs, matching the issue's WebSocket output:

```
error reading from error stream: read message: read tcp ...: read: connection reset by peer
```

Timing on the truncated runs (seconds after start):

| Run | Translator handler returned | Client saw reset |
|---|---|---|
| 0 | 44.0 | 46.1 |
| 1 | 49.3 | 50.5 |
| 2 | 44.0 | 45.2 |

The resets follow the first 5-second boundary after the handler returned (45,
50, 45), which fits the heartbeat ping as the trigger. This is inferred from
timing; no packet capture was taken.

The 64 MiB at 1 MiB/s run did not truncate because, on loopback, only about 2
seconds of data remained buffered when the translator closed (handler returned
at 65.0s, client finished at 66.9s). No ping landed in that window. Real
networks and TLS hold more in flight, which matches the higher loss rates in the
issue.

With PR #142461 the handler returned at 64.1s and the client at 64.3s: the
server now waits for the client instead of closing first.

## Wider findings in the KEP-4006 code

- **The translator can turn upstream truncation into explicit success.** It
  uses the client-go SPDY executor toward the runtime. If the runtime cuts the
  SPDY leg short, `StreamWithContext` returns nil (loss point 2) and
  `streamtranslator.go:148` writes a `Success` status to the WebSocket client.
  This is read from the code and was not tested.
- **The KEP does not specify server-side termination.** `v5.channel.k8s.io`
  adds only the client-to-server `CLOSE` signal, for stdin half-close. Nothing
  says how the server ends a session so that buffered output is delivered. The
  test plan has no slow-consumer case.
- **This is GA-bound code.** `kep.yaml` lists stage `stable` with
  `latest-milestone: v1.38`.
- **The status write deadline does not hold** (PR #142596, described above). If
  that PR merges without the close fix, a slow client could lose the status
  frame after 10s, so the two should be sequenced.

## State of the upstream discussion (as of 2026-10-05)

- **Issue #142376:** open, `needs-triage`, labelled sig/node, sig/api-machinery
  and sig/cli.
- **PR #142461** (semx), open: "exec/attach: let the client close the connection
  so no output is lost".
  - The CRI streaming server and the translator send the status, signal the end
    (SPDY: close the output streams; WebSocket: a close frame through a new
    `wsstream.Conn.CloseWrite`), then wait for the client to close.
  - The wait is bounded by the request context and 15 minutes.
  - client-go treats an empty status on v4/v5 as an error.
- **PR #142596** (semx), open: "wsstream: keep a write deadline across writes".
- **containerd/containerd#14275**, open: port needed for release/2.2, which has
  its own copy of the streaming server.
- **aojea's position:** this is a graceful-termination problem across several
  proxied hops and components. It needs a KEP defining the protocol change,
  per-component rollout and compatibility matrix, with KEP-4006 as the model.
  Holding sessions open is a resource-exhaustion risk. exec, attach and cp are
  debugging tools, not production data paths.
- **semx's response:** agreed to write the KEP. Measurements in the thread show
  500 ms cuts even a fast LAN client and 30 s cuts a 256 KiB/s reader, and that
  a client can already hold a session for 4h today.
- **Backport plan in the thread:** cherry-picks to 1.37 and 1.36, manual
  backport to 1.35, then dependency bumps or ports in containerd and CRI-O.

## Recommendation

Split the translator and `wsstream` change out of #142461 and land it under
KEP-4006.

- It is not a wire-protocol change. The server completes the standard WebSocket
  closing handshake instead of closing the socket first, and existing clients
  already echo the close frame.
- Version skew is benign: a new server works with old clients, and an old server
  with a new client behaves as today.
- The cri-streaming change and the client-go "empty status is an error" change
  carry the real skew and rollout questions. They can follow the KEP path aojea
  asked for.

## Open questions

- **The wait bound.** 15 minutes is generous. A bound tied to drain progress
  would answer the resource-holding concern but is more work, and progress is
  only directly observable on Linux.
- **Sequencing with #142596**, as noted above.
- **The upgrade-aware proxy** as a further loss point, which nobody has
  isolated.
- **Port-forward** (`streamtunnel.go`) was not examined.
- **A slow-consumer test** for the KEP-4006 test plan before GA.

## Verified versus inferred

Verified by running code here:

- The translator hop alone truncates a slow WebSocket client on master.
- PR #142461 removes that truncation in the same harness.
- A fast reader is unaffected.

Read from code, not tested here:

- The CRI streaming server and client-go loss points (independently reproduced
  by others in the issue thread).
- The translator reporting success after upstream truncation.
- The write-deadline override (PR #142596 has its own test).

Inferred:

- The heartbeat ping as the trigger for the reset, from timing.
- That the bug predates KEP-4006, from #60140, the `crictl` result and the code.

## Files on this branch

- `EXEC_TRUNCATION_142376_RESEARCH.md`: this report.
- `staging/src/k8s.io/apiserver/pkg/util/proxy/tmp_issue142376_slowclient_repro_test.go`:
  the harness. Temporary and hand-written; not for merging.
- `EXEC_TRUNCATION_142376_INPUTS/`: sources as fetched on 2026-10-05.
  - `issue-142376-body.txt`, `issue-142376-comments.txt`
  - `pr-142461-description.txt`, `pr-142461.diff`
  - `pr-142596-description.txt`, `pr-142596.diff`
  - `containerd-issue-14275.txt`
  - `kep-4006-README.md`, `kep-4006-kep.yaml`
