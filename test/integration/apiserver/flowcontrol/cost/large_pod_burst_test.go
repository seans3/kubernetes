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

package cost

import (
	"fmt"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/ktesting"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

func drainAllocatorPool() {
	runtime.GC()
	runtime.GC()
}

func BenchmarkLargePodBurst(b *testing.B) {
	featuregatetesting.SetFeatureGatesDuringTest(b, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
		features.AllocatorPoolBufferCap:   true,
		features.DetectCacheInconsistency: false,
		features.WatchList:                true,
	})

	tCtx := ktesting.Init(b)
	tCtx, server, informer := setupAPIServer(tCtx)
	ensureNamespace(tCtx, ephemeralNamespace)

	kubeletStatusPayloads := [2][]byte{
		[]byte(`{"status":{"phase":"Running","podIP":"10.244.7.83","podIPs":[{"ip":"10.244.7.83"}],"conditions":[{"type":"Ready","status":"True","reason":"ContainersReady","lastTransitionTime":"2026-04-23T12:24:00Z"}],"containerStatuses":[{"name":"main","ready":true,"started":true,"restartCount":0,"image":"registry.k8s.io/pause:3.9","imageID":"registry.k8s.io/pause@sha256:1234","containerID":"containerd://main0","state":{"running":{"startedAt":"2026-04-23T12:24:00Z"}}}]}}`),
		[]byte(`{"status":{"phase":"Running","podIP":"10.244.7.83","podIPs":[{"ip":"10.244.7.83"}],"conditions":[{"type":"Ready","status":"True","reason":"ContainersReady","lastTransitionTime":"2026-04-23T12:24:00Z"}],"containerStatuses":[{"name":"main","ready":true,"started":true,"restartCount":0,"image":"registry.k8s.io/pause:3.9","imageID":"registry.k8s.io/pause@sha256:1234","containerID":"containerd://main1","state":{"running":{"startedAt":"2026-04-23T12:24:00Z"}}}]}}`),
	}

	podSizes := []struct {
		label      string
		extraBytes int
	}{
		{label: "45KB", extraBytes: 35 * 1024},
		{label: "85KB", extraBytes: 75 * 1024},
	}

	caps := []struct {
		label string
		bytes int
	}{
		{label: "256KiB", bytes: 256 * 1024},
		{label: "32KiB", bytes: 32 * 1024},
	}

	const poolCount = 64

	for _, ps := range podSizes {
		b.Run("PodSize="+ps.label, func(b *testing.B) {
			seedPods(tCtx, informer, ephemeralNamespace, poolCount, ps.extraBytes)
			defer clearNamespace(tCtx, informer, ephemeralNamespace)

			paths := make([]string, poolCount)
			perPodStep := make([]atomic.Uint64, poolCount)
			for i := range poolCount {
				paths[i] = fmt.Sprintf("/api/v1/namespaces/%s/pods/pod-%03d/status?fieldManager=kubelet", ephemeralNamespace, i)
			}

			// 1. Concurrent committed kubelet PATCH /status burst across distinct >32KB pods
			for _, capCfg := range caps {
				b.Run("KubeletPatchStatus_CommittedBurst/Cap="+capCfg.label, func(b *testing.B) {
					kruntime.SetMaxPooledBufferCapacity(capCfg.bytes)
					drainAllocatorPool()

					var workerSeq atomic.Uint64
					var respBytes atomic.Int64
					var startMem, endMem runtime.MemStats
					runtime.ReadMemStats(&startMem)
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						myPod := int((workerSeq.Add(1) - 1) % uint64(poolCount))
						for pb.Next() {
							step := perPodStep[myPod].Add(1)
							n := runHTTP(b, server, http.MethodPatch, paths[myPod], string(types.StrategicMergePatchType), acceptProtobuf, kubeletStatusPayloads[step%2])
							respBytes.Add(int64(n))
						}
					})
					b.StopTimer()
					runtime.ReadMemStats(&endMem)

					elapsedSec := b.Elapsed().Seconds()
					if elapsedSec > 0 {
						b.ReportMetric(float64(b.N)/elapsedSec, "patches/s")
					}
					b.ReportMetric(float64(respBytes.Load())/float64(b.N), "resp-B/op")
					b.ReportMetric(float64(endMem.NumGC-startMem.NumGC), "gc-cycles")
				})
			}

			// 2. Concurrent dryRun kubelet PATCH /status burst (pure CPU/serialization bound, no etcd disk I/O)
			dryRunPaths := make([]string, poolCount)
			for i := range poolCount {
				dryRunPaths[i] = paths[i] + "&dryRun=All"
			}
			for _, capCfg := range caps {
				b.Run("KubeletPatchStatus_DryRunBurst/Cap="+capCfg.label, func(b *testing.B) {
					kruntime.SetMaxPooledBufferCapacity(capCfg.bytes)
					drainAllocatorPool()

					runHTTP(b, server, http.MethodPatch, dryRunPaths[0], string(types.StrategicMergePatchType), acceptProtobuf, kubeletStatusPayloads[0])

					var workerSeq atomic.Uint64
					var startMem, endMem runtime.MemStats
					runtime.ReadMemStats(&startMem)
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						myPod := int((workerSeq.Add(1) - 1) % uint64(poolCount))
						step := 0
						for pb.Next() {
							runHTTP(b, server, http.MethodPatch, dryRunPaths[myPod], string(types.StrategicMergePatchType), acceptProtobuf, kubeletStatusPayloads[step%2])
							step++
						}
					})
					b.StopTimer()
					runtime.ReadMemStats(&endMem)

					elapsedSec := b.Elapsed().Seconds()
					if elapsedSec > 0 {
						b.ReportMetric(float64(b.N)/elapsedSec, "patches/s")
					}
					b.ReportMetric(float64(endMem.NumGC-startMem.NumGC), "gc-cycles")
				})
			}

			// 3. Full Scheduling Cycle Burst (POST create -> POST /binding -> PATCH /status -> DELETE)
			// Pre-encode per-worker pod create & binding bodies so we only measure kube-apiserver cost.
			createPath := fmt.Sprintf("/api/v1/namespaces/%s/pods?fieldManager=controller", ephemeralNamespace)
			payload := make([]byte, ps.extraBytes)
			for i := range payload {
				payload[i] = 'x'
			}
			payloadStr := string(payload)

			createBodies := make([][]byte, poolCount)
			bindBodies := make([][]byte, poolCount)
			bindPaths := make([]string, poolCount)
			statusPaths := make([]string, poolCount)
			delPaths := make([]string, poolCount)
			for i := range poolCount {
				podName := fmt.Sprintf("sched-pod-%03d", i)
				pod := exemplarPod.DeepCopy()
				pod.Namespace = ephemeralNamespace
				pod.Name = podName
				pod.Annotations["bench.k8s.io/payload"] = payloadStr
				createBodies[i] = encodeObject(b, acceptProtobuf, pod)

				bindObj := &v1.Binding{
					ObjectMeta: metav1.ObjectMeta{Namespace: ephemeralNamespace, Name: podName},
					Target:     v1.ObjectReference{Kind: "Node", Name: "kind-worker0"},
				}
				bindBodies[i] = encodeObject(b, acceptProtobuf, bindObj)
				bindPaths[i] = fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/binding?fieldManager=kube-scheduler", ephemeralNamespace, podName)
				statusPaths[i] = fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/status?fieldManager=kubelet", ephemeralNamespace, podName)
				delPaths[i] = fmt.Sprintf("/api/v1/namespaces/%s/pods/%s?gracePeriodSeconds=0", ephemeralNamespace, podName)
			}

			for _, capCfg := range caps {
				b.Run("SchedulingLifecycle_CommittedBurst/Cap="+capCfg.label, func(b *testing.B) {
					kruntime.SetMaxPooledBufferCapacity(capCfg.bytes)
					drainAllocatorPool()

					var workerSeq atomic.Uint64
					var startMem, endMem runtime.MemStats
					runtime.ReadMemStats(&startMem)
					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						mySlot := int((workerSeq.Add(1) - 1) % uint64(poolCount))
						for pb.Next() {
							// Step 1: Create >32KB unbound pod (Protobuf request & response)
							runHTTP(b, server, http.MethodPost, createPath, acceptProtobuf, acceptProtobuf, createBodies[mySlot])
							// Step 2: Scheduler POST /binding (Protobuf request & response)
							runHTTP(b, server, http.MethodPost, bindPaths[mySlot], acceptProtobuf, acceptProtobuf, bindBodies[mySlot])
							// Step 3: Kubelet PATCH /status (JSON StrategicMergePatch request, Protobuf response)
							runHTTP(b, server, http.MethodPatch, statusPaths[mySlot], string(types.StrategicMergePatchType), acceptProtobuf, kubeletStatusPayloads[0])
							// Step 4: Delete pod (gracePeriod=0) so slot can be reused immediately
							runHTTP(b, server, http.MethodDelete, delPaths[mySlot], "", acceptProtobuf, nil)
						}
					})
					b.StopTimer()
					runtime.ReadMemStats(&endMem)

					elapsedSec := b.Elapsed().Seconds()
					if elapsedSec > 0 {
						b.ReportMetric(float64(b.N)/elapsedSec, "pods/s")
					}
					b.ReportMetric(float64(endMem.NumGC-startMem.NumGC), "gc-cycles")
				})
			}
		})
	}
}
