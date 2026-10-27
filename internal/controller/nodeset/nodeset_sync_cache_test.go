// SPDX-FileCopyrightText: Copyright (C) SchedMD LLC.
// SPDX-License-Identifier: Apache-2.0

package nodeset

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	slurmapi "github.com/SlinkyProject/slurm-client/api/v0044"
	slurmclient "github.com/SlinkyProject/slurm-client/pkg/client"
	slurminterceptor "github.com/SlinkyProject/slurm-client/pkg/client/interceptor"
	"github.com/SlinkyProject/slurm-client/pkg/client/token"
	slurmerrors "github.com/SlinkyProject/slurm-client/pkg/errors"
	slurmobject "github.com/SlinkyProject/slurm-client/pkg/object"

	slinkyv1beta1 "github.com/SlinkyProject/slurm-operator/api/v1beta1"
	"github.com/SlinkyProject/slurm-operator/internal/utils/podutils"
)

func TestAutoPruningWaitsForNewWorkerRegistration(t *testing.T) {
	nodeset := newNodeSet("workers", "slurm", 1)
	nodeset.Spec.PruneSlurmNodeRecords = slinkyv1beta1.NodeSetPruneNodeRecordTypeAuto
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "workers-0", Namespace: nodeset.Namespace,
			Labels: map[string]string{
				slinkyv1beta1.LabelNodeSetSlurmNodeNameMode: string(slinkyv1beta1.SlurmNodeNameModeKubernetesNode),
				slinkyv1beta1.LabelNodeSetPodHostname:       "worker-a",
			},
		},
		Spec:   corev1.PodSpec{NodeName: "worker-a", Hostname: "workers-0"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	kubeClient := fake.NewFakeClient(nodeset, pod)
	slurmClient := newFakeClientList(slurminterceptor.Funcs{}, &slurmapi.V0044NodeObjectList{})
	reconciler := newNodeSetController(kubeClient, newClientMap("slurm", slurmClient))
	require.NoError(t, reconciler.syncSlurmNodeRecords(t.Context(), nodeset, []*corev1.Pod{pod}))
	require.NoError(t, kubeClient.Get(t.Context(), client.ObjectKeyFromObject(pod), &corev1.Pod{}))
}

func TestProcessCondemnedNewRegistrationWithRealCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	var mu sync.Mutex
	nodes := []slurmapi.V0044Node{{
		Name: ptr.To("worker-a"), State: ptr.To([]slurmapi.V0044NodeState{slurmapi.V0044NodeStateIDLE}),
	}}
	lists, drains := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if request.Method == http.MethodGet && request.URL.Path == "/slurm/v0.0.44/nodes/" {
			lists++
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"nodes": nodes}))
			return
		}
		name := strings.Trim(strings.TrimPrefix(request.URL.Path, "/slurm/v0.0.44/node/"), "/")
		for i := range nodes {
			if ptr.Deref(nodes[i].Name, "") != name {
				continue
			}
			if request.Method == http.MethodPost {
				update := slurmapi.V0044UpdateNodeMsg{}
				if !assert.NoError(t, json.NewDecoder(request.Body).Decode(&update)) || !assert.NotNil(t, update.State) {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				assert.Equal(t, []slurmapi.V0044UpdateNodeMsgState{slurmapi.V0044UpdateNodeMsgStateDRAIN}, *update.State)
				nodes[i].State = ptr.To([]slurmapi.V0044NodeState{slurmapi.V0044NodeStateDRAIN})
				drains++
			}
			assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"nodes": nodes[i : i+1]}))
			return
		}
		assert.NoError(t, json.NewEncoder(w).Encode(map[string]any{"nodes": []slurmapi.V0044Node{}}))
	}))
	defer server.Close()
	slurmClient, err := slurmclient.NewClient(&slurmclient.Config{
		Server: server.URL, HTTPClient: server.Client(), TokenProvider: token.StaticProvider("unused"),
	}, &slurmclient.ClientOptions{
		CacheSyncPeriod: time.Hour,
		EnableFor:       []slurmobject.Object{&slurmapi.V0044Node{}},
	})
	require.NoError(t, err)
	go slurmClient.Start(ctx)
	defer slurmClient.Stop()
	initial := &slurmapi.V0044NodeObjectList{}
	informer := slurmClient.GetInformer(slurmapi.ObjectTypeV0044Node)
	require.Eventually(t, informer.HasStarted, 2*time.Second, 5*time.Millisecond)
	require.NoError(t, slurmClient.List(ctx, initial, &slurmclient.ListOptions{RefreshCache: true}))
	mu.Lock()
	nodes = append(nodes, slurmapi.V0044Node{
		Name: ptr.To("worker-b"), State: ptr.To([]slurmapi.V0044NodeState{slurmapi.V0044NodeStateIDLE}),
	})
	initialLists := lists
	mu.Unlock()
	require.ErrorIs(t, slurmClient.Get(ctx, "worker-b", &slurmapi.V0044Node{}), slurmerrors.ErrNotFound)
	require.NoError(t, slurmClient.Get(ctx, "worker-b", &slurmapi.V0044Node{}, &slurmclient.GetOptions{SkipCache: true}))
	nodeset := newNodeSet("workers", "slurm", 0)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "workers-1", Namespace: nodeset.Namespace,
			Labels: map[string]string{
				slinkyv1beta1.LabelNodeSetSlurmNodeNameMode: string(slinkyv1beta1.SlurmNodeNameModeKubernetesNode),
				slinkyv1beta1.LabelNodeSetPodHostname:       "worker-b",
			},
		},
		Spec:   corev1.PodSpec{NodeName: "worker-b", Hostname: "workers-1"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	kubeClient := fake.NewFakeClient(nodeset, pod)
	reconciler := newNodeSetController(kubeClient, newClientMap("slurm", slurmClient))
	require.NoError(t, reconciler.processCondemned(ctx, nodeset, []*corev1.Pod{pod}, 0))
	current := &corev1.Pod{}
	require.NoError(t, kubeClient.Get(ctx, client.ObjectKeyFromObject(pod), current))
	require.True(t, podutils.IsPodCordon(current))
	mu.Lock()
	actualDrains, actualLists := drains, lists
	mu.Unlock()
	require.Equal(t, 1, actualDrains)
	require.Equal(t, initialLists, actualLists, "scale-down must not force a full synchronous cache refresh")
	require.NoError(t, reconciler.processCondemned(ctx, nodeset, []*corev1.Pod{current}, 0))
	require.True(t, apierrors.IsNotFound(kubeClient.Get(ctx, client.ObjectKeyFromObject(pod), &corev1.Pod{})))
}
