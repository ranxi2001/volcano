/*
Copyright 2026 The Volcano Authors.

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

package framework

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
)

func TestCalculateQueueAllocationsFiltersAndIncludesParents(t *testing.T) {
	leaf := &scheduling.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "leaf"},
		Spec:       scheduling.QueueSpec{Parent: "root"},
	}
	root := &scheduling.Queue{ObjectMeta: metav1.ObjectMeta{Name: "root"}}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod", Namespace: "ns", UID: "pod-uid"},
		Spec: v1.PodSpec{
			NodeName: "node-1",
			Containers: []v1.Container{{
				Name: "main",
				Resources: v1.ResourceRequirements{Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("2"),
				}},
			}},
		},
		Status: v1.PodStatus{Phase: v1.PodRunning},
	}
	task := api.NewTaskInfo(pod)
	job := api.NewJobInfo("ns/job")
	job.Queue = "leaf"
	job.AddTaskInfo(task)
	ssn := &Session{
		Jobs: map[api.JobID]*api.JobInfo{job.UID: job},
		Queues: map[api.QueueID]*api.QueueInfo{
			"leaf": api.NewQueueInfo(leaf),
			"root": api.NewQueueInfo(root),
		},
		Nodes: map[string]*api.NodeInfo{},
	}

	allocations := calculateQueueAllocations(ssn, func(_ *api.JobInfo, _ *api.TaskInfo) bool { return true })
	leafAllocation := allocations["leaf"]
	if got := leafAllocation.Cpu().Value(); got != 2 {
		t.Fatalf("leaf CPU = %d", got)
	}
	rootAllocation := allocations["root"]
	if got := rootAllocation.Cpu().Value(); got != 2 {
		t.Fatalf("root CPU = %d", got)
	}

	filtered := calculateQueueAllocations(ssn, func(_ *api.JobInfo, _ *api.TaskInfo) bool { return false })
	filteredLeaf := filtered["leaf"]
	filteredRoot := filtered["root"]
	if !filteredLeaf.Cpu().IsZero() || !filteredRoot.Cpu().IsZero() {
		t.Fatalf("filtered allocation is not zero: %#v", filtered)
	}
}
