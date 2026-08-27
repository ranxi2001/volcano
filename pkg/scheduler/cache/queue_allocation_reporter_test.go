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

package cache

import (
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kcache "k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
	"stathat.com/c/consistent"

	"volcano.sh/apis/pkg/apis/scheduling"
	vcv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	vcfake "volcano.sh/apis/pkg/client/clientset/versioned/fake"
	"volcano.sh/volcano/pkg/scheduler/allocationreporting"
	"volcano.sh/volcano/pkg/scheduler/api"
)

func findHashKey(t *testing.T, ring *consistent.Consistent, owner string) string {
	t.Helper()
	for index := 0; index < 10000; index++ {
		key := fmt.Sprintf("job-%d", index)
		got, err := ring.Get(key)
		if err != nil {
			t.Fatalf("ring.Get(%q): %v", key, err)
		}
		if got == owner {
			return key
		}
	}
	t.Fatalf("failed to find key owned by %s", owner)
	return ""
}

func TestQueueAllocationReporterOwnsWorkloadPartition(t *testing.T) {
	ring := consistent.New()
	ring.Add("volcano-scheduler-0")
	ring.Add("volcano-scheduler-1")
	localOwner := findHashKey(t, ring, "volcano-scheduler-0")
	foreignOwner := findHashKey(t, ring, "volcano-scheduler-1")

	sc := &SchedulerCache{
		schedulerNames: []string{"volcano"},
		multiSchedulerInfo: multiSchedulerInfo{
			schedulerPodName: "volcano-scheduler-0",
			c:                ring,
		},
	}
	reporter := &queueAllocationReporter{cache: sc, ring: &allocationreporting.FixedRing{}}
	makeJob := func(owner string) *api.JobInfo {
		return &api.JobInfo{PodGroup: &api.PodGroup{PodGroup: scheduling.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pg",
				OwnerReferences: []metav1.OwnerReference{{
					Name:       owner,
					Controller: ptr.To(true),
				}},
			},
		}}}
	}
	task := &api.TaskInfo{Pod: &v1.Pod{Spec: v1.PodSpec{SchedulerName: "volcano"}}}

	if !reporter.Owns(makeJob(localOwner), task) {
		t.Fatal("local workload was not owned")
	}
	if reporter.Owns(makeJob(foreignOwner), task) {
		t.Fatal("foreign workload was owned")
	}
	task.Pod.Spec.SchedulerName = "other"
	if reporter.Owns(makeJob(localOwner), task) {
		t.Fatal("foreign scheduler workload was owned")
	}
}

func TestQueueAllocationReporterWaitsForInitialSync(t *testing.T) {
	sc := &SchedulerCache{}
	reporter := &queueAllocationReporter{cache: sc, ring: &allocationreporting.FixedRing{}}
	if reporter.Ready() {
		t.Fatal("reporter was ready without registered handlers")
	}

	registration := &mockHandlerRegistration{}
	sc.registeredHandlers = map[string]kcache.ResourceEventHandlerRegistration{"pod": registration}
	if reporter.Ready() {
		t.Fatal("reporter was ready before handler synchronization")
	}
	registration.synced = true
	if !reporter.Ready() {
		t.Fatal("reporter was not ready after handler synchronization")
	}
}

func TestQueueAllocationReporterApply(t *testing.T) {
	ring := &allocationreporting.FixedRing{
		RingID:          "volcano-system/volcano-scheduler",
		RingGeneration:  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ReporterID:      "volcano-system/volcano-scheduler/0",
		FieldManager:    "volcano-scheduler-allocation-test",
		MemberIndex:     0,
		ExpectedMembers: 2,
	}
	client := vcfake.NewSimpleClientset(&vcv1beta1.Queue{ObjectMeta: metav1.ObjectMeta{Name: "q1"}})
	sc := &SchedulerCache{vcClient: client}
	reporter := &queueAllocationReporter{cache: sc, ring: ring}
	queue := api.NewQueueInfo(&scheduling.Queue{ObjectMeta: metav1.ObjectMeta{Name: "q1"}})

	err := reporter.Apply(queue, v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")})
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	updated, err := client.SchedulingV1beta1().Queues().Get(context.Background(), "q1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Queue: %v", err)
	}
	report, found := updated.Status.SchedulerAllocations[ring.ReporterID]
	if !found {
		t.Fatalf("report %q not found: %#v", ring.ReporterID, updated.Status.SchedulerAllocations)
	}
	if report.Allocated.Cpu().Value() != 2 || report.RingGeneration != ring.RingGeneration {
		t.Fatalf("unexpected report: %#v", report)
	}
}
