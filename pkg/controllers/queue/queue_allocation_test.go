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

package queue

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/util/workqueue"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	vcfake "volcano.sh/apis/pkg/client/clientset/versioned/fake"
	"volcano.sh/volcano/pkg/controllers/apis"
	"volcano.sh/volcano/pkg/controllers/framework"
	"volcano.sh/volcano/pkg/features"
)

const (
	testRingID      = "volcano-system/volcano-scheduler"
	testRingMembers = int32(2)
	testGeneration  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
)

func makeReport(index, members int32, allocated v1.ResourceList) schedulingv1beta1.SchedulerAllocation {
	return schedulingv1beta1.SchedulerAllocation{
		RingID:          testRingID,
		RingGeneration:  testGeneration,
		MemberIndex:     index,
		ExpectedMembers: members,
		Allocated:       allocated,
	}
}

func makeCompleteQueue() *schedulingv1beta1.Queue {
	return &schedulingv1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "q1", ResourceVersion: "1"},
		Status: schedulingv1beta1.QueueStatus{
			State: schedulingv1beta1.QueueStateOpen,
			SchedulerAllocations: map[string]schedulingv1beta1.SchedulerAllocation{
				testRingID + "/0": makeReport(0, 2, v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("500m"),
				}),
				testRingID + "/1": makeReport(1, 2, v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("1"),
				}),
			},
		},
	}
}

func TestDesiredQueueAllocation(t *testing.T) {
	queue := makeCompleteQueue()
	allocated, reporting, complete := desiredQueueAllocation(queue, testRingID, testRingMembers)
	if !complete {
		t.Fatal("expected complete cohort")
	}
	if got := allocated.Cpu().MilliValue(); got != 1500 {
		t.Fatalf("CPU = %dm, want 1500m", got)
	}
	if reporting.RingID != testRingID || reporting.RingGeneration != testGeneration || reporting.ExpectedMembers != 2 {
		t.Fatalf("unexpected active reporting status: %#v", reporting)
	}

	delete(queue.Status.SchedulerAllocations, testRingID+"/1")
	if _, _, complete := desiredQueueAllocation(queue, testRingID, testRingMembers); complete {
		t.Fatal("incomplete cohort was activated")
	}
}

func TestDesiredQueueAllocationRejectsInconsistentCohort(t *testing.T) {
	queue := makeCompleteQueue()
	report := queue.Status.SchedulerAllocations[testRingID+"/1"]
	report.RingGeneration = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	queue.Status.SchedulerAllocations[testRingID+"/1"] = report
	if _, _, complete := desiredQueueAllocation(queue, testRingID, testRingMembers); complete {
		t.Fatal("mixed generation cohort was activated")
	}

	queue = makeCompleteQueue()
	queue.Status.SchedulerAllocations[testRingID+"/other"] = makeReport(1, 2, nil)
	if _, _, complete := desiredQueueAllocation(queue, testRingID, testRingMembers); complete {
		t.Fatal("duplicate member index cohort was activated")
	}
}

func TestDesiredQueueAllocationUsesAuthoritativeMemberCount(t *testing.T) {
	queue := makeCompleteQueue()
	wrong := makeReport(0, 1, v1.ResourceList{v1.ResourceCPU: resource.MustParse("100")})
	wrong.RingGeneration = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	queue.Status.SchedulerAllocations[testRingID+"/0"] = wrong

	if _, _, complete := desiredQueueAllocation(queue, testRingID, testRingMembers); complete {
		t.Fatal("self-declared one-member cohort was activated for a two-member authoritative ring")
	}
}

func TestDesiredQueueAllocationKeepsActiveCohort(t *testing.T) {
	queue := makeCompleteQueue()
	queue.Status.AllocationReporting = &schedulingv1beta1.QueueAllocationReportingStatus{
		RingID:          testRingID,
		RingGeneration:  testGeneration,
		ExpectedMembers: 2,
	}
	queue.Status.SchedulerAllocations["other/0"] = schedulingv1beta1.SchedulerAllocation{
		RingID:          "other",
		RingGeneration:  "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		MemberIndex:     0,
		ExpectedMembers: 1,
		Allocated:       v1.ResourceList{v1.ResourceCPU: resource.MustParse("100")},
	}
	allocated, _, complete := desiredQueueAllocation(queue, testRingID, testRingMembers)
	if !complete || allocated.Cpu().MilliValue() != 1500 {
		t.Fatalf("active cohort changed: complete=%v allocated=%v", complete, allocated)
	}
}

func TestDesiredQueueAllocationTransitionsCompleteGeneration(t *testing.T) {
	queue := makeCompleteQueue()
	queue.Status.AllocationReporting = &schedulingv1beta1.QueueAllocationReportingStatus{
		RingID:          testRingID,
		RingGeneration:  testGeneration,
		ExpectedMembers: testRingMembers,
	}
	newGeneration := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	report := queue.Status.SchedulerAllocations[testRingID+"/0"]
	report.RingGeneration = newGeneration
	report.Allocated = v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}
	queue.Status.SchedulerAllocations[testRingID+"/0"] = report

	if _, _, complete := desiredQueueAllocation(queue, testRingID, testRingMembers); complete {
		t.Fatal("partial replacement generation was activated")
	}

	report = queue.Status.SchedulerAllocations[testRingID+"/1"]
	report.RingGeneration = newGeneration
	report.Allocated = v1.ResourceList{v1.ResourceCPU: resource.MustParse("3")}
	queue.Status.SchedulerAllocations[testRingID+"/1"] = report
	allocated, reporting, complete := desiredQueueAllocation(queue, testRingID, testRingMembers)
	if !complete || allocated.Cpu().Value() != 5 || reporting.RingGeneration != newGeneration {
		t.Fatalf("complete replacement generation was not activated: complete=%v allocated=%v reporting=%#v", complete, allocated, reporting)
	}
}

func TestSyncQueueAllocationReportingPreservesOtherStatus(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.QueueAllocationReporting, true)
	queue := makeCompleteQueue()
	queue.Status.Reservation = schedulingv1beta1.Reservation{
		Nodes: []string{"node-1"},
	}
	client := vcfake.NewSimpleClientset(queue.DeepCopy())
	stored, err := client.SchedulingV1beta1().Queues().Get(context.Background(), queue.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Queue: %v", err)
	}
	controller := &queuecontroller{
		vcClient:                           client,
		authoritativeAllocationRingID:      testRingID,
		authoritativeAllocationRingMembers: testRingMembers,
	}
	updated, err := controller.syncQueueAllocationReporting(stored)
	if err != nil {
		t.Fatalf("syncQueueAllocationReporting() error = %v", err)
	}
	if updated.Status.Allocated.Cpu().MilliValue() != 1500 {
		t.Fatalf("CPU = %s", updated.Status.Allocated.Cpu().String())
	}
	if updated.Status.State != schedulingv1beta1.QueueStateOpen || len(updated.Status.Reservation.Nodes) != 1 {
		t.Fatalf("unrelated status changed: %#v", updated.Status)
	}
}

func TestSyncQueueAllocationReportingClearsAggregate(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.QueueAllocationReporting, true)
	queue := makeCompleteQueue()
	queue.Status.Allocated = v1.ResourceList{v1.ResourceCPU: resource.MustParse("10")}
	for reporterID, report := range queue.Status.SchedulerAllocations {
		report.Allocated = v1.ResourceList{}
		queue.Status.SchedulerAllocations[reporterID] = report
	}
	client := vcfake.NewSimpleClientset(queue.DeepCopy())
	stored, err := client.SchedulingV1beta1().Queues().Get(context.Background(), queue.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Queue: %v", err)
	}
	controller := &queuecontroller{
		vcClient:                           client,
		authoritativeAllocationRingID:      testRingID,
		authoritativeAllocationRingMembers: testRingMembers,
	}
	updated, err := controller.syncQueueAllocationReporting(stored)
	if err != nil {
		t.Fatalf("syncQueueAllocationReporting() error = %v", err)
	}
	if len(updated.Status.Allocated) != 0 {
		t.Fatalf("Allocated was not cleared: %#v", updated.Status.Allocated)
	}
}

func TestQueueControllerRequiresAuthoritativeRing(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.QueueAllocationReporting, true)
	controller := &queuecontroller{}
	if err := controller.Initialize(&framework.ControllerOption{}); err == nil {
		t.Fatal("Initialize() expected authoritative ring error")
	}
	controller.authoritativeAllocationRingID = testRingID
	if err := controller.Initialize(&framework.ControllerOption{}); err == nil {
		t.Fatal("Initialize() expected authoritative member count error")
	}
}

func TestUpdateQueueTriggersOnlyForReportingChanges(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.QueueAllocationReporting, true)
	controller := &queuecontroller{
		queue:                              workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[*apis.Request]()),
		authoritativeAllocationRingID:      testRingID,
		authoritativeAllocationRingMembers: testRingMembers,
	}
	t.Cleanup(controller.queue.ShutDown)

	oldQueue := makeCompleteQueue()
	allocated, reporting, complete := desiredQueueAllocation(oldQueue, testRingID, testRingMembers)
	if !complete {
		t.Fatal("test cohort is incomplete")
	}
	oldQueue.Status.Allocated = allocated
	oldQueue.Status.AllocationReporting = reporting

	unrelatedStatus := oldQueue.DeepCopy()
	unrelatedStatus.Status.State = schedulingv1beta1.QueueStateClosing
	controller.updateQueue(oldQueue, unrelatedStatus)
	if got := controller.queue.Len(); got != 0 {
		t.Fatalf("unrelated status update enqueued %d items", got)
	}

	reportChanged := oldQueue.DeepCopy()
	report := reportChanged.Status.SchedulerAllocations[testRingID+"/0"]
	report.Allocated[v1.ResourceCPU] = resource.MustParse("2")
	reportChanged.Status.SchedulerAllocations[testRingID+"/0"] = report
	controller.updateQueue(oldQueue, reportChanged)
	if got := controller.queue.Len(); got != 1 {
		t.Fatalf("report update enqueued %d items, want 1", got)
	}
}

func TestUpdateQueueIgnoresReportsWhenFeatureDisabled(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.QueueAllocationReporting, false)
	controller := &queuecontroller{
		queue: workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[*apis.Request]()),
	}
	t.Cleanup(controller.queue.ShutDown)

	oldQueue := makeCompleteQueue()
	newQueue := oldQueue.DeepCopy()
	report := newQueue.Status.SchedulerAllocations[testRingID+"/0"]
	report.Allocated[v1.ResourceCPU] = resource.MustParse("2")
	newQueue.Status.SchedulerAllocations[testRingID+"/0"] = report
	controller.updateQueue(oldQueue, newQueue)
	if got := controller.queue.Len(); got != 0 {
		t.Fatalf("disabled reporting enqueued %d items", got)
	}
}

func TestUpdateQueueRepairsWrongAggregate(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.QueueAllocationReporting, true)
	controller := &queuecontroller{
		queue:                              workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[*apis.Request]()),
		authoritativeAllocationRingID:      testRingID,
		authoritativeAllocationRingMembers: testRingMembers,
	}
	t.Cleanup(controller.queue.ShutDown)

	oldQueue := makeCompleteQueue()
	allocated, reporting, complete := desiredQueueAllocation(oldQueue, testRingID, testRingMembers)
	if !complete {
		t.Fatal("test cohort is incomplete")
	}
	oldQueue.Status.Allocated = allocated
	oldQueue.Status.AllocationReporting = reporting
	wrongAggregate := oldQueue.DeepCopy()
	wrongAggregate.Status.Allocated = v1.ResourceList{v1.ResourceCPU: resource.MustParse("100")}

	controller.updateQueue(oldQueue, wrongAggregate)
	if got := controller.queue.Len(); got != 1 {
		t.Fatalf("wrong aggregate enqueued %d items, want 1", got)
	}
}
