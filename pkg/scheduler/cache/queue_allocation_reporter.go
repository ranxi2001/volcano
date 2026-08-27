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
	"slices"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1beta1apply "volcano.sh/apis/pkg/client/applyconfiguration/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/allocationreporting"
	"volcano.sh/volcano/pkg/scheduler/api"
)

type queueAllocationReporter struct {
	cache *SchedulerCache
	ring  *allocationreporting.FixedRing
}

func newQueueAllocationReporter(sc *SchedulerCache, ring *allocationreporting.FixedRing) QueueAllocationReporter {
	return &queueAllocationReporter{cache: sc, ring: ring}
}

// QueueAllocationReporter returns the configured fixed-ring reporter, or nil
// when the scheduler is using the legacy Queue status writer.
func (sc *SchedulerCache) QueueAllocationReporter() QueueAllocationReporter {
	return sc.queueAllocationReporter
}

func (r *queueAllocationReporter) Ready() bool {
	if len(r.cache.registeredHandlers) == 0 {
		return false
	}
	for _, handler := range r.cache.registeredHandlers {
		if !handler.HasSynced() {
			return false
		}
	}
	return true
}

func (r *queueAllocationReporter) Owns(job *api.JobInfo, task *api.TaskInfo) bool {
	if job == nil || job.PodGroup == nil || task == nil || task.Pod == nil {
		return false
	}
	if !slices.Contains(r.cache.schedulerNames, task.Pod.Spec.SchedulerName) {
		return false
	}
	return responsibleForPodGroup(job.PodGroup, r.cache.schedulerPodName, r.cache.c)
}

func (r *queueAllocationReporter) Apply(queue *api.QueueInfo, allocated v1.ResourceList) error {
	if queue == nil || queue.Queue == nil {
		return fmt.Errorf("queue must not be nil")
	}
	if allocated == nil {
		allocated = v1.ResourceList{}
	}

	if current, found := queue.Queue.Status.SchedulerAllocations[r.ring.ReporterID]; found &&
		current.RingID == r.ring.RingID &&
		current.RingGeneration == r.ring.RingGeneration &&
		current.MemberIndex == r.ring.MemberIndex &&
		current.ExpectedMembers == r.ring.ExpectedMembers &&
		equality.Semantic.DeepEqual(current.Allocated, allocated) {
		return nil
	}

	report := v1beta1apply.SchedulerAllocation().
		WithRingID(r.ring.RingID).
		WithRingGeneration(r.ring.RingGeneration).
		WithMemberIndex(r.ring.MemberIndex).
		WithExpectedMembers(r.ring.ExpectedMembers).
		WithAllocated(allocated.DeepCopy())
	status := v1beta1apply.QueueStatus().WithSchedulerAllocations(
		map[string]v1beta1apply.SchedulerAllocationApplyConfiguration{
			r.ring.ReporterID: *report,
		},
	)
	queueApply := v1beta1apply.Queue(queue.Name).WithStatus(status)
	_, err := r.cache.vcClient.SchedulingV1beta1().Queues().ApplyStatus(
		context.Background(),
		queueApply,
		metav1.ApplyOptions{FieldManager: r.ring.FieldManager, Force: true},
	)
	if err != nil {
		return fmt.Errorf("apply Queue allocation report for %s: %w", queue.Name, err)
	}
	return nil
}
