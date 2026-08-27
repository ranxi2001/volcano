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
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/features"
)

type allocationCohort struct {
	ringID          string
	ringGeneration  string
	expectedMembers int32
}

type cohortReports struct {
	reports map[int32]schedulingv1beta1.SchedulerAllocation
	invalid bool
}

type jsonPatchOperation struct {
	Op    string      `json:"op"`
	Path  string      `json:"path"`
	Value interface{} `json:"value,omitempty"`
}

const queueAllocationAggregatorFieldManager = "volcano-queue-allocation-aggregator"

func desiredQueueAllocation(
	queue *schedulingv1beta1.Queue,
	authoritativeRingID string,
	authoritativeRingMembers int32,
) (v1.ResourceList, *schedulingv1beta1.QueueAllocationReportingStatus, bool) {
	if queue == nil || authoritativeRingID == "" || authoritativeRingMembers <= 0 {
		return nil, nil, false
	}

	cohorts := make(map[allocationCohort]*cohortReports)
	for reporterID, report := range queue.Status.SchedulerAllocations {
		if report.RingID != authoritativeRingID || report.ExpectedMembers != authoritativeRingMembers ||
			report.MemberIndex < 0 || report.MemberIndex >= report.ExpectedMembers {
			continue
		}
		cohort := allocationCohort{
			ringID:          report.RingID,
			ringGeneration:  report.RingGeneration,
			expectedMembers: report.ExpectedMembers,
		}
		state := cohorts[cohort]
		if state == nil {
			state = &cohortReports{reports: make(map[int32]schedulingv1beta1.SchedulerAllocation)}
			cohorts[cohort] = state
		}
		expectedReporterID := fmt.Sprintf("%s/%d", report.RingID, report.MemberIndex)
		if reporterID != expectedReporterID {
			state.invalid = true
			continue
		}
		if _, found := state.reports[report.MemberIndex]; found {
			state.invalid = true
			continue
		}
		state.reports[report.MemberIndex] = report
	}

	if queue.Status.AllocationReporting != nil {
		active := allocationCohort{
			ringID:          queue.Status.AllocationReporting.RingID,
			ringGeneration:  queue.Status.AllocationReporting.RingGeneration,
			expectedMembers: queue.Status.AllocationReporting.ExpectedMembers,
		}
		if active.ringID == authoritativeRingID && active.expectedMembers == authoritativeRingMembers {
			if allocated, reporting, complete := aggregateCompleteCohort(active, cohorts[active]); complete {
				return allocated, reporting, true
			}
			delete(cohorts, active)
		}
	}

	var selected *allocationCohort
	for cohort, reports := range cohorts {
		if !cohortComplete(cohort, reports) {
			continue
		}
		if selected != nil {
			return nil, nil, false
		}
		candidate := cohort
		selected = &candidate
	}
	if selected == nil {
		return nil, nil, false
	}
	return aggregateCompleteCohort(*selected, cohorts[*selected])
}

func cohortComplete(cohort allocationCohort, reports *cohortReports) bool {
	if reports == nil || reports.invalid || int32(len(reports.reports)) != cohort.expectedMembers {
		return false
	}
	for index := int32(0); index < cohort.expectedMembers; index++ {
		if _, found := reports.reports[index]; !found {
			return false
		}
	}
	return true
}

func aggregateCompleteCohort(
	cohort allocationCohort,
	reports *cohortReports,
) (v1.ResourceList, *schedulingv1beta1.QueueAllocationReportingStatus, bool) {
	if !cohortComplete(cohort, reports) {
		return nil, nil, false
	}

	allocated := v1.ResourceList{}
	for _, report := range reports.reports {
		for resourceName, quantity := range report.Allocated {
			current := allocated[resourceName]
			current.Add(quantity)
			if current.IsZero() {
				delete(allocated, resourceName)
			} else {
				allocated[resourceName] = current
			}
		}
	}
	reporting := &schedulingv1beta1.QueueAllocationReportingStatus{
		RingID:          cohort.ringID,
		RingGeneration:  cohort.ringGeneration,
		ExpectedMembers: cohort.expectedMembers,
	}
	return allocated, reporting, true
}

func (c *queuecontroller) syncQueueAllocationReporting(
	queue *schedulingv1beta1.Queue,
) (*schedulingv1beta1.Queue, error) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.QueueAllocationReporting) {
		return queue, nil
	}
	allocated, reporting, complete := desiredQueueAllocation(
		queue, c.authoritativeAllocationRingID, c.authoritativeAllocationRingMembers)
	if !complete || (equality.Semantic.DeepEqual(queue.Status.Allocated, allocated) &&
		equality.Semantic.DeepEqual(queue.Status.AllocationReporting, reporting)) {
		return queue, nil
	}

	patch := []jsonPatchOperation{
		{Op: "test", Path: "/metadata/resourceVersion", Value: queue.ResourceVersion},
		{Op: "add", Path: "/status/allocated", Value: allocated},
		{Op: "add", Path: "/status/allocationReporting", Value: reporting},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return nil, fmt.Errorf("marshal Queue allocation status patch: %w", err)
	}
	updated, err := c.vcClient.SchedulingV1beta1().Queues().Patch(
		context.Background(), queue.Name, types.JSONPatchType, patchBytes, metav1.PatchOptions{
			FieldManager: queueAllocationAggregatorFieldManager,
		}, "status")
	if err != nil {
		return nil, fmt.Errorf("patch Queue %s allocation status: %w", queue.Name, err)
	}
	return updated, nil
}
