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

package queueallocation

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	v1beta1apply "volcano.sh/apis/pkg/client/applyconfiguration/scheduling/v1beta1"
	vcclient "volcano.sh/apis/pkg/client/clientset/versioned"
)

func TestQueueAllocationReporterSSA(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Skip("KUBEBUILDER_ASSETS is not set")
	}
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	crdDirectory := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../../..", "config/crd/volcano/bases"))
	testEnvironment := &envtest.Environment{
		BinaryAssetsDirectory:    assets,
		CRDDirectoryPaths:        []string{crdDirectory},
		ErrorIfCRDPathMissing:    true,
		AttachControlPlaneOutput: false,
	}
	config, err := testEnvironment.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnvironment.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})

	ctx := context.Background()
	client := vcclient.NewForConfigOrDie(config)
	created, err := client.SchedulingV1beta1().Queues().Create(ctx, &schedulingv1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "queue-reporting"},
		Spec:       schedulingv1beta1.QueueSpec{Weight: 1},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create Queue: %v", err)
	}
	created.Status.State = schedulingv1beta1.QueueStateOpen
	created.Status.Reservation.Nodes = []string{"node-1"}
	created.Status.Allocated = v1.ResourceList{
		v1.ResourceCPU:                     resource.MustParse("10"),
		v1.ResourceName("example.com/old"): resource.MustParse("1"),
	}
	if _, err := client.SchedulingV1beta1().Queues().UpdateStatus(ctx, created, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("write legacy Queue status: %v", err)
	}

	const (
		ringID           = "volcano-system/volcano-scheduler"
		generation       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		aggregateManager = "volcano-queue-allocation-aggregator"
	)
	applyReport := func(queueName, manager, reporterID string, index int32, allocated v1.ResourceList) {
		t.Helper()
		report := v1beta1apply.SchedulerAllocation().
			WithRingID(ringID).
			WithRingGeneration(generation).
			WithMemberIndex(index).
			WithExpectedMembers(2).
			WithAllocated(allocated)
		status := v1beta1apply.QueueStatus().WithSchedulerAllocations(
			map[string]v1beta1apply.SchedulerAllocationApplyConfiguration{reporterID: *report},
		)
		_, err := client.SchedulingV1beta1().Queues().ApplyStatus(
			ctx,
			v1beta1apply.Queue(queueName).WithStatus(status),
			metav1.ApplyOptions{FieldManager: manager, Force: true},
		)
		if err != nil {
			t.Fatalf("apply report %s: %v", reporterID, err)
		}
	}

	if _, err := client.SchedulingV1beta1().Queues().Create(ctx, &schedulingv1beta1.Queue{
		ObjectMeta: metav1.ObjectMeta{Name: "queue-fresh"},
		Spec:       schedulingv1beta1.QueueSpec{Weight: 1},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create fresh Queue: %v", err)
	}
	applyReport("queue-fresh", "fresh-reporter", ringID+"/0", 0, v1.ResourceList{})
	fresh, err := client.SchedulingV1beta1().Queues().Get(ctx, "queue-fresh", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get fresh Queue: %v", err)
	}
	if _, found := fresh.Status.SchedulerAllocations[ringID+"/0"]; !found {
		t.Fatalf("initial empty report was not persisted: %#v", fresh.Status)
	}

	applyReport("queue-reporting", "reporter-0", ringID+"/0", 0, v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")})
	applyReport("queue-reporting", "reporter-1", ringID+"/1", 1, v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")})
	queue, err := client.SchedulingV1beta1().Queues().Get(ctx, "queue-reporting", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Queue: %v", err)
	}
	if len(queue.Status.SchedulerAllocations) != 2 {
		t.Fatalf("reports = %#v", queue.Status.SchedulerAllocations)
	}
	managers := map[string]bool{}
	for _, managedField := range queue.ManagedFields {
		managers[managedField.Manager] = true
	}
	if !managers["reporter-0"] || !managers["reporter-1"] {
		t.Fatalf("reporter managedFields missing: %#v", queue.ManagedFields)
	}

	patchAllocation := func(allocated v1.ResourceList) {
		t.Helper()
		queue, err := client.SchedulingV1beta1().Queues().Get(ctx, "queue-reporting", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get Queue before aggregate patch: %v", err)
		}
		patch := []map[string]interface{}{
			{"op": "test", "path": "/metadata/resourceVersion", "value": queue.ResourceVersion},
			{"op": "add", "path": "/status/allocated", "value": allocated},
			{"op": "add", "path": "/status/allocationReporting", "value": map[string]interface{}{
				"ringID": ringID, "ringGeneration": generation, "expectedMembers": 2,
			}},
		}
		patchBytes, err := json.Marshal(patch)
		if err != nil {
			t.Fatalf("marshal aggregate patch: %v", err)
		}
		if _, err := client.SchedulingV1beta1().Queues().Patch(
			ctx, "queue-reporting", types.JSONPatchType, patchBytes, metav1.PatchOptions{
				FieldManager: aggregateManager,
			}, "status"); err != nil {
			t.Fatalf("patch aggregate: %v", err)
		}
	}
	patchAllocation(v1.ResourceList{v1.ResourceCPU: resource.MustParse("3")})
	queue, err = client.SchedulingV1beta1().Queues().Get(ctx, "queue-reporting", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Queue after aggregate patch: %v", err)
	}
	if len(queue.Status.Allocated) != 1 || queue.Status.Allocated.Cpu().Value() != 3 {
		t.Fatalf("legacy allocation was not replaced: %#v", queue.Status.Allocated)
	}
	if queue.Status.State != schedulingv1beta1.QueueStateOpen || len(queue.Status.Reservation.Nodes) != 1 ||
		len(queue.Status.SchedulerAllocations) != 2 {
		t.Fatalf("aggregate patch changed unrelated status: %#v", queue.Status)
	}
	aggregateFields := ""
	for _, managedField := range queue.ManagedFields {
		if managedField.Manager == aggregateManager && managedField.Subresource == "status" && managedField.FieldsV1 != nil {
			aggregateFields = string(managedField.FieldsV1.Raw)
			break
		}
	}
	if !strings.Contains(aggregateFields, `"f:allocated"`) ||
		!strings.Contains(aggregateFields, `"f:allocationReporting"`) ||
		strings.Contains(aggregateFields, `"f:schedulerAllocations"`) {
		t.Fatalf("aggregate managed fields = %s", aggregateFields)
	}

	applyReport("queue-reporting", "reporter-0", ringID+"/0", 0, v1.ResourceList{})
	queue, err = client.SchedulingV1beta1().Queues().Get(ctx, "queue-reporting", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Queue after empty report: %v", err)
	}
	if len(queue.Status.SchedulerAllocations[ringID+"/0"].Allocated) != 0 {
		t.Fatalf("reporter-0 allocation was not atomically cleared: %#v", queue.Status.SchedulerAllocations)
	}
	reporterOne := queue.Status.SchedulerAllocations[ringID+"/1"]
	if reporterOne.Allocated.Cpu().Value() != 2 {
		t.Fatalf("reporter-1 was changed: %#v", queue.Status.SchedulerAllocations)
	}
	patchAllocation(v1.ResourceList{})
	queue, err = client.SchedulingV1beta1().Queues().Get(ctx, "queue-reporting", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Queue after aggregate clear: %v", err)
	}
	if len(queue.Status.Allocated) != 0 || len(queue.Status.SchedulerAllocations) != 2 {
		t.Fatalf("aggregate clear changed reports or retained resources: %#v", queue.Status)
	}

	extensions := apiextensionsclient.NewForConfigOrDie(config)
	crd, err := extensions.ApiextensionsV1().CustomResourceDefinitions().Get(
		ctx, "queues.scheduling.volcano.sh", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Queue CRD: %v", err)
	}
	statusSchema := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["status"]
	reportsSchema := statusSchema.Properties["schedulerAllocations"]
	if reportsSchema.XMapType == nil || *reportsSchema.XMapType != "granular" {
		t.Fatalf("schedulerAllocations map type = %v", reportsSchema.XMapType)
	}
	if reportsSchema.AdditionalProperties == nil || reportsSchema.AdditionalProperties.Schema == nil ||
		reportsSchema.AdditionalProperties.Schema.XMapType == nil ||
		*reportsSchema.AdditionalProperties.Schema.XMapType != "atomic" {
		t.Fatalf("SchedulerAllocation map type is not atomic: %#v", reportsSchema.AdditionalProperties)
	}
}
