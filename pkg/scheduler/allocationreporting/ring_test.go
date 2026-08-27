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

package allocationreporting

import (
	"strings"
	"testing"
)

func setValidRingEnv(t *testing.T) {
	t.Helper()
	t.Setenv(MultiSchedulerEnableEnv, "true")
	t.Setenv(SchedulerNumEnv, "3")
	t.Setenv(SchedulerPodNameEnv, "volcano-scheduler-1")
	t.Setenv(SchedulerNamespaceEnv, "volcano-system")
}

func TestLoadFixedRing(t *testing.T) {
	setValidRingEnv(t)

	ring, err := LoadFixedRing([]string{"volcano-b", "volcano-a", "volcano-a"}, "default")
	if err != nil {
		t.Fatalf("LoadFixedRing() error = %v", err)
	}
	if ring.RingID != "volcano-system/volcano-scheduler" {
		t.Fatalf("RingID = %q", ring.RingID)
	}
	if ring.ReporterID != "volcano-system/volcano-scheduler/1" {
		t.Fatalf("ReporterID = %q", ring.ReporterID)
	}
	if ring.MemberIndex != 1 || ring.ExpectedMembers != 3 {
		t.Fatalf("member = %d/%d", ring.MemberIndex, ring.ExpectedMembers)
	}
	if len(ring.RingGeneration) != 64 {
		t.Fatalf("RingGeneration length = %d", len(ring.RingGeneration))
	}
	if len(ring.FieldManager) > 128 || !strings.HasPrefix(ring.FieldManager, fieldManagerPrefix) {
		t.Fatalf("FieldManager = %q", ring.FieldManager)
	}

	same, err := LoadFixedRing([]string{"volcano-a", "volcano-b"}, "default")
	if err != nil {
		t.Fatalf("LoadFixedRing() second error = %v", err)
	}
	if same.RingGeneration != ring.RingGeneration || same.FieldManager != ring.FieldManager {
		t.Fatalf("ring identity is not stable: %#v vs %#v", ring, same)
	}

	different, err := LoadFixedRing([]string{"volcano-a", "volcano-b"}, "other")
	if err != nil {
		t.Fatalf("LoadFixedRing() changed config error = %v", err)
	}
	if different.RingGeneration == ring.RingGeneration {
		t.Fatal("RingGeneration did not change with reporting configuration")
	}
}

func TestLoadFixedRingRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T)
	}{
		{name: "multi scheduler disabled", mutate: func(t *testing.T) { t.Setenv(MultiSchedulerEnableEnv, "false") }},
		{name: "namespace missing", mutate: func(t *testing.T) { t.Setenv(SchedulerNamespaceEnv, "") }},
		{name: "pod ordinal missing", mutate: func(t *testing.T) { t.Setenv(SchedulerPodNameEnv, "volcano-scheduler") }},
		{name: "member count invalid", mutate: func(t *testing.T) { t.Setenv(SchedulerNumEnv, "zero") }},
		{name: "member count zero", mutate: func(t *testing.T) { t.Setenv(SchedulerNumEnv, "0") }},
		{name: "ordinal outside ring", mutate: func(t *testing.T) {
			t.Setenv(SchedulerPodNameEnv, "volcano-scheduler-3")
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setValidRingEnv(t)
			test.mutate(t)
			if _, err := LoadFixedRing([]string{"volcano"}, "default"); err == nil {
				t.Fatal("LoadFixedRing() expected error")
			}
		})
	}
}
