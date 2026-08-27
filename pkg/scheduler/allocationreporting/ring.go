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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

const (
	MultiSchedulerEnableEnv = "MULTI_SCHEDULER_ENABLE"
	SchedulerNumEnv         = "SCHEDULER_NUM"
	SchedulerPodNameEnv     = "SCHEDULER_POD_NAME"
	SchedulerNamespaceEnv   = "SCHEDULER_POD_NAMESPACE"

	reportingProtocolVersion = "v1alpha1"
	fieldManagerPrefix       = "volcano-scheduler-allocation-"
)

// FixedRing identifies one member of a fixed consistent-hash scheduler ring.
type FixedRing struct {
	RingID          string
	RingGeneration  string
	ReporterID      string
	FieldManager    string
	MemberIndex     int32
	ExpectedMembers int32
}

type generationInput struct {
	ProtocolVersion string   `json:"protocolVersion"`
	RingID          string   `json:"ringID"`
	ExpectedMembers int32    `json:"expectedMembers"`
	SchedulerNames  []string `json:"schedulerNames"`
	DefaultQueue    string   `json:"defaultQueue"`
}

// LoadFixedRing loads and validates the fixed-ring identity from scheduler configuration.
func LoadFixedRing(schedulerNames []string, defaultQueue string) (*FixedRing, error) {
	if os.Getenv(MultiSchedulerEnableEnv) != "true" {
		return nil, fmt.Errorf("%s must be true", MultiSchedulerEnableEnv)
	}

	namespace := strings.TrimSpace(os.Getenv(SchedulerNamespaceEnv))
	if namespace == "" {
		return nil, fmt.Errorf("%s must not be empty", SchedulerNamespaceEnv)
	}

	podName := strings.TrimSpace(os.Getenv(SchedulerPodNameEnv))
	ringName, memberIndex, err := parseStatefulSetMember(podName)
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", SchedulerPodNameEnv, err)
	}

	expectedMembers64, err := strconv.ParseInt(strings.TrimSpace(os.Getenv(SchedulerNumEnv)), 10, 32)
	if err != nil || expectedMembers64 <= 0 {
		return nil, fmt.Errorf("%s must be a positive integer", SchedulerNumEnv)
	}
	expectedMembers := int32(expectedMembers64)
	if memberIndex >= expectedMembers {
		return nil, fmt.Errorf("member index %d must be less than %s %d", memberIndex, SchedulerNumEnv, expectedMembers)
	}

	names := slices.Clone(schedulerNames)
	slices.Sort(names)
	names = slices.Compact(names)
	if len(names) == 0 || names[0] == "" {
		return nil, fmt.Errorf("scheduler names must not be empty")
	}

	ringID := namespace + "/" + ringName
	generationBytes, err := json.Marshal(generationInput{
		ProtocolVersion: reportingProtocolVersion,
		RingID:          ringID,
		ExpectedMembers: expectedMembers,
		SchedulerNames:  names,
		DefaultQueue:    defaultQueue,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal ring generation input: %w", err)
	}
	generationHash := sha256.Sum256(generationBytes)
	reporterID := fmt.Sprintf("%s/%d", ringID, memberIndex)
	managerHash := sha256.Sum256([]byte(reporterID))

	return &FixedRing{
		RingID:          ringID,
		RingGeneration:  hex.EncodeToString(generationHash[:]),
		ReporterID:      reporterID,
		FieldManager:    fieldManagerPrefix + hex.EncodeToString(managerHash[:16]),
		MemberIndex:     memberIndex,
		ExpectedMembers: expectedMembers,
	}, nil
}

func parseStatefulSetMember(podName string) (string, int32, error) {
	separator := strings.LastIndexByte(podName, '-')
	if separator <= 0 || separator == len(podName)-1 {
		return "", 0, fmt.Errorf("pod name %q must end in a StatefulSet ordinal", podName)
	}

	ordinal64, err := strconv.ParseInt(podName[separator+1:], 10, 32)
	if err != nil || ordinal64 < 0 {
		return "", 0, fmt.Errorf("pod name %q has an invalid StatefulSet ordinal", podName)
	}

	return podName[:separator], int32(ordinal64), nil
}
