// Copyright 2019 The Kubeflow Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kubeflow "github.com/kuizhiqing/resilient-training-operator/pkg/apis/kubeflow/v2beta1"
)

const (
	// mpiJobCreatedReason is added in a resilientjob when it is created.
	mpiJobCreatedReason = "ResilientJobCreated"
	// mpiJobSucceededReason is added in a resilientjob when it is succeeded.
	mpiJobSucceededReason = "ResilientJobSucceeded"
	// mpiJobRunningReason is added in a resilientjob when it is running.
	mpiJobRunningReason = "ResilientJobRunning"
	// mpiJobSuspendedReason is added in a resilientjob when it is suspended.
	mpiJobSuspendedReason = "ResilientJobSuspended"
	// mpiJobResumedReason is added in a resilientjob when it is resumed.
	mpiJobResumedReason = "ResilientJobResumed"
	// mpiJobFailedReason is added in a resilientjob when it is failed.
	mpiJobFailedReason = "ResilientJobFailed"
)

// initializeResilientJobStatuses initializes the ReplicaStatuses for ResilientJob.
func initializeResilientJobStatuses(mpiJob *kubeflow.ResilientJob, mtype kubeflow.MPIReplicaType) {
	if mpiJob.Status.ReplicaStatuses == nil {
		mpiJob.Status.ReplicaStatuses = make(map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus)
	}

	mpiJob.Status.ReplicaStatuses[mtype] = &kubeflow.ReplicaStatus{}
}

// updateResilientJobConditions updates the conditions of the given mpiJob.
func updateResilientJobConditions(mpiJob *kubeflow.ResilientJob, conditionType kubeflow.JobConditionType, status corev1.ConditionStatus, reason, message string) bool {
	condition := newCondition(conditionType, status, reason, message)
	return setCondition(&mpiJob.Status, condition)
}

// newCondition creates a new mpiJob condition.
func newCondition(conditionType kubeflow.JobConditionType, status corev1.ConditionStatus, reason, message string) kubeflow.JobCondition {
	return kubeflow.JobCondition{
		Type:               conditionType,
		Status:             status,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// getCondition returns the condition with the provided type.
func getCondition(status kubeflow.JobStatus, condType kubeflow.JobConditionType) *kubeflow.JobCondition {
	for _, condition := range status.Conditions {
		if condition.Type == condType {
			return &condition
		}
	}
	return nil
}

func hasCondition(status kubeflow.JobStatus, condType kubeflow.JobConditionType) bool {
	for _, condition := range status.Conditions {
		if condition.Type == condType && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func isAlreadyRun(status kubeflow.JobStatus) bool {
	return isFinished(status) || isRunning(status)
}

func isFinished(status kubeflow.JobStatus) bool {
	return isSucceeded(status) || isFailed(status)
}

func isSucceeded(status kubeflow.JobStatus) bool {
	return hasCondition(status, kubeflow.JobSucceeded)
}

func isFailed(status kubeflow.JobStatus) bool {
	return hasCondition(status, kubeflow.JobFailed)
}

func isRunning(status kubeflow.JobStatus) bool {
	return hasCondition(status, kubeflow.JobRunning)
}

// setCondition updates the mpiJob to include the provided condition.
// If the condition that we are about to add already exists
// and has the same status and reason then we are not going to update.
func setCondition(status *kubeflow.JobStatus, condition kubeflow.JobCondition) bool {
	currentCond := getCondition(*status, condition.Type)

	// Do nothing if condition doesn't change
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return false
	}

	// Do not update lastTransitionTime if the status of the condition doesn't change.
	if currentCond != nil && currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}

	// Append the updated condition
	newConditions := filterOutCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
	return true
}

// filterOutCondition returns a new slice of mpiJob conditions without conditions with the provided type.
func filterOutCondition(conditions []kubeflow.JobCondition, condType kubeflow.JobConditionType) []kubeflow.JobCondition {
	var newConditions []kubeflow.JobCondition
	for _, c := range conditions {
		if condType == kubeflow.JobRestarting && c.Type == kubeflow.JobRunning {
			continue
		}
		if condType == kubeflow.JobRunning && c.Type == kubeflow.JobRestarting {
			continue
		}

		if c.Type == condType {
			continue
		}

		// Set the running condition status to be false when current condition failed or succeeded
		if (condType == kubeflow.JobFailed || condType == kubeflow.JobSucceeded) && (c.Type == kubeflow.JobRunning || c.Type == kubeflow.JobFailed) {
			c.Status = corev1.ConditionFalse
		}

		newConditions = append(newConditions, c)
	}
	return newConditions
}

func isJobFinished(j *corev1.Pod) bool {
	return isPodSucceeded(j) || isPodFailed(j)
}

func isPodRunning(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodRunning
}

func isPodPending(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodPending
}

func isPodFailed(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodFailed
}

func isPodSucceeded(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded
}

func isPodEvicted(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodFailed && p.Status.Reason == "Evicted"
}
