// Copyright 2021 The Kubeflow Authors.
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

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/reference"
	"k8s.io/utils/pointer"
	schedv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	schedclientset "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	volcanov1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	volcanoclient "volcano.sh/apis/pkg/client/clientset/versioned"

	kubeflow "github.com/kuizhiqing/resilient-training-operator/pkg/apis/kubeflow/v2beta1"
	clientset "github.com/kuizhiqing/resilient-training-operator/pkg/client/clientset/versioned"
	"github.com/kuizhiqing/resilient-training-operator/pkg/client/clientset/versioned/scheme"
	informers "github.com/kuizhiqing/resilient-training-operator/pkg/client/informers/externalversions"
	"github.com/kuizhiqing/resilient-training-operator/pkg/controller"
)

const (
	waitInterval   = 100 * time.Millisecond
	launcherSuffix = "-launcher"
	waitTimeout    = 15 * time.Second
)

func TestResilientJobSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := newTestSetup(ctx, t)
	startController(ctx, s.kClient, s.mpiClient, nil)

	mpiJob := &kubeflow.ResilientJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job",
			Namespace: s.namespace,
		},
		Spec: kubeflow.ResilientJobSpec{
			SlotsPerWorker: newInt32(1),
			RunPolicy: kubeflow.RunPolicy{
				CleanPodPolicy: kubeflow.NewCleanPodPolicy(kubeflow.CleanPodPolicyRunning),
			},
			MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
				kubeflow.MPIReplicaTypeLauncher: {
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
						},
					},
				},
				kubeflow.MPIReplicaTypeWorker: {
					Replicas: newInt32(2),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
						},
					},
				},
			},
		},
	}
	var err error
	mpiJob, err = s.mpiClient.KubeflowV2beta1().ResilientJobs(s.namespace).Create(ctx, mpiJob, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed sending job to apiserver: %v", err)
	}

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeNormal,
		Reason: "ResilientJobCreated",
	}, mpiJob))

	workerPods, launcherPod := validateResilientJobDependencies(ctx, t, s.kClient, mpiJob, 2, true, nil)
	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {},
		kubeflow.MPIReplicaTypeWorker:   {},
		kubeflow.MPIReplicaTypeHorker:   {},
	})
	if !mpiJobHasCondition(mpiJob, kubeflow.JobCreated) {
		t.Errorf("ResilientJob missing Created condition")
	}
	s.events.verify(t)

	err = updatePodsToPhase(ctx, s.kClient, workerPods, corev1.PodRunning)
	if err != nil {
		t.Fatalf("Updating worker Pods to Running phase: %v", err)
	}
	validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {},
		kubeflow.MPIReplicaTypeWorker: {
			Active: 2,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeNormal,
		Reason: "ResilientJobRunning",
	}, mpiJob))

	err = updatePodsToPhase(ctx, s.kClient, []*corev1.Pod{launcherPod}, corev1.PodRunning)
	if err != nil {
		t.Fatalf("Updating launcher Pods to Running phase: %v", err)
	}
	validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Active: 1,
		},
		kubeflow.MPIReplicaTypeWorker: {
			Active: 2,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})
	s.events.verify(t)

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeNormal,
		Reason: "ResilientJobSucceeded",
	}, mpiJob))

	// update launcher
	launcherPod, err = getLauncherPodForJob(ctx, s.kClient, mpiJob)
	if err != nil {
		t.Fatalf("Get launcher pod failed: %v", err)
	}
	err = updatePodsToPhase(ctx, s.kClient, []*corev1.Pod{launcherPod}, corev1.PodSucceeded)
	if err != nil {
		t.Fatalf("Updating launcher Pods to Running phase: %v", err)
	}

	validateResilientJobDependencies(ctx, t, s.kClient, mpiJob, 0, false, nil)
	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Succeeded: 1,
		},
		kubeflow.MPIReplicaTypeWorker: {},
		kubeflow.MPIReplicaTypeHorker: {},
	})
	s.events.verify(t)
	if !mpiJobHasCondition(mpiJob, kubeflow.JobSucceeded) {
		t.Errorf("ResilientJob doesn't have Succeeded condition after launcher Job succeeded")
	}
}

func TestResilientJobWaitWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := newTestSetup(ctx, t)
	startController(ctx, s.kClient, s.mpiClient, nil)

	mpiJob := &kubeflow.ResilientJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job",
			Namespace: s.namespace,
		},
		Spec: kubeflow.ResilientJobSpec{
			SlotsPerWorker:         newInt32(1),
			LauncherCreationPolicy: "WaitForWorkersReady",
			RunPolicy: kubeflow.RunPolicy{
				CleanPodPolicy: kubeflow.NewCleanPodPolicy(kubeflow.CleanPodPolicyRunning),
			},
			MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
				kubeflow.MPIReplicaTypeLauncher: {
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
						},
					},
				},
				kubeflow.MPIReplicaTypeWorker: {
					Replicas: newInt32(2),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
						},
					},
				},
			},
		},
	}
	var err error
	mpiJob, err = s.mpiClient.KubeflowV2beta1().ResilientJobs(s.namespace).Create(ctx, mpiJob, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed sending job to apiserver: %v", err)
	}

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeNormal,
		Reason: "ResilientJobCreated",
	}, mpiJob))

	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {},
		kubeflow.MPIReplicaTypeWorker:   {},
		kubeflow.MPIReplicaTypeHorker:   {},
	})
	if !mpiJobHasCondition(mpiJob, kubeflow.JobCreated) {
		t.Errorf("ResilientJob missing Created condition")
	}
	s.events.verify(t)

	workerPods, err := getWorkerPodsForJob(ctx, s.kClient, mpiJob)
	if err != nil {
		t.Fatalf("Cannot get worker pods from job: %v", err)
	}

	err = updatePodsToPhase(ctx, s.kClient, workerPods, corev1.PodRunning)
	if err != nil {
		t.Fatalf("Updating worker Pods to Running phase: %v", err)
	}

	// No launcher here, workers are running, but not ready yet
	validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {},
		kubeflow.MPIReplicaTypeWorker: {
			Active: 2,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})

	err = updatePodsCondition(ctx, s.kClient, workerPods, corev1.PodCondition{
		Type:   corev1.PodReady,
		Status: corev1.ConditionTrue,
	})
	if err != nil {
		t.Fatalf("Updating worker Pods to Ready: %v", err)
	}

	validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {},
		kubeflow.MPIReplicaTypeWorker: {
			Active: 2,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})

	_, launcherPod := validateResilientJobDependencies(ctx, t, s.kClient, mpiJob, 2, true, nil)

	err = updatePodsToPhase(ctx, s.kClient, []*corev1.Pod{launcherPod}, corev1.PodRunning)
	if err != nil {
		t.Fatalf("Updating launcher Pods to Running phase: %v", err)
	}

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeNormal,
		Reason: "ResilientJobRunning",
	}, mpiJob))

	validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Active: 1,
		},
		kubeflow.MPIReplicaTypeWorker: {
			Active: 2,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})
	s.events.verify(t)
}

func TestResilientJobFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := newTestSetup(ctx, t)
	startController(ctx, s.kClient, s.mpiClient, nil)

	mpiJob := &kubeflow.ResilientJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job",
			Namespace: s.namespace,
			Labels: map[string]string{
				"kubeflow.org/elastic": "false",
			},
		},
		Spec: kubeflow.ResilientJobSpec{
			SlotsPerWorker: newInt32(1),
			RunPolicy: kubeflow.RunPolicy{
				CleanPodPolicy: kubeflow.NewCleanPodPolicy(kubeflow.CleanPodPolicyRunning),
			},
			MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
				kubeflow.MPIReplicaTypeLauncher: {
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
						},
					},
				},
				kubeflow.MPIReplicaTypeWorker: {
					Replicas: newInt32(2),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
						},
					},
				},
			},
		},
	}

	var err error
	mpiJob, err = s.mpiClient.KubeflowV2beta1().ResilientJobs(s.namespace).Create(ctx, mpiJob, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed sending job to apiserver: %v", err)
	}

	workerPods, launcherPod := validateResilientJobDependencies(ctx, t, s.kClient, mpiJob, 2, true, nil)

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeNormal,
		Reason: "ResilientJobRunning",
	}, mpiJob))
	err = updatePodsToPhase(ctx, s.kClient, workerPods, corev1.PodRunning)
	if err != nil {
		t.Fatalf("Updating worker Pods to Running phase: %v", err)
	}
	err = updatePodsToPhase(ctx, s.kClient, []*corev1.Pod{launcherPod}, corev1.PodRunning)
	if err != nil {
		t.Fatalf("Updating launcher Pods to Running phase: %v", err)
	}
	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Active: 1,
		},
		kubeflow.MPIReplicaTypeWorker: {
			Active: 2,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})
	if !mpiJobHasCondition(mpiJob, kubeflow.JobRunning) {
		t.Errorf("ResilientJob has no running condition")
	}
	s.events.verify(t)

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeWarning,
		Reason: "ResilientJobFailed",
	}, mpiJob))

	launcherPod, err = getLauncherPodForJob(ctx, s.kClient, mpiJob)
	if err != nil {
		t.Fatalf("Failed to create mock pod for launcher Job: %v", err)
	}
	err = updatePodsToPhase(ctx, s.kClient, []*corev1.Pod{launcherPod}, corev1.PodFailed)
	if err != nil {
		t.Fatalf("Failed to update launcher Pod to Running phase: %v", err)
	}
	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Failed: 1,
		},
		kubeflow.MPIReplicaTypeWorker: {
			Active: 0,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})
	if !mpiJobHasCondition(mpiJob, kubeflow.JobFailed) {
		t.Errorf("ResilientJob has no Failed condition when a launcher Pod fails")
	}

	s.events.verify(t)
}

func TestResilientJobFailOver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := newTestSetup(ctx, t)
	startController(ctx, s.kClient, s.mpiClient, nil)

	mpiJob := &kubeflow.ResilientJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job",
			Namespace: s.namespace,
			Labels: map[string]string{
				"kubeflow.org/elastic": "true",
			},
		},
		Spec: kubeflow.ResilientJobSpec{
			SlotsPerWorker: newInt32(1),
			RunPolicy: kubeflow.RunPolicy{
				CleanPodPolicy: kubeflow.NewCleanPodPolicy(kubeflow.CleanPodPolicyRunning),
			},
			MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
				kubeflow.MPIReplicaTypeLauncher: {
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
						},
					},
				},
				kubeflow.MPIReplicaTypeWorker: {
					Replicas: newInt32(2),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
						},
					},
				},
			},
		},
	}

	var err error
	mpiJob, err = s.mpiClient.KubeflowV2beta1().ResilientJobs(s.namespace).Create(ctx, mpiJob, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed sending job to apiserver: %v", err)
	}

	workerPods, launcherPod := validateResilientJobDependencies(ctx, t, s.kClient, mpiJob, 2, true, nil)

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeNormal,
		Reason: "ResilientJobRunning",
	}, mpiJob))
	err = updatePodsToPhase(ctx, s.kClient, workerPods, corev1.PodFailed)
	if err != nil {
		t.Fatalf("Updating worker Pods to Running phase: %v", err)
	}
	// no restart if launcher not running
	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Active: 0,
		},
		kubeflow.MPIReplicaTypeWorker: {
			Failed: 2,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})
	err = updatePodsToPhase(ctx, s.kClient, []*corev1.Pod{launcherPod}, corev1.PodRunning)
	if err != nil {
		t.Fatalf("Updating launcher Pods to Running phase: %v", err)
	}
	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Active: 1,
		},
		kubeflow.MPIReplicaTypeWorker: {
			Active: 0,
			Failed: 0,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})
	// get new worker pods
	workerPods, _ = validateResilientJobDependencies(ctx, t, s.kClient, mpiJob, 2, true, nil)
	if err != nil {
		t.Fatalf("Failed to get worker pods: %v", err)
	}
	err = updatePodsToPhase(ctx, s.kClient, workerPods, corev1.PodRunning)
	if err != nil {
		t.Fatalf("Failed to update worker Pod phase: %v", err)
	}
	s.events.verify(t)

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeWarning,
		Reason: "ResilientJobFailed",
	}, mpiJob))
	// get new worker pods
	workerPods, _ = validateResilientJobDependencies(ctx, t, s.kClient, mpiJob, 2, true, nil)
	if err != nil {
		t.Fatalf("Failed to get worker pods: %v", err)
	}
	err = updatePodsToPhase(ctx, s.kClient, workerPods, corev1.PodFailed)
	if err != nil {
		t.Fatalf("Failed to update worker Pod to failed phase: %v", err)
	}
	// get new worker pods
	workerPods, _ = validateResilientJobDependencies(ctx, t, s.kClient, mpiJob, 2, true, nil)
	if err != nil {
		t.Fatalf("Failed to get worker pods: %v", err)
	}
	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Active: 1,
		},
		kubeflow.MPIReplicaTypeWorker: {
			Active: 0,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})
	err = updatePodsToPhase(ctx, s.kClient, workerPods, corev1.PodRunning)
	if err != nil {
		t.Fatalf("Failed to update worker Pod phase: %v", err)
	}
	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Active: 1,
		},
		kubeflow.MPIReplicaTypeWorker: {
			Active: 2,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})
	launcherPod, err = getLauncherPodForJob(ctx, s.kClient, mpiJob)
	if err != nil {
		t.Fatalf("Failed to create mock pod for launcher Job: %v", err)
	}
	err = updatePodsToPhase(ctx, s.kClient, []*corev1.Pod{launcherPod}, corev1.PodFailed)
	if err != nil {
		t.Fatalf("Failed to update worker Pod phase: %v", err)
	}

	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Failed: 1,
			Active: 0,
		},
		kubeflow.MPIReplicaTypeWorker: {
			Active: 0,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})
	if !mpiJobHasCondition(mpiJob, kubeflow.JobFailed) {
		t.Errorf("ResilientJob has no Failed condition when worker failed twice")
	}

	s.events.verify(t)
}

func TestLowPriorityResilientJobFailOver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := newTestSetup(ctx, t)
	startController(ctx, s.kClient, s.mpiClient, nil)

	prioClass := "low-priority"
	mpiJob := &kubeflow.ResilientJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job",
			Namespace: s.namespace,
			Labels: map[string]string{
				"kubeflow.org/elastic": "false",
			},
		},
		Spec: kubeflow.ResilientJobSpec{
			SlotsPerWorker: newInt32(1),
			RunPolicy:      kubeflow.RunPolicy{
				// CleanPodPolicy: kubeflow.NewCleanPodPolicy(kubeflow.CleanPodPolicyRunning),
			},
			MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
				kubeflow.MPIReplicaTypeLauncher: {
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
							PriorityClassName: prioClass,
						},
					},
				},
				kubeflow.MPIReplicaTypeWorker: {
					Replicas: newInt32(1),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
							PriorityClassName: prioClass,
						},
					},
				},
			},
		},
	}

	priorityClass := &schedulingv1.PriorityClass{
		TypeMeta: metav1.TypeMeta{
			APIVersion: schedulingv1.SchemeGroupVersion.String(),
			Kind:       "PriorityClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: prioClass,
		},
		Value: 100,
	}
	var err error
	_, err = s.kClient.SchedulingV1().PriorityClasses().Create(ctx, priorityClass, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed sending priorityClass to apiserver: %v", err)
	}

	mpiJob, err = s.mpiClient.KubeflowV2beta1().ResilientJobs(s.namespace).Create(ctx, mpiJob, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed sending job to apiserver: %v", err)
	}

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeNormal,
		Reason: "ResilientJobCreated",
	}, mpiJob))

	workerPods, launcherPod := validateResilientJobDependencies(ctx, t, s.kClient, mpiJob, 1, true, nil)
	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {},
		kubeflow.MPIReplicaTypeWorker:   {},
		kubeflow.MPIReplicaTypeHorker:   {},
	})
	if !mpiJobHasCondition(mpiJob, kubeflow.JobCreated) {
		t.Errorf("ResilientJob missing Created condition")
	}
	s.events.verify(t)

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeNormal,
		Reason: "ResilientJobRunning",
	}, mpiJob))
	err = updatePodsToPhase(ctx, s.kClient, []*corev1.Pod{launcherPod}, corev1.PodRunning)
	if err != nil {
		t.Fatalf("Updating launcher Pods to Running phase: %v", err)
	}
	err = updatePodsToPhase(ctx, s.kClient, workerPods, corev1.PodRunning)
	if err != nil {
		t.Fatalf("Failed to update worker Pod phase: %v", err)
	}
	s.events.verify(t)

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeWarning,
		Reason: "ResilientJobFailed",
	}, mpiJob))
	err = updatePodsToPhase(ctx, s.kClient, workerPods, corev1.PodFailed)
	if err != nil {
		t.Fatalf("Failed to update worker Pod to failed phase: %v", err)
	}
	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Active: 1,
		},
		kubeflow.MPIReplicaTypeWorker: {
			Active: 0,
			Failed: 1,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	})
	if !mpiJobHasCondition(mpiJob, kubeflow.JobFailed) {
		t.Errorf("ResilientJob not failed")
	}

	s.events.verify(t)
}

func TestResilientJobWithSchedulerPlugins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := newTestSetup(ctx, t)
	gangSchedulerCfg := &gangSchedulerConfig{
		schedulerName: "default-scheduler",
		schedClient:   s.gangSchedulerCfg.schedClient,
	}
	startController(ctx, s.kClient, s.mpiClient, gangSchedulerCfg)

	mpiJob := &kubeflow.ResilientJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job",
			Namespace: s.namespace,
		},
		Spec: kubeflow.ResilientJobSpec{
			SlotsPerWorker: newInt32(1),
			RunPolicy: kubeflow.RunPolicy{
				CleanPodPolicy: kubeflow.NewCleanPodPolicy(kubeflow.CleanPodPolicyRunning),
				SchedulingPolicy: &kubeflow.SchedulingPolicy{
					ScheduleTimeoutSeconds: pointer.Int32(900),
				},
			},
			MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
				kubeflow.MPIReplicaTypeLauncher: {
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							PriorityClassName: "test-pc",
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
						},
					},
				},
				kubeflow.MPIReplicaTypeWorker: {
					Replicas: newInt32(2),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
						},
					},
				},
			},
		},
	}
	priorityClass := &schedulingv1.PriorityClass{
		TypeMeta: metav1.TypeMeta{
			APIVersion: schedulingv1.SchemeGroupVersion.String(),
			Kind:       "PriorityClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pc",
		},
		Value: 100_000,
	}
	// 1. Create PriorityClass
	var err error
	_, err = s.kClient.SchedulingV1().PriorityClasses().Create(ctx, priorityClass, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed sending priorityClass to apiserver: %v", err)
	}

	// 2. Create ResilientJob
	mpiJob, err = s.mpiClient.KubeflowV2beta1().ResilientJobs(s.namespace).Create(ctx, mpiJob, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed sending job to apiserver: %v", err)
	}

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeNormal,
		Reason: "ResilientJobCreated",
	}, mpiJob))

	validateResilientJobDependencies(ctx, t, s.kClient, mpiJob, 2, true, gangSchedulerCfg)
	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {},
		kubeflow.MPIReplicaTypeWorker:   {},
		kubeflow.MPIReplicaTypeHorker:   {},
	})
	if !mpiJobHasCondition(mpiJob, kubeflow.JobCreated) {
		t.Errorf("ResilientJob missing Created condition")
	}
	s.events.verify(t)

	// 3. Update SchedulingPolicy of ResilientJob
	updatedScheduleTimeSeconds := int32(10)
	mpiJob.Spec.RunPolicy.SchedulingPolicy.ScheduleTimeoutSeconds = &updatedScheduleTimeSeconds
	mpiJob, err = s.mpiClient.KubeflowV2beta1().ResilientJobs(s.namespace).Update(ctx, mpiJob, metav1.UpdateOptions{})
	if err != nil {
		t.Errorf("Failed updating job: %v", err)
	}
	if err = wait.Poll(waitInterval, waitTimeout, func() (bool, error) {
		pg, err := getSchedPodGroup(ctx, gangSchedulerCfg.schedClient, mpiJob)
		if err != nil {
			return false, err
		}
		if pg == nil {
			return false, nil
		}
		return *pg.Spec.ScheduleTimeoutSeconds == updatedScheduleTimeSeconds, nil
	}); err != nil {
		t.Errorf("Failed updating scheduler-plugins PodGroup: %v", err)
	}
}

func TestResilientJobWithVolcanoScheduler(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s := newTestSetup(ctx, t)
	gangSchedulerCfg := &gangSchedulerConfig{
		schedulerName: "volcano",
		volcanoClient: s.gangSchedulerCfg.volcanoClient,
	}
	startController(ctx, s.kClient, s.mpiClient, gangSchedulerCfg)

	prioClass := "test-pc-volcano"
	mpiJob := &kubeflow.ResilientJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job-by-volcano",
			Namespace: s.namespace,
		},
		Spec: kubeflow.ResilientJobSpec{
			SlotsPerWorker: newInt32(1),
			RunPolicy: kubeflow.RunPolicy{
				CleanPodPolicy: kubeflow.NewCleanPodPolicy(kubeflow.CleanPodPolicyRunning),
				SchedulingPolicy: &kubeflow.SchedulingPolicy{
					Queue:         "default",
					PriorityClass: prioClass,
				},
			},
			MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
				kubeflow.MPIReplicaTypeLauncher: {
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							PriorityClassName: prioClass,
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
						},
					},
				},
				kubeflow.MPIReplicaTypeWorker: {
					Replicas: newInt32(2),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "main",
									Image: "mpi-image",
								},
							},
						},
					},
				},
			},
		},
	}
	priorityClass := &schedulingv1.PriorityClass{
		TypeMeta: metav1.TypeMeta{
			APIVersion: schedulingv1.SchemeGroupVersion.String(),
			Kind:       "PriorityClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: prioClass,
		},
		Value: 100_000,
	}
	// 1. Create PriorityClass
	var err error
	_, err = s.kClient.SchedulingV1().PriorityClasses().Create(ctx, priorityClass, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed sending priorityClass to apiserver: %v", err)
	}

	// 2. Create ResilientJob
	mpiJob, err = s.mpiClient.KubeflowV2beta1().ResilientJobs(s.namespace).Create(ctx, mpiJob, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed sending job to apiserver: %v", err)
	}

	s.events.expect(eventForJob(corev1.Event{
		Type:   corev1.EventTypeNormal,
		Reason: "ResilientJobCreated",
	}, mpiJob))

	validateResilientJobDependencies(ctx, t, s.kClient, mpiJob, 2, true, gangSchedulerCfg)
	mpiJob = validateResilientJobStatus(ctx, t, s.mpiClient, mpiJob, map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {},
		kubeflow.MPIReplicaTypeWorker:   {},
		kubeflow.MPIReplicaTypeHorker:   {},
	})
	if !mpiJobHasCondition(mpiJob, kubeflow.JobCreated) {
		t.Errorf("ResilientJob missing Created condition")
	}
	s.events.verify(t)

	// 3. Update SchedulingPolicy of ResilientJob
	updatedMinAvaiable := int32(2)
	updatedQueueName := "queue-for-resilientjob"
	mpiJob.Spec.RunPolicy.SchedulingPolicy.MinAvailable = &updatedMinAvaiable
	mpiJob.Spec.RunPolicy.SchedulingPolicy.Queue = updatedQueueName
	mpiJob, err = s.mpiClient.KubeflowV2beta1().ResilientJobs(s.namespace).Update(ctx, mpiJob, metav1.UpdateOptions{})
	if err != nil {
		t.Errorf("Failed updating job: %v", err)
	}
	if err = wait.Poll(waitInterval, waitTimeout, func() (bool, error) {
		pg, err := getVolcanoPodGroup(ctx, gangSchedulerCfg.volcanoClient, mpiJob)
		if err != nil {
			return false, err
		}
		if pg == nil {
			return false, nil
		}
		return pg.Spec.MinMember == updatedMinAvaiable && pg.Spec.Queue == updatedQueueName, nil
	}); err != nil {
		t.Errorf("Failed updating volcano PodGroup: %v", err)
	}
}

func startController(
	ctx context.Context,
	kClient kubernetes.Interface,
	mpiClient clientset.Interface,
	gangSchedulerCfg *gangSchedulerConfig,
) {
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kClient, 0)
	mpiInformerFactory := informers.NewSharedInformerFactory(mpiClient, 0)
	var (
		volcanoClient volcanoclient.Interface
		schedClient   schedclientset.Interface
		schedulerName string
	)
	if gangSchedulerCfg != nil {
		schedulerName = gangSchedulerCfg.schedulerName
		if gangSchedulerCfg.volcanoClient != nil {
			volcanoClient = gangSchedulerCfg.volcanoClient
		} else if gangSchedulerCfg.schedClient != nil {
			schedClient = gangSchedulerCfg.schedClient
		}
	}
	ctrl := controller.NewResilientJobController(
		kClient,
		mpiClient,
		volcanoClient,
		schedClient,
		kubeInformerFactory.Core().V1().Events(),
		kubeInformerFactory.Core().V1().ConfigMaps(),
		kubeInformerFactory.Core().V1().Secrets(),
		kubeInformerFactory.Core().V1().Services(),
		kubeInformerFactory.Core().V1().Pods(),
		kubeInformerFactory.Scheduling().V1().PriorityClasses(),
		mpiInformerFactory.Kubeflow().V2beta1().ResilientJobs(),
		metav1.NamespaceAll, schedulerName,
		"",
		"",
		2,
	)

	go kubeInformerFactory.Start(ctx.Done())
	go mpiInformerFactory.Start(ctx.Done())
	if ctrl.PodGroupCtrl != nil {
		ctrl.PodGroupCtrl.StartInformerFactory(ctx.Done())
	}

	go func() {
		if err := ctrl.Run(1, ctx.Done()); err != nil {
			panic(err)
		}
	}()
}

func validateResilientJobDependencies(
	ctx context.Context,
	t *testing.T,
	kubeClient kubernetes.Interface,
	job *kubeflow.ResilientJob,
	workers int,
	hasLauncher bool,
	gangSchedulingCfg *gangSchedulerConfig,
) ([]*corev1.Pod, *corev1.Pod) {
	t.Helper()
	var (
		svc         *corev1.Service
		cfgMap      *corev1.ConfigMap
		secret      *corev1.Secret
		workerPods  []*corev1.Pod
		launcherPod *corev1.Pod
		podGroup    metav1.Object
	)
	var problems []string
	if err := wait.Poll(waitInterval, waitTimeout, func() (bool, error) {
		problems = nil
		var err error
		svc, err = getServiceForJob(ctx, kubeClient, job)
		if err != nil {
			return false, err
		}
		if svc == nil {
			problems = append(problems, "Service not found")
		}
		cfgMap, err = getConfigMapForJob(ctx, kubeClient, job)
		if err != nil {
			return false, err
		}
		if cfgMap == nil {
			problems = append(problems, "ConfigMap not found")
		}
		secret, err = getSecretForJob(ctx, kubeClient, job)
		if err != nil {
			return false, err
		}
		if secret == nil {
			problems = append(problems, "Secret not found")
		}
		workerPods, err = getWorkerPodsForJob(ctx, kubeClient, job)
		if err != nil {
			return false, err
		}
		if workers != len(workerPods) {
			problems = append(problems, fmt.Sprintf("got %d workers, want %d", len(workerPods), workers))
		}
		launcherPod, err = getLauncherPodForJob(ctx, kubeClient, job)
		if err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		if hasLauncher && launcherPod == nil {
			problems = append(problems, "Launcher Pod not found")
		}
		if cfg := gangSchedulingCfg; cfg != nil {
			if cfg.schedulerName == "volcano" {
				podGroup, err = getVolcanoPodGroup(ctx, cfg.volcanoClient, job)
				if err != nil {
					return false, err
				}
				if podGroup == nil {
					problems = append(problems, "Volcano PodGroup not found")
				}
			} else if len(cfg.schedulerName) != 0 {
				podGroup, err = getSchedPodGroup(ctx, cfg.schedClient, job)
				if err != nil {
					return false, err
				}
				if podGroup == nil {
					problems = append(problems, "Scheduler Plugins PodGroup not found")
				}
			}
		}

		if len(problems) == 0 {
			return true, nil
		}
		return false, nil
	}); err != nil {
		for _, p := range problems {
			t.Error(p)
		}
		t.Fatalf("Waiting for job dependencies: %v", err)
	}
	svcSelector, err := labels.ValidatedSelectorFromSet(svc.Spec.Selector)
	if err != nil {
		t.Fatalf("Invalid workers Service selector: %v", err)
	}
	for _, p := range workerPods {
		if !svcSelector.Matches(labels.Set(p.Labels)) {
			t.Errorf("Workers Service selector doesn't match pod %s", p.Name)
		}
		if !hasVolumeForSecret(&p.Spec, secret) {
			t.Errorf("Secret %s not mounted in Pod %s", secret.Name, p.Name)
		}
	}
	if hasLauncher {
		if !hasVolumeForSecret(&launcherPod.Spec, secret) {
			t.Errorf("Secret %s not mounted in launcher Job", secret.Name)
		}
		if !hasVolumeForConfigMap(&launcherPod.Spec, cfgMap) {
			t.Errorf("ConfigMap %s not mounted in launcher Job", secret.Name)
		}
	}
	return workerPods, launcherPod
}

func validateResilientJobStatus(ctx context.Context, t *testing.T, client clientset.Interface, job *kubeflow.ResilientJob, want map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus) *kubeflow.ResilientJob {
	t.Helper()
	var (
		newJob *kubeflow.ResilientJob
		err    error
		got    map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus
	)
	if err := wait.Poll(waitInterval, waitTimeout, func() (bool, error) {
		newJob, err = client.KubeflowV2beta1().ResilientJobs(job.Namespace).Get(ctx, job.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		got = newJob.Status.ReplicaStatuses
		return cmp.Equal(want, got), nil
	}); err != nil {
		diff := cmp.Diff(want, got)
		t.Fatalf("Waiting for Job status: %v\n(-want,+got)\n%s", err, diff)
	}
	return newJob
}

func updatePodsToPhase(ctx context.Context, client kubernetes.Interface, pods []*corev1.Pod, phase corev1.PodPhase) error {
	for i, p := range pods {
		p.Status.Phase = phase
		newPod, err := client.CoreV1().Pods(p.Namespace).UpdateStatus(ctx, p, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		pods[i] = newPod
	}
	return nil
}

func updatePodsCondition(ctx context.Context, client kubernetes.Interface, pods []*corev1.Pod, condition corev1.PodCondition) error {
	for i, p := range pods {
		p.Status.Conditions = append(p.Status.Conditions, condition)
		newPod, err := client.CoreV1().Pods(p.Namespace).UpdateStatus(ctx, p, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		pods[i] = newPod
	}
	return nil
}

func getServiceForJob(ctx context.Context, client kubernetes.Interface, job *kubeflow.ResilientJob) (*corev1.Service, error) {
	result, err := client.CoreV1().Services(job.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, obj := range result.Items {
		if obj.Name != job.Name+launcherSuffix {
			if metav1.IsControlledBy(&obj, job) {
				return &obj, nil
			}
		}
	}
	return nil, nil
}

func getConfigMapForJob(ctx context.Context, client kubernetes.Interface, job *kubeflow.ResilientJob) (*corev1.ConfigMap, error) {
	result, err := client.CoreV1().ConfigMaps(job.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, obj := range result.Items {
		if metav1.IsControlledBy(&obj, job) {
			return &obj, nil
		}
	}
	return nil, nil
}

func getSecretForJob(ctx context.Context, client kubernetes.Interface, job *kubeflow.ResilientJob) (*corev1.Secret, error) {
	result, err := client.CoreV1().Secrets(job.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, obj := range result.Items {
		if metav1.IsControlledBy(&obj, job) {
			return &obj, nil
		}
	}
	return nil, nil
}

func getWorkerPodsForJob(ctx context.Context, client kubernetes.Interface, job *kubeflow.ResilientJob) ([]*corev1.Pod, error) {
	result, err := client.CoreV1().Pods(job.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	var pods []*corev1.Pod
	for i, p := range result.Items {
		if p.DeletionTimestamp == nil && metav1.IsControlledBy(&p, job) {
			if p.Name != job.Name+launcherSuffix {
				pods = append(pods, &result.Items[i])
			}
		}
	}
	return pods, nil
}

func getLauncherPodForJob(ctx context.Context, client kubernetes.Interface, mpiJob *kubeflow.ResilientJob) (*corev1.Pod, error) {
	launcher, err := client.CoreV1().Pods(mpiJob.Namespace).Get(ctx, mpiJob.Name+launcherSuffix, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	if metav1.IsControlledBy(launcher, mpiJob) {
		return launcher, nil
	}
	return nil, nil

}

func getSchedPodGroup(ctx context.Context, client schedclientset.Interface, job *kubeflow.ResilientJob) (*schedv1alpha1.PodGroup, error) {
	result, err := client.SchedulingV1alpha1().PodGroups(job.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, pg := range result.Items {
		if metav1.IsControlledBy(&pg, job) {
			return &pg, nil
		}
	}
	return nil, nil
}

func getVolcanoPodGroup(ctx context.Context, client volcanoclient.Interface, job *kubeflow.ResilientJob) (*volcanov1beta1.PodGroup, error) {
	result, err := client.SchedulingV1beta1().PodGroups(job.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, pg := range result.Items {
		if metav1.IsControlledBy(&pg, job) {
			return &pg, nil
		}
	}
	return nil, nil
}

func hasVolumeForSecret(podSpec *corev1.PodSpec, secret *corev1.Secret) bool {
	for _, v := range podSpec.Volumes {
		if v.Secret != nil && v.Secret.SecretName == secret.Name {
			return true
		}
	}
	return false
}

func hasVolumeForConfigMap(podSpec *corev1.PodSpec, cm *corev1.ConfigMap) bool {
	for _, v := range podSpec.Volumes {
		if v.ConfigMap != nil && v.ConfigMap.Name == cm.Name {
			return true
		}
	}
	return false
}

func mpiJobHasCondition(job *kubeflow.ResilientJob, cond kubeflow.JobConditionType) bool {
	return mpiJobHasConditionWithStatus(job, cond, corev1.ConditionTrue)
}

func mpiJobHasConditionWithStatus(job *kubeflow.ResilientJob, cond kubeflow.JobConditionType, status corev1.ConditionStatus) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == cond && c.Status == status {
			return true
		}
	}
	return false
}

func newInt32(v int32) *int32 {
	return &v
}

func eventForJob(event corev1.Event, job *kubeflow.ResilientJob) corev1.Event {
	event.Namespace = job.Namespace
	event.Source.Component = "mpi-job-controller"
	ref, err := reference.GetReference(scheme.Scheme, job)
	runtime.Must(err)
	event.InvolvedObject = *ref
	return event
}
