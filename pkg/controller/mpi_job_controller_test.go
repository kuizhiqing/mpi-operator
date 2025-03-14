// Copyright 2018 The Kubeflow Authors.
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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubeinformers "k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	schedv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	schedclientset "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	volcanov1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	volcanofake "volcano.sh/apis/pkg/client/clientset/versioned/fake"

	kubeflow "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	"github.com/kubeflow/mpi-operator/pkg/client/clientset/versioned/fake"
	"github.com/kubeflow/mpi-operator/pkg/client/clientset/versioned/scheme"
	informers "github.com/kubeflow/mpi-operator/pkg/client/informers/externalversions"
)

const (
	discover_hosts_tmp = "#!/bin/sh\ncat /etc/launch/hostfile | awk '{print $1}'\n"
	ssh_config_tmp     = `LogLevel ERROR
Host *
    Port 36000
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
`
	sshd_config_tmp = `StrictModes no
HostKey /etc/ssh/ssh_host_rsa_key
PermitUserEnvironment yes
AcceptEnv *
UsePAM yes
AllowUsers root
PermitRootLogin yes
Port 36000
ListenAddress 0.0.0.0
`
)

var (
	alwaysReady        = func() bool { return true }
	noResyncPeriodFunc = func() time.Duration { return 0 }

	ignoreConditionTimes = cmpopts.IgnoreFields(kubeflow.JobCondition{}, "LastUpdateTime", "LastTransitionTime")
	ignoreSecretEntries  = cmpopts.IgnoreMapEntries(func(k string, v []uint8) bool { return true })
	ignoreReferences     = cmpopts.IgnoreFields(metav1.ObjectMeta{}, "OwnerReferences")
)

type fixture struct {
	t *testing.T

	client        *fake.Clientset
	kubeClient    *k8sfake.Clientset
	volcanoClient *volcanofake.Clientset
	schedClient   *schedclientset.Clientset

	// Objects to put in the store.
	configMapLister       []*corev1.ConfigMap
	serviceLister         []*corev1.Service
	secretLister          []*corev1.Secret
	volcanoPodGroupLister []*volcanov1beta1.PodGroup
	schedPodGroupLister   []*schedv1alpha1.PodGroup
	jobLister             []*batchv1.Job
	podLister             []*corev1.Pod
	priorityClassLister   []*schedulingv1.PriorityClass
	mpiJobLister          []*kubeflow.MPIJob

	// Actions expected to happen on the client.
	kubeActions []core.Action
	actions     []core.Action

	// Objects from here are pre-loaded into NewSimpleFake.
	kubeObjects []runtime.Object
	objects     []runtime.Object

	gangSchedulingName string
}

func newFixture(t *testing.T, gangSchedulingName string) *fixture {
	f := &fixture{}
	f.t = t
	f.objects = []runtime.Object{}
	f.kubeObjects = []runtime.Object{}
	f.gangSchedulingName = gangSchedulingName
	return f
}

func newMPIJobCommon(name string, startTime, completionTime *metav1.Time) *kubeflow.MPIJob {
	cleanPodPolicyAll := kubeflow.CleanPodPolicyAll
	mpiJob := &kubeflow.MPIJob{
		TypeMeta: metav1.TypeMeta{APIVersion: kubeflow.SchemeGroupVersion.String()},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: kubeflow.MPIJobSpec{
			RunPolicy: kubeflow.RunPolicy{
				CleanPodPolicy: &cleanPodPolicyAll,
			},
			MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
				kubeflow.MPIReplicaTypeLauncher: {
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "foo",
									Image: "bar",
								},
							},
						},
					},
					Replicas: newInt32(1),
				},
			},
		},
		Status: kubeflow.JobStatus{},
	}

	if startTime != nil {
		mpiJob.Status.StartTime = startTime
	}
	if completionTime != nil {
		mpiJob.Status.CompletionTime = completionTime
	}

	return mpiJob
}

func newMPIJob(name string, replicas *int32, startTime, completionTime *metav1.Time) *kubeflow.MPIJob {
	mpiJob := newMPIJobCommon(name, startTime, completionTime)
	if *replicas > 0 {
		mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker] =
			&kubeflow.ReplicaSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "foo",
								Image: "bar",
							},
						},
					},
				},
				Replicas: replicas,
			}
	}
	return mpiJob
}

func (f *fixture) newController(clock clock.WithTicker) (*MPIJobController, informers.SharedInformerFactory, kubeinformers.SharedInformerFactory) {
	f.client = fake.NewSimpleClientset(f.objects...)
	f.kubeClient = k8sfake.NewSimpleClientset(f.kubeObjects...)
	i := informers.NewSharedInformerFactory(f.client, noResyncPeriodFunc())
	k8sI := kubeinformers.NewSharedInformerFactory(f.kubeClient, noResyncPeriodFunc())

	c := NewMPIJobControllerWithClock(
		f.kubeClient,
		f.client,
		f.volcanoClient,
		f.schedClient,
		k8sI.Core().V1().Events(),
		k8sI.Core().V1().ConfigMaps(),
		k8sI.Core().V1().Secrets(),
		k8sI.Core().V1().Services(),
		k8sI.Core().V1().Pods(),
		k8sI.Scheduling().V1().PriorityClasses(),
		i.Kubeflow().V2beta1().MPIJobs(),
		clock,
		metav1.NamespaceAll,
		f.gangSchedulingName,
		"",
		"",
		5,
	)

	c.eventSynced = alwaysReady
	c.configMapSynced = alwaysReady
	c.serviceSynced = alwaysReady
	c.secretSynced = alwaysReady
	c.podSynced = alwaysReady
	c.podGroupSynced = alwaysReady
	c.mpiJobSynced = alwaysReady
	c.recorder = &record.FakeRecorder{}

	for _, configMap := range f.configMapLister {
		err := k8sI.Core().V1().ConfigMaps().Informer().GetIndexer().Add(configMap)
		if err != nil {
			fmt.Println("Failed to create config map")
		}
	}

	for _, service := range f.serviceLister {
		err := k8sI.Core().V1().Services().Informer().GetIndexer().Add(service)
		if err != nil {
			fmt.Println("Failed to create service account")
		}
	}

	for _, secret := range f.secretLister {
		err := k8sI.Core().V1().Secrets().Informer().GetIndexer().Add(secret)
		if err != nil {
			fmt.Println("Failed to create role")
		}
	}

	for _, job := range f.jobLister {
		err := k8sI.Batch().V1().Jobs().Informer().GetIndexer().Add(job)
		if err != nil {
			fmt.Println("Failed to create job")
		}
	}

	for _, pod := range f.podLister {
		err := k8sI.Core().V1().Pods().Informer().GetIndexer().Add(pod)
		if err != nil {
			fmt.Println("Failed to create pod")
		}
	}

	if c.PodGroupCtrl != nil {
		for _, podGroup := range f.volcanoPodGroupLister {
			err := c.PodGroupCtrl.PodGroupSharedIndexInformer().GetIndexer().Add(podGroup)
			if err != nil {
				fmt.Println("Failed to create volcano pod group")
			}
		}
		for _, podGroup := range f.schedPodGroupLister {
			err := c.PodGroupCtrl.PodGroupSharedIndexInformer().GetIndexer().Add(podGroup)
			if err != nil {
				fmt.Println("Failed to create scheduler-plugins pod group")
			}
		}
		for _, priorityClass := range f.priorityClassLister {
			err := k8sI.Scheduling().V1().PriorityClasses().Informer().GetIndexer().Add(priorityClass)
			if err != nil {
				fmt.Println("Failed to create priorityClass")
			}
		}
	}

	for _, mpiJob := range f.mpiJobLister {
		err := i.Kubeflow().V2beta1().MPIJobs().Informer().GetIndexer().Add(mpiJob)
		if err != nil {
			fmt.Println("Failed to create mpijob")
		}
	}

	return c, i, k8sI
}

func (f *fixture) run(mpiJobName string) {
	f.runWithClock(mpiJobName, clock.RealClock{})
}

func (f *fixture) runWithClock(mpiJobName string, clock clock.WithTicker) {
	f.runController(mpiJobName, true, false, clock)
}

func (f *fixture) runExpectError(mpiJobName string) {
	f.runController(mpiJobName, true, true, clock.RealClock{})
}

func (f *fixture) runController(mpiJobName string, startInformers, expectError bool, clock clock.WithTicker) {
	c, i, k8sI := f.newController(clock)
	if startInformers {
		stopCh := make(chan struct{})
		defer close(stopCh)
		i.Start(stopCh)
		k8sI.Start(stopCh)
		if c.PodGroupCtrl != nil {
			c.PodGroupCtrl.StartInformerFactory(stopCh)
		}
	}

	err := c.syncHandler(mpiJobName)
	if !expectError && err != nil {
		f.t.Errorf("error syncing mpi job: %v", err)
	} else if expectError && err == nil {
		f.t.Error("expected error syncing mpi job, got nil")
	}

	actions := filterInformerActions(f.client.Actions())
	for i, action := range actions {
		if len(f.actions) < i+1 {
			f.t.Errorf("%d unexpected actions: %+v", len(actions)-len(f.actions), actions[i:])
			break
		}

		expectedAction := f.actions[i]
		checkAction(expectedAction, action, f.t)
	}

	if len(f.actions) > len(actions) {
		f.t.Errorf("%d additional expected actions:%+v", len(f.actions)-len(actions), f.actions[len(actions):])
	}

	k8sActions := filterInformerActions(f.kubeClient.Actions())
	for i, action := range k8sActions {
		if len(f.kubeActions) < i+1 {
			f.t.Errorf("%d unexpected actions: %+v", len(k8sActions)-len(f.kubeActions), k8sActions[i:])
			break
		}

		expectedAction := f.kubeActions[i]
		checkAction(expectedAction, action, f.t)
	}

	if len(f.kubeActions) > len(k8sActions) {
		f.t.Errorf("%d additional expected actions:%+v", len(f.kubeActions)-len(k8sActions), f.kubeActions[len(k8sActions):])
	}
}

// checkAction verifies that expected and actual actions are equal and both have
// same attached resources
func checkAction(expected, actual core.Action, t *testing.T) {
	if !(expected.Matches(actual.GetVerb(), actual.GetResource().Resource) && actual.GetSubresource() == expected.GetSubresource()) {
		t.Errorf("Expected\n\t%#v\ngot\n\t%#v", expected, actual)
		return
	}

	if reflect.TypeOf(actual) != reflect.TypeOf(expected) {
		t.Errorf("Action has wrong type. Expected: %t. Got: %t", expected, actual)
		return
	}

	//nolint
	switch a := actual.(type) {
	case core.UpdateAction:
		e, _ := expected.(core.UpdateAction)
		expObject := e.GetObject()
		object := a.GetObject()

		if diff := cmp.Diff(expObject, object, ignoreSecretEntries, ignoreConditionTimes); diff != "" {
			t.Errorf("Action %s %s has wrong object (-want +got):\n %s", a.GetVerb(), a.GetResource().Resource, diff)
		}
	case core.CreateAction:
		e, _ := expected.(core.CreateAction)
		expObject := e.GetObject()
		object := a.GetObject()

		if diff := cmp.Diff(expObject, object, ignoreSecretEntries); diff != "" {
			t.Errorf("Action %s %s has wrong object (-want +got):\n %s", a.GetVerb(), a.GetResource().Resource, diff)
		}
	case core.PatchAction:
		e, _ := expected.(core.PatchAction)
		expPatch := e.GetPatch()
		patch := a.GetPatch()

		if diff := cmp.Diff(expPatch, patch); diff != "" {
			t.Errorf("Action %s %s has wrong patch (-want +got):\n %s", a.GetVerb(), a.GetResource().Resource, diff)
		}
	}
}

// filterInformerActions filters list and watch actions for testing resources.
// Since list and watch don't change resource state we can filter it to lower
// nose level in our tests.
func filterInformerActions(actions []core.Action) []core.Action {
	var ret []core.Action
	for _, action := range actions {
		if len(action.GetNamespace()) == 0 && validAction(action) {
			continue
		}
		if action.GetResource().Resource == "events" {
			continue
		}
		ret = append(ret, action)
	}

	return ret
}

func validAction(action core.Action) bool {
	return action.GetVerb() == "list" || action.GetVerb() == "watch"
}

func (f *fixture) expectCreatePodAction(d *corev1.Pod) {
	f.kubeActions = append(f.kubeActions, core.NewCreateAction(schema.GroupVersionResource{Resource: "pods"}, d.Namespace, d))
}

func (f *fixture) expectCreateServiceAction(d *corev1.Service) {
	f.kubeActions = append(f.kubeActions, core.NewCreateAction(schema.GroupVersionResource{Resource: "services"}, d.Namespace, d))
}

func (f *fixture) expectCreateConfigMapAction(d *corev1.ConfigMap) {
	f.kubeActions = append(f.kubeActions, core.NewCreateAction(schema.GroupVersionResource{Resource: "configmaps"}, d.Namespace, d))
}

func (f *fixture) expectCreateSecretAction(d *corev1.Secret) {
	f.kubeActions = append(f.kubeActions, core.NewCreateAction(schema.GroupVersionResource{Resource: "secrets"}, d.Namespace, d))
}

func (f *fixture) expectUpdateMPIJobStatusAction(mpiJob *kubeflow.MPIJob) {
	action := core.NewUpdateAction(schema.GroupVersionResource{Resource: "mpijobs"}, mpiJob.Namespace, mpiJob)
	action.Subresource = "status"
	f.actions = append(f.actions, action)
}

func (f *fixture) setUpMPIJob(mpiJob *kubeflow.MPIJob) {
	f.mpiJobLister = append(f.mpiJobLister, mpiJob)
	f.objects = append(f.objects, mpiJob)
}

func (f *fixture) setUpPod(pod *corev1.Pod) {
	f.podLister = append(f.podLister, pod)
	f.kubeObjects = append(f.kubeObjects, pod)
}

func (f *fixture) setUpConfigMap(configMap *corev1.ConfigMap) {
	f.configMapLister = append(f.configMapLister, configMap)
	f.kubeObjects = append(f.kubeObjects, configMap)
}

func (f *fixture) setUpService(service *corev1.Service) {
	f.serviceLister = append(f.serviceLister, service)
	f.kubeObjects = append(f.kubeObjects, service)
}

func (f *fixture) setUpSecret(secret *corev1.Secret) {
	f.secretLister = append(f.secretLister, secret)
	f.kubeObjects = append(f.kubeObjects, secret)
}

func (f *fixture) setUpPriorityClass(priorityClass *schedulingv1.PriorityClass) {
	f.priorityClassLister = append(f.priorityClassLister, priorityClass)
	f.kubeObjects = append(f.kubeObjects, priorityClass)
}

func setUpMPIJobTimestamp(mpiJob *kubeflow.MPIJob, startTime, completionTime *metav1.Time) {
	if startTime != nil {
		mpiJob.Status.StartTime = startTime
	}

	if completionTime != nil {
		mpiJob.Status.CompletionTime = completionTime
	}
}

func getKey(mpiJob *kubeflow.MPIJob, t *testing.T) string {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(mpiJob)
	if err != nil {
		t.Errorf("Unexpected error getting key for mpi job %v: %v", mpiJob.Name, err)
		return ""
	}
	return key
}

func TestDoNothingWithInvalidKey(t *testing.T) {
	f := newFixture(t, "")
	f.run("foo/bar/baz")
}

func TestDoNothingWithNonexistentMPIJob(t *testing.T) {
	f := newFixture(t, "")
	startTime := metav1.Now()
	completionTime := metav1.Now()
	mpiJob := newMPIJob("test", newInt32(64), &startTime, &completionTime)
	f.run(getKey(mpiJob, t))
}

func TestDoNothingWithInvalidMPIJob(t *testing.T) {
	f := newFixture(t, "")
	// An empty MPIJob doesn't pass validation.
	mpiJob := &kubeflow.MPIJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "bar",
		},
	}
	f.setUpMPIJob(mpiJob)
	f.run(getKey(mpiJob, t))
}

func TestLauncherNotControlledByUs(t *testing.T) {
	f := newFixture(t, "")
	startTime := metav1.Now()
	completionTime := metav1.Now()

	mpiJob := newMPIJob("test", newInt32(64), &startTime, &completionTime)
	f.setUpMPIJob(mpiJob)

	fmjc := f.newFakeMPIJobController()
	mpiJobCopy := mpiJob.DeepCopy()
	scheme.Scheme.Default(mpiJobCopy)
	launcher := fmjc.newLauncherPod(mpiJobCopy)
	launcher.OwnerReferences = nil
	f.setUpPod(launcher)

	f.runExpectError(getKey(mpiJob, t))
}

func TestConfigMapNotControlledByUs(t *testing.T) {
	f := newFixture(t, "")
	startTime := metav1.Now()
	completionTime := metav1.Now()

	var replicas int32 = 64
	mpiJob := newMPIJob("test", &replicas, &startTime, &completionTime)
	f.setUpMPIJob(mpiJob)
	f.setUpService(newJobService(mpiJob))

	configMap := newConfigMap(mpiJob)
	configMap.OwnerReferences = nil
	f.setUpConfigMap(configMap)

	f.runExpectError(getKey(mpiJob, t))
}

func TestWorkerServiceNotControlledByUs(t *testing.T) {
	f := newFixture(t, "")
	startTime := metav1.Now()
	completionTime := metav1.Now()

	var replicas int32 = 2
	mpiJob := newMPIJob("test", &replicas, &startTime, &completionTime)
	f.setUpMPIJob(mpiJob)

	mpiJobCopy := mpiJob.DeepCopy()
	scheme.Scheme.Default(mpiJobCopy)
	service := newJobService(mpiJobCopy)
	service.OwnerReferences = nil
	f.setUpService(service)

	f.runExpectError(getKey(mpiJob, t))
}

func TestLauncherServiceNotControlledByUs(t *testing.T) {
	f := newFixture(t, "")
	startTime := metav1.Now()
	completionTime := metav1.Now()

	var replicas int32 = 2
	mpiJob := newMPIJob("test", &replicas, &startTime, &completionTime)
	mpiJob.Spec.MPIImplementation = kubeflow.MPIImplementationIntel
	f.setUpMPIJob(mpiJob)

	mpiJobCopy := mpiJob.DeepCopy()
	scheme.Scheme.Default(mpiJobCopy)
	service := newJobService(mpiJobCopy)
	service.OwnerReferences = nil
	f.setUpService(service)
	configMap := newConfigMap(mpiJobCopy)
	secret, err := newSSHAuthSecret(mpiJobCopy)
	tjName := fmt.Sprintf("%s-%s", mpiJob.Name, launch)
	tjCM := newLaunchConfig(mpiJobCopy, tjName)
	if err != nil {
		t.Fatalf("Creating SSH auth Secret: %v", err)
	}
	f.setUpSecret(secret)
	f.setUpConfigMap(configMap)
	f.setUpConfigMap(tjCM)
	fmjc := f.newFakeMPIJobController()
	for i := 0; i < int(replicas); i++ {
		worker := fmjc.newReplicas(mpiJobCopy, i, kubeflow.MPIReplicaTypeWorker)
		f.setUpPod(worker)
	}

	f.runExpectError(getKey(mpiJob, t))
}

func TestSecretNotControlledByUs(t *testing.T) {
	f := newFixture(t, "")
	startTime := metav1.Now()
	completionTime := metav1.Now()

	var replicas int32 = 64
	mpiJob := newMPIJob("test", &replicas, &startTime, &completionTime)
	f.setUpMPIJob(mpiJob)

	mpiJobCopy := mpiJob.DeepCopy()
	scheme.Scheme.Default(mpiJobCopy)
	configMap := newConfigMap(mpiJobCopy)
	f.setUpConfigMap(configMap)
	f.setUpService(newJobService(mpiJobCopy))

	secret, err := newSSHAuthSecret(mpiJobCopy)
	if err != nil {
		t.Fatalf("Creating SSH auth Secret: %v", err)
	}
	secret.OwnerReferences = nil
	f.setUpSecret(secret)

	f.runExpectError(getKey(mpiJob, t))
}

func TestPriorityCheck(t *testing.T) {
	f := newFixture(t, "")
	startTime := metav1.Now()
	completionTime := metav1.Now()

	mpiJob := newMPIJob("test", newInt32(64), &startTime, &completionTime)
	f.setUpMPIJob(mpiJob)

	fmjc := f.newFakeMPIJobController()
	mpiJobCopy := mpiJob.DeepCopy()
	scheme.Scheme.Default(mpiJobCopy)
	launcher := fmjc.newLauncherPod(mpiJobCopy)
	launcher.OwnerReferences = nil
	f.setUpPod(launcher)

	f.runExpectError(getKey(mpiJob, t))
}

func TestAllResourcesCreated(t *testing.T) {
	impls := []kubeflow.MPIImplementation{kubeflow.MPIImplementationOpenMPI, kubeflow.MPIImplementationIntel, kubeflow.MPIImplementationMPICH}
	for _, implementation := range impls {
		t.Run(string(implementation), func(t *testing.T) {
			f := newFixture(t, "")
			now := metav1.Now()
			mpiJob := newMPIJob("foo", newInt32(5), &now, nil)
			mpiJob.Spec.MPIImplementation = implementation
			f.setUpMPIJob(mpiJob)

			fmjc := f.newFakeMPIJobController()
			mpiJobCopy := mpiJob.DeepCopy()
			scheme.Scheme.Default(mpiJobCopy)
			f.expectCreateServiceAction(newJobService(mpiJobCopy))
			cfgMap := newConfigMap(mpiJobCopy)
			f.expectCreateConfigMapAction(cfgMap)
			secret, err := newSSHAuthSecret(mpiJobCopy)
			if err != nil {
				t.Fatalf("Failed creating secret")
			}
			f.expectCreateSecretAction(secret)
			tjName := fmt.Sprintf("%s-%s", mpiJob.Name, launch)
			tjCM := newLaunchConfig(mpiJobCopy, tjName)
			f.expectCreateConfigMapAction(tjCM)
			for i := 0; i < 5; i++ {
				f.expectCreatePodAction(fmjc.newReplicas(mpiJobCopy, i, kubeflow.MPIReplicaTypeWorker))
			}
			f.expectCreatePodAction(fmjc.newLauncherPod(mpiJobCopy))

			mpiJobCopy.Status.Conditions = []kubeflow.JobCondition{newCondition(kubeflow.JobCreated, corev1.ConditionTrue, mpiJobCreatedReason, "MPIJob default/foo is created.")}
			mpiJobCopy.Status.ReplicaStatuses = map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
				kubeflow.MPIReplicaTypeLauncher: {},
				kubeflow.MPIReplicaTypeWorker:   {},
				kubeflow.MPIReplicaTypeHorker:   {},
			}
			f.expectUpdateMPIJobStatusAction(mpiJobCopy)

			f.run(getKey(mpiJob, t))
		})
	}
}

func TestLauncherSucceeded(t *testing.T) {
	f := newFixture(t, "")

	startTime := metav1.Now()
	completionTime := metav1.Now()

	mpiJob := newMPIJob("test", newInt32(64), &startTime, &completionTime)
	f.setUpMPIJob(mpiJob)

	fmjc := f.newFakeMPIJobController()
	mpiJobCopy := mpiJob.DeepCopy()
	scheme.Scheme.Default(mpiJobCopy)
	launcher := fmjc.newLauncherPod(mpiJobCopy)
	launcher.Status.Phase = corev1.PodSucceeded
	f.setUpPod(launcher)

	mpiJobCopy.Status.ReplicaStatuses = map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Active:    0,
			Succeeded: 1,
			Failed:    0,
		},
		kubeflow.MPIReplicaTypeWorker: {},
		kubeflow.MPIReplicaTypeHorker: {},
	}

	setUpMPIJobTimestamp(mpiJobCopy, &startTime, &completionTime)

	msg := fmt.Sprintf("MPIJob %s/%s is created.", mpiJob.Namespace, mpiJob.Name)
	updateMPIJobConditions(mpiJobCopy, kubeflow.JobCreated, corev1.ConditionTrue, mpiJobCreatedReason, msg)
	msg = fmt.Sprintf("MPIJob %s/%s successfully completed.", mpiJob.Namespace, mpiJob.Name)
	updateMPIJobConditions(mpiJobCopy, kubeflow.JobSucceeded, corev1.ConditionTrue, mpiJobSucceededReason, msg)
	f.expectUpdateMPIJobStatusAction(mpiJobCopy)

	f.run(getKey(mpiJob, t))
}

func TestLauncherFailed(t *testing.T) {
	f := newFixture(t, "")
	startTime := metav1.Now()
	completionTime := metav1.Now()

	mpiJob := newMPIJob("test", newInt32(64), &startTime, &completionTime)
	f.setUpMPIJob(mpiJob)

	fmjc := f.newFakeMPIJobController()
	mpiJobCopy := mpiJob.DeepCopy()
	scheme.Scheme.Default(mpiJobCopy)
	launcher := fmjc.newLauncherPod(mpiJobCopy)
	now := time.Now()
	launcher.Status.Phase = corev1.PodFailed
	launcher.Status.Reason = "FailedReason1"
	launcher.Status.Message = "first message"
	launcher.CreationTimestamp = metav1.NewTime(now)
	f.setUpPod(launcher)

	mpiJobCopy.Status.ReplicaStatuses = map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Active:    0,
			Succeeded: 0,
			Failed:    1,
		},
		kubeflow.MPIReplicaTypeWorker: {},
		kubeflow.MPIReplicaTypeHorker: {},
	}
	setUpMPIJobTimestamp(mpiJobCopy, &startTime, &completionTime)

	msg := fmt.Sprintf("MPIJob %s/%s is created.", mpiJob.Namespace, mpiJob.Name)
	updateMPIJobConditions(mpiJobCopy, kubeflow.JobCreated, corev1.ConditionTrue, mpiJobCreatedReason, msg)
	// Reason:  "launcher-failed: FailedReason1. first message."
	// Message: "MPIJob default/test failed: launcher-failed: FailedReason1. first message."
	_, reason, message := fmjc.checkJobFailedWithReason(mpiJobCopy, launcher, nil, nil)
	msg = fmt.Sprintf("MPIJob %s/%s failed: %s", mpiJob.Namespace, mpiJob.Name, message)
	updateMPIJobConditions(mpiJobCopy, kubeflow.JobFailed, corev1.ConditionTrue, reason, msg)

	f.expectUpdateMPIJobStatusAction(mpiJobCopy)

	f.run(getKey(mpiJob, t))
}

func TestLauncherActiveWorkerNotReady(t *testing.T) {
	f := newFixture(t, "")
	startTime := metav1.Now()
	completionTime := metav1.Now()

	var replicas int32 = 8
	mpiJob := newMPIJob("test", &replicas, &startTime, &completionTime)
	f.setUpMPIJob(mpiJob)

	mpiJobCopy := mpiJob.DeepCopy()
	scheme.Scheme.Default(mpiJobCopy)
	configMap := newConfigMap(mpiJobCopy)
	f.setUpConfigMap(configMap)
	tjName := fmt.Sprintf("%s-%s", mpiJob.Name, launch)
	tjCM := newLaunchConfig(mpiJobCopy, tjName)
	f.setUpConfigMap(tjCM)
	f.setUpService(newJobService(mpiJobCopy))
	secret, err := newSSHAuthSecret(mpiJobCopy)
	if err != nil {
		t.Fatalf("Creating SSH auth secret: %v", err)
	}
	f.setUpSecret(secret)

	fmjc := f.newFakeMPIJobController()
	launcher := fmjc.newLauncherPod(mpiJobCopy)
	launcher.Status.Phase = corev1.PodRunning
	f.setUpPod(launcher)

	for i := 0; i < int(replicas); i++ {
		worker := fmjc.newReplicas(mpiJobCopy, i, kubeflow.MPIReplicaTypeWorker)
		worker.Status.Phase = corev1.PodPending
		f.setUpPod(worker)
	}
	msg := fmt.Sprintf("MPIJob %s/%s is created.", mpiJob.Namespace, mpiJob.Name)
	updateMPIJobConditions(mpiJobCopy, kubeflow.JobCreated, corev1.ConditionTrue, mpiJobCreatedReason, msg)
	mpiJobCopy.Status.ReplicaStatuses = map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Active:    1,
			Succeeded: 0,
			Failed:    0,
		},
		kubeflow.MPIReplicaTypeWorker: {
			Active:    0,
			Succeeded: 0,
			Failed:    0,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	}
	setUpMPIJobTimestamp(mpiJobCopy, &startTime, &completionTime)
	f.expectUpdateMPIJobStatusAction(mpiJobCopy)

	f.run(getKey(mpiJob, t))
}

func TestLauncherActiveWorkerReady(t *testing.T) {
	f := newFixture(t, "")
	startTime := metav1.Now()
	// completed job can not be running
	// completionTime := metav1.Now()

	var replicas int32 = 8
	mpiJob := newMPIJob("test", &replicas, &startTime, nil)
	f.setUpMPIJob(mpiJob)

	mpiJobCopy := mpiJob.DeepCopy()
	scheme.Scheme.Default(mpiJobCopy)
	f.setUpService(newJobService(mpiJobCopy))
	secret, err := newSSHAuthSecret(mpiJobCopy)
	if err != nil {
		t.Fatalf("Creating SSH auth secret: %v", err)
	}
	f.setUpSecret(secret)

	fmjc := f.newFakeMPIJobController()
	launcher := fmjc.newLauncherPod(mpiJobCopy)
	launcher.Status.Phase = corev1.PodRunning
	f.setUpPod(launcher)

	for i := 0; i < int(replicas); i++ {
		worker := fmjc.newReplicas(mpiJobCopy, i, kubeflow.MPIReplicaTypeWorker)
		worker.Status.Phase = corev1.PodRunning
		f.setUpPod(worker)
	}

	configMap := newConfigMap(mpiJobCopy)
	f.setUpConfigMap(configMap)
	tjName := fmt.Sprintf("%s-%s", mpiJob.Name, launch)
	tjCM := newLaunchConfig(mpiJobCopy, tjName)
	f.setUpConfigMap(tjCM)

	mpiJobCopy.Status.ReplicaStatuses = map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
		kubeflow.MPIReplicaTypeLauncher: {
			Active:    1,
			Succeeded: 0,
			Failed:    0,
		},
		kubeflow.MPIReplicaTypeWorker: {
			Active:    8,
			Succeeded: 0,
			Failed:    0,
		},
		kubeflow.MPIReplicaTypeHorker: {},
	}
	setUpMPIJobTimestamp(mpiJobCopy, &startTime, nil)
	msg := fmt.Sprintf("MPIJob %s/%s is created.", mpiJob.Namespace, mpiJob.Name)
	updateMPIJobConditions(mpiJobCopy, kubeflow.JobCreated, corev1.ConditionTrue, mpiJobCreatedReason, msg)
	msg = fmt.Sprintf("MPIJob %s/%s is running.", mpiJob.Namespace, mpiJob.Name)
	updateMPIJobConditions(mpiJobCopy, kubeflow.JobRunning, corev1.ConditionTrue, mpiJobRunningReason, msg)
	f.expectUpdateMPIJobStatusAction(mpiJobCopy)

	f.run(getKey(mpiJob, t))
}

func TestWorkerNotControlledByUs(t *testing.T) {
	f := newFixture(t, "")
	startTime := metav1.Now()
	completionTime := metav1.Now()

	var replicas int32 = 8
	mpiJob := newMPIJob("test", &replicas, &startTime, &completionTime)
	f.setUpMPIJob(mpiJob)

	mpiJobCopy := mpiJob.DeepCopy()
	scheme.Scheme.Default(mpiJobCopy)
	configMap := newConfigMap(mpiJobCopy)
	f.setUpConfigMap(configMap)
	tjName := fmt.Sprintf("%s-%s", mpiJob.Name, launch)
	tjCM := newLaunchConfig(mpiJobCopy, tjName)
	f.setUpConfigMap(tjCM)
	f.setUpService(newJobService(mpiJobCopy))
	secret, err := newSSHAuthSecret(mpiJobCopy)
	if err != nil {
		t.Fatalf("Creating SSH auth secret: %v", err)
	}
	f.setUpSecret(secret)
	fmjc := f.newFakeMPIJobController()

	for i := 0; i < int(replicas); i++ {
		worker := fmjc.newReplicas(mpiJobCopy, i, kubeflow.MPIReplicaTypeWorker)
		worker.OwnerReferences = nil
		f.setUpPod(worker)
	}

	f.runExpectError(getKey(mpiJob, t))
}

func TestNewConfigMap(t *testing.T) {
	testCases := map[string]struct {
		mpiJob         *kubeflow.MPIJob
		workerReplicas int32
		wantCM         *corev1.ConfigMap
	}{
		"configmap": {
			mpiJob: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "launch-cm",
					Namespace: "tenant-a",
				},
				Spec: kubeflow.MPIJobSpec{
					MPIImplementation: kubeflow.MPIImplementationOpenMPI,
				},
			},
			workerReplicas: 2,
			wantCM: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "launch-cm" + configSuffix,
					Namespace: "tenant-a",
					Labels: map[string]string{
						"app": "launch-cm",
					},
				},
				Data: map[string]string{
					"discover_hosts.sh": discover_hosts_tmp,
					"ssh_config":        ssh_config_tmp,
					"sshd_config":       sshd_config_tmp,
					"environ":           "",
					"hostfile":          "launch-cm-launcher.launch-cm.tenant-a slots=1\n",
				},
			},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cm := newConfigMap(tc.mpiJob)
			if !metav1.IsControlledBy(cm, tc.mpiJob) {
				t.Errorf("Created configMap is not controlled by MPIJob")
			}
			if diff := cmp.Diff(tc.wantCM, cm, ignoreReferences); len(diff) != 0 {
				t.Errorf("Unexpected configMap (-want,+got):\n%s", diff)
			}
		})
	}
}

func (f *fixture) newFakeMPIJobController() *MPIJobController {
	kubeClient := k8sfake.NewSimpleClientset(f.kubeObjects...)

	k8sI := kubeinformers.NewSharedInformerFactory(kubeClient, noResyncPeriodFunc())
	return &MPIJobController{
		recorder:  &record.FakeRecorder{},
		podLister: k8sI.Core().V1().Pods().Lister(),
	}
}
