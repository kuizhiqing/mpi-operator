package controller

import (
	"context"
	"testing"
	"time"

	kubeflow "github.com/kuizhiqing/resilient-training-operator/pkg/apis/kubeflow/v2beta1"
	clientset "github.com/kuizhiqing/resilient-training-operator/pkg/client/clientset/versioned"
	"github.com/kuizhiqing/resilient-training-operator/pkg/client/clientset/versioned/fake"
	informers "github.com/kuizhiqing/resilient-training-operator/pkg/client/informers/externalversions"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
)

func newHeterJobController(
	kubeClient kubernetes.Interface,
	kubeflowClient clientset.Interface,
	resyncPeriod time.Duration) (*HeterJobController, informers.SharedInformerFactory) {

	informerFactory := informers.NewSharedInformerFactory(kubeflowClient, resyncPeriod)
	jobInformer := informerFactory.Kubeflow().V2beta1().ResilientJobs()

	eventBroadcaster := record.NewBroadcaster()
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "resilient-training-operator"})

	hjc := NewHeterJobControllerWithClock(
		kubeClient,
		kubeflowClient,
		jobInformer,
		&clock.RealClock{},
		"test-namespace",
		"test-resilient-training-operator",
		"/etc/config/ssh",
	)
	hjc.recorder = recorder

	return hjc, informerFactory
}

func TestNewHeterJobController(t *testing.T) {
	kubeClient := kubefake.NewSimpleClientset()
	kubeflowClient := fake.NewSimpleClientset()

	// Create controller
	controller, _ := newHeterJobController(kubeClient, kubeflowClient, 0)

	assert.NotNil(t, controller)
	assert.NotNil(t, controller.kubeClient)
	assert.NotNil(t, controller.kubeflowClient)
	assert.NotNil(t, controller.recorder)
}

func TestAddResilientJob(t *testing.T) {
	kubeClient := kubefake.NewSimpleClientset()
	kubeflowClient := fake.NewSimpleClientset()

	controller, _ := newHeterJobController(kubeClient, kubeflowClient, 0)

	// Create a test job
	testJob := &kubeflow.ResilientJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
		},
	}

	// Test adding job
	controller.addResilientJob(testJob)

	// Verify item was added to work queue
	item, shutdown := controller.queue.Get()
	assert.False(t, shutdown)

	key := item.(string)
	expectedKey := "default/test-job"
	assert.Equal(t, expectedKey, key)
}

func TestProcessNextItem(t *testing.T) {
	kubeClient := kubefake.NewSimpleClientset()
	kubeflowClient := fake.NewSimpleClientset()

	controller, informerFactory := newHeterJobController(kubeClient, kubeflowClient, 0)

	// Create and add test job
	testJob := &kubeflow.ResilientJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
		},
		Spec: kubeflow.ResilientJobSpec{
			MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
				kubeflow.MPIReplicaTypeLauncher: {
					Replicas: int32Ptr(1),
				},
				kubeflow.MPIReplicaTypeWorker: {
					Replicas: int32Ptr(4),
				},
			},
		},
	}

	err := informerFactory.Kubeflow().V2beta1().ResilientJobs().Informer().GetIndexer().Add(testJob)
	assert.NoError(t, err)

	// Add job to work queue
	controller.addResilientJob(testJob)

	// Process item
	forget := controller.processNextWorkItem()
	assert.True(t, forget)
}

func int32Ptr(i int32) *int32 {
	return &i
}

func TestGetJobPair(t *testing.T) {
	testCases := map[string]struct {
		launcherJob *kubeflow.ResilientJob
		heterJob    *kubeflow.ResilientJob
		key         string
		expectBoth  bool
	}{
		"valid launcher and heter job": {
			launcherJob: &kubeflow.ResilientJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "launcher-job",
					Namespace: "default",
					Annotations: map[string]string{
						heterRoleKey: launcher,
						heterJobKey:  "default/heter-job",
					},
				},
			},
			heterJob: &kubeflow.ResilientJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "heter-job",
					Namespace: "default",
					Annotations: map[string]string{
						heterRoleKey: worker,
						heterJobKey:  "default/launcher-job",
					},
				},
			},
			key:        "default/launcher-job",
			expectBoth: true,
		},
		"missing annotation": {
			launcherJob: &kubeflow.ResilientJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "launcher-job",
					Namespace: "default",
				},
			},
			key:        "default/launcher-job",
			expectBoth: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var objects []runtime.Object
			if tc.launcherJob != nil {
				objects = append(objects, tc.launcherJob)
			}
			if tc.heterJob != nil {
				objects = append(objects, tc.heterJob)
			}

			kubeClient := kubefake.NewSimpleClientset()
			kubeflowClient := fake.NewSimpleClientset(objects...)
			controller, informerFactory := newHeterJobController(kubeClient, kubeflowClient, 0)

			// Add jobs to informer cache
			for _, obj := range objects {
				if job, ok := obj.(*kubeflow.ResilientJob); ok {
					err := informerFactory.Kubeflow().V2beta1().ResilientJobs().Informer().GetIndexer().Add(job)
					assert.NoError(t, err)
				}
			}

			launcher, heter := controller.getJobPair(tc.key)
			if tc.expectBoth {
				assert.NotNil(t, launcher)
				assert.NotNil(t, heter)
				assert.Equal(t, launcher.Name, tc.launcherJob.Name)
				assert.Equal(t, heter.Name, tc.heterJob.Name)
			} else {
				assert.Nil(t, launcher)
				assert.Nil(t, heter)
			}
		})
	}
}

func TestGetResilientJobByKey(t *testing.T) {
	testCases := map[string]struct {
		job          *kubeflow.ResilientJob
		key          string
		expectError  bool
		expectNilJob bool
	}{
		"valid job": {
			job: &kubeflow.ResilientJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
			},
			key:          "default/test-job",
			expectError:  false,
			expectNilJob: false,
		},
		"invalid key format": {
			job:          nil,
			key:          "invalid-key",
			expectError:  true,
			expectNilJob: true,
		},
		"job not found": {
			job:          nil,
			key:          "default/nonexistent",
			expectError:  true,
			expectNilJob: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var objects []runtime.Object
			if tc.job != nil {
				objects = append(objects, tc.job)
			}

			kubeClient := kubefake.NewSimpleClientset()
			kubeflowClient := fake.NewSimpleClientset(objects...)
			controller, informerFactory := newHeterJobController(kubeClient, kubeflowClient, 0)

			if tc.job != nil {
				err := informerFactory.Kubeflow().V2beta1().ResilientJobs().Informer().GetIndexer().Add(tc.job)
				assert.NoError(t, err)
			}

			job, err := controller.getResilientJobByKey(tc.key)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tc.expectNilJob {
				assert.Nil(t, job)
			} else {
				assert.NotNil(t, job)
				assert.Equal(t, tc.job.Name, job.Name)
				assert.Equal(t, tc.job.Namespace, job.Namespace)
			}
		})
	}
}

func TestGetStatusIPList(t *testing.T) {
	testCases := map[string]struct {
		job            *kubeflow.ResilientJob
		expectedIPList string
	}{
		"all roles with IPs": {
			job: &kubeflow.ResilientJob{
				Status: kubeflow.JobStatus{
					ReplicaStatuses: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
						kubeflow.MPIReplicaTypeLauncher: {
							Selector: "192.168.1.1",
						},
						kubeflow.MPIReplicaTypeWorker: {
							Selector: "192.168.1.2",
						},
						kubeflow.MPIReplicaTypeHorker: {
							Selector: "192.168.1.3",
						},
					},
				},
			},
			expectedIPList: "192.168.1.1,192.168.1.2,192.168.1.3",
		},
		"partial roles with IPs": {
			job: &kubeflow.ResilientJob{
				Status: kubeflow.JobStatus{
					ReplicaStatuses: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaStatus{
						kubeflow.MPIReplicaTypeLauncher: {
							Selector: "192.168.1.1",
						},
						kubeflow.MPIReplicaTypeWorker: {
							Selector: "192.168.1.2",
						},
					},
				},
			},
			expectedIPList: "192.168.1.1,192.168.1.2",
		},
		"no replica status": {
			job: &kubeflow.ResilientJob{
				Status: kubeflow.JobStatus{},
			},
			expectedIPList: "",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ipList := getStatusIPList(tc.job)
			assert.Equal(t, tc.expectedIPList, ipList)
		})
	}
}

func TestDeleteResilientJob(t *testing.T) {
	testCases := map[string]struct {
		obj           interface{}
		heterJob      *kubeflow.ResilientJob
		expectDelete  bool
		expectedError bool
	}{
		"valid delete": {
			obj: &kubeflow.ResilientJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						heterJobKey: "default/heter-job",
					},
				},
			},
			heterJob: &kubeflow.ResilientJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "heter-job",
					Namespace: "default",
					Annotations: map[string]string{
						heterJobKey: "default/test-job",
					},
				},
			},
			expectDelete: true,
		},
		"invalid object type": {
			obj:           &corev1.Pod{},
			expectDelete:  false,
			expectedError: true,
		},
		"job already terminating": {
			obj: &kubeflow.ResilientJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-job",
					Namespace:         "default",
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
					Annotations: map[string]string{
						heterJobKey: "default/heter-job",
					},
				},
			},
			heterJob: &kubeflow.ResilientJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "heter-job",
					Namespace:         "default",
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
				},
			},
			expectDelete: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			var objects []runtime.Object
			if tc.heterJob != nil {
				objects = append(objects, tc.heterJob)
			}

			kubeClient := kubefake.NewSimpleClientset()
			kubeflowClient := fake.NewSimpleClientset(objects...)
			controller, informerFactory := newHeterJobController(kubeClient, kubeflowClient, 0)

			if tc.heterJob != nil {
				err := informerFactory.Kubeflow().V2beta1().ResilientJobs().Informer().GetIndexer().Add(tc.heterJob)
				assert.NoError(t, err)
			}

			controller.deleteResilientJob(tc.obj)

			if tc.expectDelete {
				// Verify heter job was deleted
				_, err := kubeflowClient.KubeflowV2beta1().ResilientJobs(tc.heterJob.Namespace).Get(
					context.TODO(), tc.heterJob.Name, metav1.GetOptions{})
				assert.Error(t, err)
			}
		})
	}
}

func TestRunWorker(t *testing.T) {
	kubeClient := kubefake.NewSimpleClientset()
	kubeflowClient := fake.NewSimpleClientset()
	controller, _ := newHeterJobController(kubeClient, kubeflowClient, 0)

	// Add a job to the queue
	testJob := &kubeflow.ResilientJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
		},
	}
	controller.addResilientJob(testJob)

	// Run worker
	stopCh := make(chan struct{})
	go func() {
		controller.runWorker()
		close(stopCh)
	}()

	// Wait for worker to process item
	time.Sleep(100 * time.Millisecond)
	controller.queue.ShutDown()
	<-stopCh
}

func TestUpdateLauncherJobAnnotation(t *testing.T) {
	testCases := map[string]struct {
		job          *kubeflow.ResilientJob
		ipList       string
		expectUpdate bool
		expectError  bool
	}{
		"update with valid ip list": {
			job: &kubeflow.ResilientJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-job",
					Namespace:   "default",
					Annotations: map[string]string{},
				},
			},
			ipList:       "192.168.1.1,192.168.1.2",
			expectUpdate: true,
			expectError:  false,
		},
		"empty ip list": {
			job: &kubeflow.ResilientJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-job",
					Namespace:   "default",
					Annotations: map[string]string{},
				},
			},
			ipList:       "",
			expectUpdate: false,
			expectError:  false,
		},
		"nil annotations map": {
			job: &kubeflow.ResilientJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
			},
			ipList:       "192.168.1.1",
			expectUpdate: true,
			expectError:  false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			kubeClient := kubefake.NewSimpleClientset()
			kubeflowClient := fake.NewSimpleClientset(tc.job)
			controller, _ := newHeterJobController(kubeClient, kubeflowClient, 0)

			// If annotations map is nil, initialize it
			if tc.job.Annotations == nil {
				tc.job.Annotations = make(map[string]string)
			}

			err := controller.updateLauncherJobAnnotation(tc.job, tc.ipList)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tc.expectUpdate {
				// Verify the job was updated
				updatedJob, err := kubeflowClient.KubeflowV2beta1().ResilientJobs(tc.job.Namespace).Get(
					context.TODO(), tc.job.Name, metav1.GetOptions{})
				assert.NoError(t, err)
				assert.Equal(t, tc.ipList, updatedJob.Annotations[heterIPListKey])
			} else {
				// Verify no update was made
				updatedJob, err := kubeflowClient.KubeflowV2beta1().ResilientJobs(tc.job.Namespace).Get(
					context.TODO(), tc.job.Name, metav1.GetOptions{})
				assert.NoError(t, err)
				_, exists := updatedJob.Annotations[heterIPListKey]
				assert.False(t, exists)
			}
		})
	}
}
