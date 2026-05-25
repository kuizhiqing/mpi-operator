package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	kubeflow "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	corelister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/pointer"
)

func newFakeRecorder() record.EventRecorder {
	return record.NewFakeRecorder(100)
}

func TestConfigMap(t *testing.T) {
	testCases := map[string]struct {
		mpiJob *kubeflow.MPIJob
		result string
	}{
		"default user without password": {
			mpiJob: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
				},
				Spec: kubeflow.MPIJobSpec{
					RunPolicy: kubeflow.RunPolicy{
						SchedulingPolicy: &kubeflow.SchedulingPolicy{
							MinAvailable:  pointer.Int32(2),
							Queue:         "project-y",
							PriorityClass: "high",
						},
					},
					MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
						kubeflow.MPIReplicaTypeLauncher: {
							Replicas: pointer.Int32(1),
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{
										Resources: corev1.ResourceRequirements{
											Requests: corev1.ResourceList{
												corev1.ResourceCPU:    resource.MustParse("1"),
												corev1.ResourceMemory: resource.MustParse("2Gi"),
											},
										},
									}},
								},
							},
						},
						kubeflow.MPIReplicaTypeWorker: {
							Replicas: pointer.Int32(1000),
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{
										Resources: corev1.ResourceRequirements{
											Requests: corev1.ResourceList{
												corev1.ResourceCPU:    resource.MustParse("10"),
												corev1.ResourceMemory: resource.MustParse("20Gi"),
											},
										},
									}},
								},
							},
						},
					},
				},
			},
			result: `StrictModes no
HostKey /etc/ssh/ssh_host_rsa_key
PermitUserEnvironment yes
AcceptEnv *
UsePAM yes
AllowUsers root
PermitRootLogin yes
PasswordAuthentication no
Port 36000
ListenAddress 0.0.0.0
`,
		},
		"default user with password": {
			mpiJob: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test",
				},
				Spec: kubeflow.MPIJobSpec{
					RunPolicy: kubeflow.RunPolicy{
						SchedulingPolicy: &kubeflow.SchedulingPolicy{
							MinAvailable:  pointer.Int32(2),
							Queue:         "project-y",
							PriorityClass: "high",
						},
					},
					MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
						kubeflow.MPIReplicaTypeLauncher: {
							Replicas: pointer.Int32(1),
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{
										Resources: corev1.ResourceRequirements{
											Requests: corev1.ResourceList{
												corev1.ResourceCPU:    resource.MustParse("1"),
												corev1.ResourceMemory: resource.MustParse("2Gi"),
											},
										},
										Env: []corev1.EnvVar{
											{
												Name:  "PASSWORD",
												Value: "fake",
											},
										},
									}},
								},
							},
						},
						kubeflow.MPIReplicaTypeWorker: {
							Replicas: pointer.Int32(1000),
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{
										Resources: corev1.ResourceRequirements{
											Requests: corev1.ResourceList{
												corev1.ResourceCPU:    resource.MustParse("10"),
												corev1.ResourceMemory: resource.MustParse("20Gi"),
											},
										},
									}},
								},
							},
						},
					},
				},
			},
			result: `StrictModes no
HostKey /etc/ssh/ssh_host_rsa_key
PermitUserEnvironment yes
AcceptEnv *
UsePAM yes
AllowUsers root
PermitRootLogin yes
Port 36000
ListenAddress 0.0.0.0
`,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := getSSHDConfig(tc.mpiJob)
			if diff := cmp.Diff(tc.result, result); len(diff) != 0 {
				t.Errorf("Unexpected (-want,+got):\n%s", diff)
			}
		})
	}
}

func TestNewConfigMap2(t *testing.T) {
	testCases := map[string]struct {
		mpiJob            *kubeflow.MPIJob
		expectedSSHPort   string
		expectedRootLogin bool
	}{
		"default configuration": {
			mpiJob: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Spec: kubeflow.MPIJobSpec{
					MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
						kubeflow.MPIReplicaTypeLauncher: {
							Replicas: pointer.Int32(1),
						},
						kubeflow.MPIReplicaTypeWorker: {
							Replicas: pointer.Int32(4),
						},
					},
				},
			},
			expectedSSHPort:   "36000",
			expectedRootLogin: true,
		},
		"custom ssh port": {
			mpiJob: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-custom-port",
				},
				Spec: kubeflow.MPIJobSpec{
					MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
						kubeflow.MPIReplicaTypeLauncher: {
							Replicas: pointer.Int32(1),
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{{
										Env: []corev1.EnvVar{{
											Name:  "MPI_PORT",
											Value: "2222",
										}},
									}},
								},
							},
						},
					},
				},
			},
			expectedSSHPort:   "2222",
			expectedRootLogin: true,
		},
	}

	// Remove the duplicate test case definition and keep the test logic
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cm := newConfigMap(tc.mpiJob)

			// Verify ConfigMap metadata
			assert.Equal(t, tc.mpiJob.Name+configSuffix, cm.Name)
			assert.Equal(t, tc.mpiJob.Namespace, cm.Namespace)
			assert.Equal(t, tc.mpiJob.Name, cm.Labels["app"])

			// Verify required data keys exist
			assert.Contains(t, cm.Data, sshConfigName)
			assert.Contains(t, cm.Data, sshdConfigName)
			assert.Contains(t, cm.Data, hostfileName)
			assert.Contains(t, cm.Data, envConfig)
			assert.Contains(t, cm.Data, discoverHostsScriptName)

			// Verify SSH configuration
			assert.Contains(t, cm.Data[sshdConfigName], "Port "+tc.expectedSSHPort)
			if tc.expectedRootLogin {
				assert.Contains(t, cm.Data[sshdConfigName], "PermitRootLogin yes")
			} else {
				assert.Contains(t, cm.Data[sshdConfigName], "PermitRootLogin without-password")
			}
		})
	}
}

func TestGetOrCreateConfigMap(t *testing.T) {
	testCases := map[string]struct {
		existingConfigMap *corev1.ConfigMap
		mpiJob            *kubeflow.MPIJob
		expectError       bool
	}{
		"create new configmap": {
			existingConfigMap: nil,
			mpiJob: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
				Spec: kubeflow.MPIJobSpec{
					MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
						kubeflow.MPIReplicaTypeLauncher: {
							Replicas: pointer.Int32(1),
						},
					},
				},
			},
			expectError: false,
		},
		"update existing configmap": {
			existingConfigMap: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job" + configSuffix,
					Namespace: "default",
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: kubeflow.SchemeGroupVersion.String(),
							Kind:       kubeflow.Kind,
							Name:       "test-job",
							UID:        "test-uid",
							Controller: pointer.Bool(true),
						},
					},
				},
				Data: map[string]string{
					"old-key": "old-value",
				},
			},
			mpiJob: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					UID:       "test-uid",
				},
			},
			expectError: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Create fake clientset with initial objects
			var initialObjects []runtime.Object
			if tc.existingConfigMap != nil {
				initialObjects = append(initialObjects, tc.existingConfigMap)
			}

			// Create and add launcher pod to initial objects
			launcher := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tc.mpiJob.Name + "-launcher",
					Namespace: tc.mpiJob.Namespace,
					Labels: map[string]string{
						"app":          tc.mpiJob.Name,
						"mpi-job-role": "launcher",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: kubeflow.SchemeGroupVersion.String(),
							Kind:       kubeflow.Kind,
							Name:       tc.mpiJob.Name,
							UID:        tc.mpiJob.UID,
							Controller: pointer.Bool(true),
						},
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					PodIP: "192.168.1.1",
				},
			}
			initialObjects = append(initialObjects, launcher)

			// Create worker pods and add to initial objects
			for i := 0; i < 2; i++ {
				worker := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("%s-worker-%d", tc.mpiJob.Name, i),
						Namespace: tc.mpiJob.Namespace,
						Labels: map[string]string{
							"app":          tc.mpiJob.Name,
							"mpi-job-role": "worker",
						},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
						PodIP: fmt.Sprintf("192.168.1.%d", i+2),
					},
				}
				initialObjects = append(initialObjects, worker)
			}

			kubeClient := fake.NewSimpleClientset(initialObjects...)

			// Create informers
			configMapInformer := cache.NewSharedIndexInformer(
				&cache.ListWatch{
					ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
						return kubeClient.CoreV1().ConfigMaps(tc.mpiJob.Namespace).List(context.TODO(), options)
					},
					WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
						return kubeClient.CoreV1().ConfigMaps(tc.mpiJob.Namespace).Watch(context.TODO(), options)
					},
				},
				&corev1.ConfigMap{},
				0,
				cache.Indexers{},
			)

			podInformer := cache.NewSharedIndexInformer(
				&cache.ListWatch{
					ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
						return kubeClient.CoreV1().Pods(tc.mpiJob.Namespace).List(context.TODO(), options)
					},
					WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
						return kubeClient.CoreV1().Pods(tc.mpiJob.Namespace).Watch(context.TODO(), options)
					},
				},
				&corev1.Pod{},
				0,
				cache.Indexers{},
			)

			configMapLister := corelister.NewConfigMapLister(configMapInformer.GetIndexer())
			podLister := corelister.NewPodLister(podInformer.GetIndexer())

			// Add objects to informer caches
			for _, obj := range initialObjects {
				switch o := obj.(type) {
				case *corev1.ConfigMap:
					err := configMapInformer.GetIndexer().Add(o)
					assert.NoError(t, err)
				case *corev1.Pod:
					err := podInformer.GetIndexer().Add(o)
					assert.NoError(t, err)
				}
			}

			// Create controller with fake clients
			c := &MPIJobController{
				kubeClient:      kubeClient,
				configMapLister: configMapLister,
				podLister:       podLister,
				recorder:        newFakeRecorder(),
			}

			// Test getOrCreateConfigMap
			cm, err := c.getOrCreateConfigMap(tc.mpiJob)
			if tc.expectError {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, cm)

			// Verify ConfigMap was created/updated
			createdCM, err := kubeClient.CoreV1().ConfigMaps(tc.mpiJob.Namespace).Get(
				context.TODO(),
				tc.mpiJob.Name+configSuffix,
				metav1.GetOptions{},
			)
			assert.NoError(t, err)
			assert.NotNil(t, createdCM)
			assert.Equal(t, cm.Data, createdCM.Data)
		})
	}
}

func TestUpdateServiceWithIP(t *testing.T) {
	testCases := map[string]struct {
		mpiJob       *kubeflow.MPIJob
		launcher     *corev1.Pod
		workers      []*corev1.Pod
		horkers      []*corev1.Pod
		expectUpdate bool
	}{
		"all pods running": {
			mpiJob: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					UID:       "test-uid",
				},
				Spec: kubeflow.MPIJobSpec{
					MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
						kubeflow.MPIReplicaTypeLauncher: {Replicas: pointer.Int32(1)},
						kubeflow.MPIReplicaTypeWorker:   {Replicas: pointer.Int32(2)},
					},
				},
			},
			launcher: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job-launcher",
					Namespace: "default",
					Labels: map[string]string{
						"app":          "test-job",
						"mpi-job-role": "launcher",
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					ContainerStatuses: []corev1.ContainerStatus{{
						State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
					}},
					PodIP: "192.168.1.1",
				},
			},
			workers: []*corev1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-job-worker-0",
						Namespace: "default",
						Labels: map[string]string{
							"app":          "test-job",
							"mpi-job-role": "worker",
						},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
						ContainerStatuses: []corev1.ContainerStatus{{
							State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
						}},
						PodIP: "192.168.1.2",
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-job-worker-1",
						Namespace: "default",
						Labels: map[string]string{
							"app":          "test-job",
							"mpi-job-role": "worker",
						},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
						ContainerStatuses: []corev1.ContainerStatus{{
							State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
						}},
						PodIP: "192.168.1.3",
					},
				},
			},
			expectUpdate: true,
		},
	}

	// ...rest of the test remains the same...
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// Create ConfigMap with initialized Data map
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tc.mpiJob.Name + configSuffix,
					Namespace: tc.mpiJob.Namespace,
					Labels: map[string]string{
						"app": tc.mpiJob.Name,
					},
				},
				Data: map[string]string{
					hostfileName: "",
					envConfig:    "",
				},
			}

			// Update service with IP
			updateServiceWithIP(cm, tc.mpiJob, tc.launcher, tc.workers, tc.horkers)

			if tc.expectUpdate {
				// Verify hostfile content is populated
				assert.NotEmpty(t, cm.Data[hostfileName])
				// Each worker should be listed in the hostfile
				for _, worker := range tc.workers {
					assert.Contains(t, cm.Data[hostfileName], worker.Status.PodIP)
				}

				// Verify environment config
				assert.NotEmpty(t, cm.Data[envConfig])
				// Check for the correct environment variables
				assert.Contains(t, cm.Data[envConfig], fmt.Sprintf("export CHIEF_IP=%s", tc.launcher.Status.PodIP))
				assert.Contains(t, cm.Data[envConfig], fmt.Sprintf("export NODE_IP_LIST=%s:1,%s:1,%s:1",
					tc.launcher.Status.PodIP,
					tc.workers[0].Status.PodIP,
					tc.workers[1].Status.PodIP))
			} else {
				assert.Empty(t, cm.Data[hostfileName])
				assert.Empty(t, cm.Data[envConfig])
			}
		})
	}
}
