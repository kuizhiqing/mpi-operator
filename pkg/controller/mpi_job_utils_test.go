package controller

import (
	"testing"

	kubeflow "github.com/kuizhiqing/resilient-training-operator/pkg/apis/kubeflow/v2beta1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

func TestEnableLauncherAsWorker(t *testing.T) {
	testCases := map[string]struct {
		mpiJob   *kubeflow.ResilientJob
		expected bool
	}{
		"default case": {
			mpiJob:   &kubeflow.ResilientJob{},
			expected: true,
		},
		"enabled explicitly": {
			mpiJob: &kubeflow.ResilientJob{
				Spec: kubeflow.ResilientJobSpec{
					LauncherAsWorker: pointer.Bool(true),
				},
			},
			expected: true,
		},
		"disabled by field": {
			mpiJob: &kubeflow.ResilientJob{
				Spec: kubeflow.ResilientJobSpec{
					LauncherAsWorker: pointer.Bool(false),
				},
			},
			expected: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := enableLauncherAsWorker(tc.mpiJob)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestElasticEnabled(t *testing.T) {
	testCases := map[string]struct {
		mpiJob   *kubeflow.ResilientJob
		expected bool
	}{
		"nil policy defaults to enabled": {
			mpiJob:   &kubeflow.ResilientJob{},
			expected: true,
		},
		"policy with nil enabled defaults to enabled": {
			mpiJob: &kubeflow.ResilientJob{
				Spec: kubeflow.ResilientJobSpec{
					ElasticPolicy: &kubeflow.ElasticPolicy{},
				},
			},
			expected: true,
		},
		"disabled": {
			mpiJob: &kubeflow.ResilientJob{
				Spec: kubeflow.ResilientJobSpec{
					ElasticPolicy: &kubeflow.ElasticPolicy{Enabled: pointer.Bool(false)},
				},
			},
			expected: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, elasticEnabled(tc.mpiJob))
		})
	}
}

func TestIsFrozen(t *testing.T) {
	testCases := map[string]struct {
		mpiJob   *kubeflow.ResilientJob
		expected bool
	}{
		"default not frozen": {
			mpiJob:   &kubeflow.ResilientJob{},
			expected: false,
		},
		"frozen": {
			mpiJob: &kubeflow.ResilientJob{
				Spec: kubeflow.ResilientJobSpec{Frozen: pointer.Bool(true)},
			},
			expected: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isFrozen(tc.mpiJob))
		})
	}
}

func TestRecoverEnabled(t *testing.T) {
	testCases := map[string]struct {
		mpiJob   *kubeflow.ResilientJob
		expected bool
	}{
		"nil policy defaults to disabled": {
			mpiJob:   &kubeflow.ResilientJob{},
			expected: false,
		},
		"policy with nil enabled defaults to enabled": {
			mpiJob: &kubeflow.ResilientJob{
				Spec: kubeflow.ResilientJobSpec{
					RecoverPolicy: &kubeflow.RecoverPolicy{},
				},
			},
			expected: true,
		},
		"disabled": {
			mpiJob: &kubeflow.ResilientJob{
				Spec: kubeflow.ResilientJobSpec{
					RecoverPolicy: &kubeflow.RecoverPolicy{Enabled: pointer.Bool(false)},
				},
			},
			expected: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, recoverEnabled(tc.mpiJob))
		})
	}
}

func TestNewJobService(t *testing.T) {
	job := &kubeflow.ResilientJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
			UID:       "test-uid",
		},
	}

	svc := newJobService(job)

	assert.Equal(t, job.Name, svc.Name)
	assert.Equal(t, job.Namespace, svc.Namespace)
	assert.Equal(t, job.Name, svc.Labels["app"])
	assert.Equal(t, kubeflow.OperatorName, svc.Spec.Selector[kubeflow.OperatorNameLabel])
	assert.Equal(t, job.Name, svc.Spec.Selector[kubeflow.JobNameLabel])
	assert.Equal(t, corev1.ClusterIPNone, svc.Spec.ClusterIP)

	// Verify owner reference
	assert.Len(t, svc.OwnerReferences, 1)
	assert.Equal(t, job.Name, svc.OwnerReferences[0].Name)
	assert.Equal(t, string(job.UID), string(svc.OwnerReferences[0].UID))
	assert.Equal(t, kubeflow.SchemeGroupVersion.String(), svc.OwnerReferences[0].APIVersion)
	assert.Equal(t, kubeflow.Kind, svc.OwnerReferences[0].Kind)
	assert.True(t, *svc.OwnerReferences[0].Controller)
}

func TestGetReplicasEnv(t *testing.T) {
	testCases := map[string]struct {
		mpiJob      *kubeflow.ResilientJob
		rtype       kubeflow.MPIReplicaType
		key         string
		expectFound bool
		expectValue string
	}{
		"env exists in launcher": {
			mpiJob: &kubeflow.ResilientJob{
				Spec: kubeflow.ResilientJobSpec{
					MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
						kubeflow.MPIReplicaTypeLauncher: {
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Env: []corev1.EnvVar{
												{
													Name:  "TEST_KEY",
													Value: "test-value",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			rtype:       kubeflow.MPIReplicaTypeLauncher,
			key:         "TEST_KEY",
			expectFound: true,
			expectValue: "test-value",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			found, value := getReplicasEnv(tc.mpiJob, tc.rtype, tc.key)
			assert.Equal(t, tc.expectFound, found)
			assert.Equal(t, tc.expectValue, value)
		})
	}
}

func TestReplicasName(t *testing.T) {
	job := &kubeflow.ResilientJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-job",
		},
	}

	testCases := map[string]struct {
		rtype    kubeflow.MPIReplicaType
		index    int
		expected string
	}{
		"worker name": {
			rtype:    kubeflow.MPIReplicaTypeWorker,
			index:    0,
			expected: "test-job-worker-0",
		},
		"horker name": {
			rtype:    kubeflow.MPIReplicaTypeHorker,
			index:    1,
			expected: "test-job-horker-1",
		},
		"invalid type": {
			rtype:    "invalid",
			index:    0,
			expected: "invalid",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := replicasName(job, tc.index, tc.rtype)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestWorkerReplicas(t *testing.T) {
	testCases := map[string]struct {
		job      *kubeflow.ResilientJob
		expected int32
	}{
		"with replicas": {
			job: &kubeflow.ResilientJob{
				Spec: kubeflow.ResilientJobSpec{
					MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{
						kubeflow.MPIReplicaTypeWorker: {
							Replicas: pointer.Int32(2),
						},
					},
				},
			},
			expected: 2,
		},
		"no replicas": {
			job: &kubeflow.ResilientJob{
				Spec: kubeflow.ResilientJobSpec{
					MPIReplicaSpecs: map[kubeflow.MPIReplicaType]*kubeflow.ReplicaSpec{},
				},
			},
			expected: 0,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := workerReplicas(tc.job)
			assert.Equal(t, tc.expected, result)
		})
	}
}
