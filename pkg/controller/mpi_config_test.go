package controller

import (
	"encoding/base64"
	"strings"
	"testing"

	kubeflow "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGenSSHKeyPair(t *testing.T) {
	publicKey, privateKey, err := genSSHKeyPairRandom()
	assert.NoError(t, err)
	assert.NotNil(t, publicKey)
	assert.NotNil(t, privateKey)

	// Verify public key format
	assert.True(t, strings.HasPrefix(string(publicKey), "ssh-rsa "))

	// Verify private key format
	assert.True(t, strings.Contains(string(privateKey), "-----BEGIN RSA PRIVATE KEY-----"))
	assert.True(t, strings.Contains(string(privateKey), "-----END RSA PRIVATE KEY-----"))
}

func TestGetSSHKeyPair(t *testing.T) {
	testCases := map[string]struct {
		job           *kubeflow.MPIJob
		expectErr     bool
		checkExisting bool
	}{
		"with valid annotations": {
			job: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						sshPublicKey:             base64.StdEncoding.EncodeToString([]byte("test-public-key")),
						corev1.SSHAuthPrivateKey: base64.StdEncoding.EncodeToString([]byte("test-private-key")),
					},
				},
			},
			checkExisting: true,
		},
		"without annotations": {
			job: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
			},
			expectErr: true,
		},
		"with invalid base64 public key": {
			job: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						sshPublicKey:             "invalid-base64",
						corev1.SSHAuthPrivateKey: base64.StdEncoding.EncodeToString([]byte("test-private-key")),
					},
				},
			},
			expectErr: true,
		},
		"with invalid base64 private key": {
			job: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						sshPublicKey:             base64.StdEncoding.EncodeToString([]byte("test-public-key")),
						corev1.SSHAuthPrivateKey: "invalid-base64",
					},
				},
			},
			expectErr: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			public, private, err := getSSHKeyPairFromAnnotation(tc.job)
			if tc.expectErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, public)
			assert.NotNil(t, private)

			if tc.checkExisting {
				expectedPublic, _ := base64.StdEncoding.DecodeString(tc.job.Annotations[sshPublicKey])
				expectedPrivate, _ := base64.StdEncoding.DecodeString(tc.job.Annotations[corev1.SSHAuthPrivateKey])
				assert.Equal(t, expectedPublic, public)
				assert.Equal(t, expectedPrivate, private)
			}
		})
	}
}

func TestNewSSHAuthSecret(t *testing.T) {
	job := &kubeflow.MPIJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
		},
	}

	secret, err := newSSHAuthSecret(job)
	assert.NoError(t, err)

	// Verify secret metadata
	assert.Equal(t, job.Name+sshAuthSecretSuffix, secret.Name)
	assert.Equal(t, job.Namespace, secret.Namespace)
	assert.Equal(t, job.Name, secret.Labels["app"])
	assert.Equal(t, corev1.SecretTypeSSHAuth, secret.Type)

	// Verify secret data
	assert.Contains(t, secret.Data, corev1.SSHAuthPrivateKey)
	assert.Contains(t, secret.Data, sshPublicKey)
}

func TestSetupSSHOnPod(t *testing.T) {
	job := &kubeflow.MPIJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
		},
	}

	podSpec := &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name: "main",
		}},
	}

	controller := &MPIJobController{}
	controller.setupSSHOnPod(podSpec, job)

	// Verify volumes
	assert.Equal(t, 4, len(podSpec.Volumes))
	assert.Contains(t, getVolumeNames(podSpec.Volumes), configVolumeName)
	assert.Contains(t, getVolumeNames(podSpec.Volumes), sshAuthVolume)
	assert.Contains(t, getVolumeNames(podSpec.Volumes), hostKeyVolume)
	assert.Contains(t, getVolumeNames(podSpec.Volumes), sshConfig)

	// Verify volume mounts
	container := &podSpec.Containers[0]
	assert.Equal(t, 6, len(container.VolumeMounts))
	assert.Contains(t, getVolumeMountNames(container.VolumeMounts), configVolumeName)
	assert.Contains(t, getVolumeMountNames(container.VolumeMounts), sshAuthVolume)
	assert.Contains(t, getVolumeMountNames(container.VolumeMounts), hostKeyVolume)
	assert.Contains(t, getVolumeMountNames(container.VolumeMounts), sshConfig)
}

func TestUpdateSSHAuthSecret(t *testing.T) {
	testCases := map[string]struct {
		job          *kubeflow.MPIJob
		existingData map[string][]byte
		expectErr    bool
		checkUpdated bool
	}{
		"without annotations": {
			job: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
				},
			},
			existingData: map[string][]byte{
				sshPublicKey:             []byte("old-public-key"),
				corev1.SSHAuthPrivateKey: []byte("old-private-key"),
			},
		},
		"with invalid base64 keys": {
			job: &kubeflow.MPIJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: "default",
					Annotations: map[string]string{
						sshPublicKey:             "invalid-base64",
						corev1.SSHAuthPrivateKey: "invalid-base64",
					},
				},
			},
			existingData: map[string][]byte{},
		},
	}

	for name, tc := range testCases {
		f := newFixture(t, "")
		startTime := metav1.Now()
		completionTime := metav1.Now()

		mpiJob := newMPIJob("test", newInt32(64), &startTime, &completionTime)
		f.setUpMPIJob(mpiJob)

		controller := f.newFakeMPIJobController()
		t.Run(name, func(t *testing.T) {
			secret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      tc.job.Name + sshAuthSecretSuffix,
					Namespace: tc.job.Namespace,
				},
				Data: tc.existingData,
				Type: corev1.SecretTypeSSHAuth,
			}
			f.setUpSecret(secret)
			updatedSecret, err := controller.updateSSHAuthSecret(tc.job, secret)

			if tc.expectErr {
				assert.Error(t, err)
				return
			}

			if tc.checkUpdated {
				expectedPublic, _ := base64.StdEncoding.DecodeString(tc.job.Annotations[sshPublicKey])
				expectedPrivate, _ := base64.StdEncoding.DecodeString(tc.job.Annotations[corev1.SSHAuthPrivateKey])

				assert.Equal(t, expectedPublic, updatedSecret.Data[sshPublicKey])
				assert.Equal(t, expectedPrivate, updatedSecret.Data[corev1.SSHAuthPrivateKey])
			}
		})
	}
}

// Helper functions

func getVolumeNames(volumes []corev1.Volume) []string {
	var names []string
	for _, v := range volumes {
		names = append(names, v.Name)
	}
	return names
}

func getVolumeMountNames(mounts []corev1.VolumeMount) []string {
	var names []string
	for _, m := range mounts {
		names = append(names, m.Name)
	}
	return names
}
