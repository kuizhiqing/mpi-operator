package controller

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"path/filepath"
	"sort"

	kubeflow "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// getOrCreateSSHAuthSecret gets the Secret holding the SSH auth for this job,
// or create one if it doesn't exist.
func (c *MPIJobController) getOrCreateSSHAuthSecret(job *kubeflow.MPIJob) (*corev1.Secret, error) {
	secret, err := c.secretLister.Secrets(job.Namespace).Get(job.Name + sshAuthSecretSuffix)
	if errors.IsNotFound(err) {
		secret, err := newSSHAuthSecret(job)
		if err != nil {
			return nil, err
		}
		return c.kubeClient.CoreV1().Secrets(job.Namespace).Create(context.TODO(), secret, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(secret, job) {
		msg := fmt.Sprintf(MessageResourceExists, secret.Name, secret.Kind)
		c.recorder.Event(job, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}
	// secret, err = c.updateSSHAuthSecret(job, secret)
	return secret, err
}

// nolint: unused
func (c *MPIJobController) updateSSHAuthSecret(job *kubeflow.MPIJob, secret *corev1.Secret) (*corev1.Secret, error) {
	newSecret, err := newSSHAuthSecret(job)
	if err != nil {
		return nil, fmt.Errorf("generating new secret: %w", err)
	}
	hasKeys := keysFromData(secret.Data)
	wantKeys := keysFromData(newSecret.Data)
	if !equality.Semantic.DeepEqual(hasKeys, wantKeys) {
		secret := secret.DeepCopy()
		secret.Data = newSecret.Data
		return c.kubeClient.CoreV1().Secrets(secret.Namespace).Update(context.TODO(), secret, metav1.UpdateOptions{})
	}
	return secret, nil
}

// nolint: unused
func keysFromData(data map[string][]byte) []string {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// newSSHAuthSecret creates a new Secret that holds SSH auth: a private Key
// and its public key version.
func newSSHAuthSecret(job *kubeflow.MPIJob) (*corev1.Secret, error) {
	// privateKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, fmt.Errorf("generating private SSH key: %w", err)
	}
	// privateDER, err := x509.MarshalECPrivateKey(privateKey)
	privateDER := x509.MarshalPKCS1PrivateKey(privateKey)
	privatePEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privateDER,
	})

	publicKey, err := ssh.NewPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("generating public SSH key: %w", err)
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      job.Name + sshAuthSecretSuffix,
			Namespace: job.Namespace,
			Labels: map[string]string{
				"app": job.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, kubeflow.SchemeGroupVersionKind),
			},
		},
		Type: corev1.SecretTypeSSHAuth,
		Data: map[string][]byte{
			corev1.SSHAuthPrivateKey: privatePEM,
			sshPublicKey:             ssh.MarshalAuthorizedKey(publicKey),
		},
	}, nil
}

func (c *MPIJobController) setupSSHOnPod(podSpec *corev1.PodSpec, job *kubeflow.MPIJob) {
	mainContainer := &podSpec.Containers[0]

	// /etc/launch/hostfile environ
	podSpec.Volumes = append(podSpec.Volumes,
		corev1.Volume{
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: job.Name + configSuffix,
					},
					Items: configVolumeItems,
				},
			},
		})
	mainContainer.VolumeMounts = append(mainContainer.VolumeMounts, corev1.VolumeMount{
		Name:      configVolumeName,
		MountPath: configMountPath,
	})

	// ~/.ssh/authorized_keys  id_rsa  id_rsa.pub
	podSpec.Volumes = append(podSpec.Volumes,
		corev1.Volume{
			Name: sshAuthVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					DefaultMode: newInt32(0600),
					SecretName:  job.Name + sshAuthSecretSuffix,
					Items: []corev1.KeyToPath{
						{
							Key:  corev1.SSHAuthPrivateKey,
							Path: sshPrivateKeyFile,
							Mode: newInt32(0400),
						},
						{
							Key:  sshPublicKey,
							Path: sshPublicKeyFile,
							Mode: newInt32(0644),
						},
						{
							Key:  sshPublicKey,
							Path: sshAuthorizedKeysFile,
							Mode: newInt32(0644),
						},
					},
				},
			},
		})

	// /etc/ssh/ssh_host_rsa_key ssh_host_rsa_key.pub
	podSpec.Volumes = append(podSpec.Volumes,
		corev1.Volume{
			Name: hostKeyVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					DefaultMode: newInt32(0600),
					SecretName:  job.Name + sshAuthSecretSuffix,
					Items: []corev1.KeyToPath{
						{
							Key:  corev1.SSHAuthPrivateKey,
							Path: sshPrivateHostRsaKey,
							Mode: newInt32(0400),
						},
						{
							Key:  sshPublicKey,
							Path: sshPublicHostRsaKey,
							Mode: newInt32(0644),
						},
					},
				},
			},
		})

	mainContainer.VolumeMounts = append(mainContainer.VolumeMounts,
		corev1.VolumeMount{
			Name: sshAuthVolume,
			// MountPath: job.Spec.SSHAuthMountPath,
			// always mount to root now
			MountPath: rootSSHPath,
		},
		corev1.VolumeMount{
			Name:      hostKeyVolume,
			MountPath: filepath.Join(sshConfigPath, sshPrivateHostRsaKey),
			SubPath:   sshPrivateHostRsaKey,
		},
		corev1.VolumeMount{
			Name:      hostKeyVolume,
			MountPath: filepath.Join(sshConfigPath, sshPublicHostRsaKey),
			SubPath:   sshPublicHostRsaKey,
		})

	// /etc/ssh/ssh_config
	// /etc/ssh/sshd_config
	podSpec.Volumes = append(podSpec.Volumes,
		corev1.Volume{
			Name: sshConfig,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					DefaultMode: newInt32(0644),
					LocalObjectReference: corev1.LocalObjectReference{
						Name: job.Name + configSuffix,
					},
					/*
						Items: []corev1.KeyToPath{
							{
								Key:  sshConfig,
								Path: sshConfigName,
								Mode: newInt32(0644),
							},
							{
								Key:  sshdConfig,
								Path: sshdConfigName,
								Mode: newInt32(0644),
							},
						},
					*/
				},
			},
		})

	mainContainer.VolumeMounts = append(mainContainer.VolumeMounts,
		corev1.VolumeMount{
			Name:      sshConfig,
			MountPath: filepath.Join(sshConfigPath, sshConfigName),
			SubPath:   sshConfigName,
		},
		corev1.VolumeMount{
			Name:      sshConfig,
			MountPath: filepath.Join(sshConfigPath, sshdConfigName),
			SubPath:   sshdConfigName,
		})

}
