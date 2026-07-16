package controller

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"path/filepath"

	kubeflow "github.com/kuizhiqing/resilient-training-operator/pkg/apis/kubeflow/v2beta1"
	"golang.org/x/crypto/ssh"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// getOrCreateSSHAuthSecret gets the Secret holding the SSH auth for this job,
// or create one if it doesn't exist.
func (c *ResilientJobController) getOrCreateSSHAuthSecret(job *kubeflow.ResilientJob) (*corev1.Secret, error) {
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
	secret, err = c.updateSSHAuthSecret(job, secret)
	return secret, err
}

// updateSSHAuthSecret updates the SSH auth secret only from the annotation.
func (c *ResilientJobController) updateSSHAuthSecret(job *kubeflow.ResilientJob, secret *corev1.Secret) (*corev1.Secret, error) {
	publicKey, privatePEM, err := getSSHKeyPairFromAnnotation(job)
	if err != nil {
		// no ssh key pair in annotation
		return secret, nil
	}
	if !bytes.Equal(secret.Data[corev1.SSHAuthPrivateKey], privatePEM) ||
		!bytes.Equal(secret.Data[sshPublicKey], publicKey) {
		secret := secret.DeepCopy()
		secret.Data =
			map[string][]byte{
				corev1.SSHAuthPrivateKey: privatePEM,
				sshPublicKey:             publicKey,
			}
		return c.kubeClient.CoreV1().Secrets(secret.Namespace).Update(context.TODO(), secret, metav1.UpdateOptions{})
	}
	return secret, nil
}

func genSSHKeyPairRandom() ([]byte, []byte, error) {
	// privateKey, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, fmt.Errorf("generating private SSH key: %w", err)
	}
	// privateDER, err := x509.MarshalECPrivateKey(privateKey)
	privateDER := x509.MarshalPKCS1PrivateKey(privateKey)
	privatePEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privateDER,
	})

	publicKey, err := ssh.NewPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, nil, fmt.Errorf("generating public SSH key: %w", err)
	}

	return ssh.MarshalAuthorizedKey(publicKey), privatePEM, nil
}

func getSSHKeyPairFromAnnotation(job *kubeflow.ResilientJob) ([]byte, []byte, error) {
	// get public key string from annotion
	publicKey := getAnnotation(job, sshPublicKey)
	privateKey := getAnnotation(job, corev1.SSHAuthPrivateKey)
	if publicKey != "" && privateKey != "" {
		public, err := base64.StdEncoding.DecodeString(publicKey)
		if err != nil {
			return nil, nil, fmt.Errorf("decoding public key: %w", err)
		}
		private, err := base64.StdEncoding.DecodeString(privateKey)
		if err != nil {
			return nil, nil, fmt.Errorf("decoding private key: %w", err)
		}
		return public, private, nil
	}
	return nil, nil, fmt.Errorf("no SSH key")
}

// newSSHAuthSecret creates a new Secret that holds SSH auth: a private Key
// and its public key version.
func newSSHAuthSecret(job *kubeflow.ResilientJob) (*corev1.Secret, error) {
	publicKey, privatePEM, err := getSSHKeyPairFromAnnotation(job)
	if err != nil {
		publicKey, privatePEM, err = genSSHKeyPairRandom()
	}
	if err != nil {
		return nil, fmt.Errorf("generating SSH key pair: %w", err)
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
			sshPublicKey:             publicKey,
		},
	}, nil
}

func (c *ResilientJobController) setupSSHOnPod(podSpec *corev1.PodSpec, job *kubeflow.ResilientJob) {
	mainContainer := &podSpec.Containers[0]

	// /etc/mpi/hostfile environ
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
