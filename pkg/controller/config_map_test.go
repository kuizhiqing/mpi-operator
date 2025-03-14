package controller

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	kubeflow "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

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
		"user with password": {
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
											{
												Name:  "LAUNCH_USER",
												Value: "launch",
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
RSAAuthentication yes
PubkeyAuthentication yes
PermitRootLogin without-password
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
