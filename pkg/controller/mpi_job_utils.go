package controller

import (
	"fmt"
	"strings"

	kubeflow "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// enableLauncherAsWorker check whether to run worker process in launcher pod
func enableLauncherAsWorker(mpiJob *kubeflow.MPIJob) bool {
	if v, ok := mpiJob.Labels[launcherAsWorker]; ok {
		if strings.ToLower(v) == "false" {
			return false
		}
	}
	if v, ok := mpiJob.Annotations[launcherAsWorker]; ok {
		if strings.ToLower(v) == "false" {
			return false
		}
	}
	return true
}

func getAnnotation(mpiJob *kubeflow.MPIJob, key string) string {
	if v, ok := mpiJob.Annotations[key]; ok {
		return v
	}
	return ""
}

func hasLauncher(mpiJob *kubeflow.MPIJob) bool {
	return !noLauncher(mpiJob)
}

func noLauncher(mpiJob *kubeflow.MPIJob) bool {
	launcher := mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher]
	if launcher == nil || *launcher.Replicas == 0 {
		return true
	}
	return false
}

func elasticEnabled(mpiJob *kubeflow.MPIJob) bool {
	if elastic, ok := mpiJob.Labels[elasticLableName]; ok {
		if strings.ToLower(elastic) == "false" {
			return false
		}
	}
	if v, ok := mpiJob.Annotations[elasticLableName]; ok {
		if strings.ToLower(v) == "false" {
			return false
		}
	}
	return true
}

// isFrozen reports whether the MPIJob is marked as frozen via the
// `kubeflow.org/frozen=true` annotation. A frozen job is paused: the
// controller skips status changes, failure checks, and pod
// create/clean-up operations until the annotation is removed or set
// back to "false".
func isFrozen(mpiJob *kubeflow.MPIJob) bool {
	if v, ok := mpiJob.Annotations[frozenAnnotation]; ok {
		if strings.ToLower(v) == "true" {
			return true
		}
	}
	return false
}

func newJobService(job *kubeflow.MPIJob) *corev1.Service {
	labels := map[string]string{
		kubeflow.OperatorNameLabel: kubeflow.OperatorName,
		kubeflow.JobNameLabel:      job.Name,
	}
	return newService(job, job.Name, labels)
}

func newService(job *kubeflow.MPIJob, name string, selector map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: job.Namespace,
			Labels: map[string]string{
				"app": job.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(job, kubeflow.SchemeGroupVersionKind),
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  selector,
		},
	}
}

func replicasName(mpiJob *kubeflow.MPIJob, index int, rtype kubeflow.MPIReplicaType) string {
	if rtype == kubeflow.MPIReplicaTypeWorker {
		return workerName(mpiJob, index)
	} else if rtype == kubeflow.MPIReplicaTypeHorker {
		return horkerName(mpiJob, index)
	} else {
		return "invalid"
	}
}

func launcherName(mpiJob *kubeflow.MPIJob) string {
	return fmt.Sprintf("%s-%s", mpiJob.Name, launcher)
}

func workerName(mpiJob *kubeflow.MPIJob, index int) string {
	return fmt.Sprintf("%s-%s-%d", mpiJob.Name, worker, index)
}

func horkerName(mpiJob *kubeflow.MPIJob, index int) string {
	return fmt.Sprintf("%s-%s-%d", mpiJob.Name, horker, index)
}

func replicasService(mpiJob *kubeflow.MPIJob, index int, rtype kubeflow.MPIReplicaType) string {
	if rtype == kubeflow.MPIReplicaTypeWorker {
		return workerService(mpiJob, index)
	} else if rtype == kubeflow.MPIReplicaTypeHorker {
		return horkerService(mpiJob, index)
	} else {
		return "invalid"
	}
}

func launcherService(mpiJob *kubeflow.MPIJob) string {
	return fmt.Sprintf("%s.%s.%s", launcherName(mpiJob), mpiJob.Name, mpiJob.Namespace)
}

func workerService(mpiJob *kubeflow.MPIJob, index int) string {
	return fmt.Sprintf("%s.%s.%s", workerName(mpiJob, index), mpiJob.Name, mpiJob.Namespace)
}

func horkerService(mpiJob *kubeflow.MPIJob, index int) string {
	return fmt.Sprintf("%s.%s.%s", horkerName(mpiJob, index), mpiJob.Name, mpiJob.Namespace)
}

func hasEnv(envs []corev1.EnvVar, key string) (bool, string) {
	for _, envVar := range envs {
		if envVar.Name == key {
			return true, envVar.Value
		}
	}
	return false, ""
}

func getReplicasEnv(mpiJob *kubeflow.MPIJob, rtype kubeflow.MPIReplicaType, key string) (bool, string) {
	spec := mpiJob.Spec.MPIReplicaSpecs[rtype]
	if spec != nil && len(spec.Template.Spec.Containers) > 0 {
		container := spec.Template.Spec.Containers[0]
		return hasEnv(container.Env, key)
	}
	return false, ""
}

func getJobEnv(mpiJob *kubeflow.MPIJob, key string) (bool, string) {
	if ok, v := getReplicasEnv(mpiJob, kubeflow.MPIReplicaTypeLauncher, key); ok {
		return ok, v
	}
	if ok, v := getReplicasEnv(mpiJob, kubeflow.MPIReplicaTypeWorker, key); ok {
		return ok, v
	}
	if ok, v := getReplicasEnv(mpiJob, kubeflow.MPIReplicaTypeHorker, key); ok {
		return ok, v
	}
	return false, ""
}

func getAbsIndex(mpiJob *kubeflow.MPIJob, index int, rtype kubeflow.MPIReplicaType) int {
	if rtype == kubeflow.MPIReplicaTypeLauncher {
		return 0
	}
	offset := 0
	if hasLauncher(mpiJob) && enableLauncherAsWorker(mpiJob) {
		offset += 1
	}
	if rtype == kubeflow.MPIReplicaTypeWorker {
		return index + offset
	} else if rtype == kubeflow.MPIReplicaTypeHorker {
		return index + offset + int(workerReplicas(mpiJob))
	} else {
		return -1
	}
}

func workerReplicas(job *kubeflow.MPIJob) int32 {
	workerSpec := job.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker]
	if workerSpec != nil && workerSpec.Replicas != nil {
		return *workerSpec.Replicas
	}
	return 0
}

func horkerReplicas(job *kubeflow.MPIJob) int32 {
	workerSpec := job.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeHorker]
	if workerSpec != nil && workerSpec.Replicas != nil {
		return *workerSpec.Replicas
	}
	return 0
}

func defaultLabels(jobName, role string) map[string]string {
	return map[string]string{
		kubeflow.OperatorNameLabel: kubeflow.OperatorName,
		kubeflow.JobNameLabel:      jobName,
		kubeflow.JobRoleLabel:      role,
	}
}

func getSelector(mpiJobName string, rtype kubeflow.MPIReplicaType) (labels.Selector, error) {
	if rtype == kubeflow.MPIReplicaTypeLauncher {
		set := defaultLabels(mpiJobName, launcher)
		return labels.ValidatedSelectorFromSet(set)
	} else if rtype == kubeflow.MPIReplicaTypeWorker {
		set := defaultLabels(mpiJobName, worker)
		return labels.ValidatedSelectorFromSet(set)
	} else if rtype == kubeflow.MPIReplicaTypeHorker {
		set := defaultLabels(mpiJobName, horker)
		return labels.ValidatedSelectorFromSet(set)
	} else {
		return nil, fmt.Errorf("unknown replica type %q", rtype)
	}
}

func ownerReferenceAndGVK(object metav1.Object) (*metav1.OwnerReference, schema.GroupVersionKind, error) {
	ownerRef := metav1.GetControllerOf(object)
	if ownerRef == nil {
		return nil, schema.GroupVersionKind{}, nil
	}
	gv, err := schema.ParseGroupVersion(ownerRef.APIVersion)
	if err != nil {
		return nil, schema.GroupVersionKind{}, fmt.Errorf("parsing owner's API version: %w", err)
	}
	return ownerRef, gv.WithKind(ownerRef.Kind), nil
}

func newInt32(v int32) *int32 {
	return &v
}

// truncateMessage truncates a message if it hits the NoteLengthLimit.
func truncateMessage(message string) string {
	if len(message) <= eventMessageLimit {
		return message
	}
	suffix := "..."
	return message[:eventMessageLimit-len(suffix)] + suffix
}

func splitStringToMapByComma(s string) map[string]bool {
	if s == "" {
		return map[string]bool{}
	}

	ss := strings.Split(s, ",")
	rs := make(map[string]bool, len(ss))
	for _, ns := range ss {
		rs[ns] = true
	}
	return rs
}
