package controller

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	kubeflow "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
)

const hostListKey = "kubeflow.org/host-list"

// get hostlist from annotation
func getHostListFromAnnotation(mpiJob *kubeflow.MPIJob, rtype kubeflow.MPIReplicaType) []string {
	hosts := getFullHostListFromAnnotation(mpiJob)
	ret := []string{}
	for _, h := range hosts {
		if checkRTypeByPodName(h, rtype) {
			ret = append(ret, h)
		}
	}
	return ret
}

func getFullHostListFromAnnotation(mpiJob *kubeflow.MPIJob) []string {
	ann, ok := mpiJob.Annotations[hostListKey]
	if !ok {
		return nil
	}
	hosts := []string{}
	for _, h := range strings.Split(ann, ",") {
		hh := strings.TrimSpace(h)
		if hh != "" && strings.HasPrefix(hh, mpiJob.Name) {
			hosts = append(hosts, hh)
		}
	}
	return hosts
}

func (c *MPIJobController) getOrCreateReplicasByNames(mpiJob *kubeflow.MPIJob, rtype kubeflow.MPIReplicaType, podNames []string) ([]*corev1.Pod, error) {
	// Create pods in the host list annotation.
	for _, name := range podNames {
		_, err := c.podLister.Pods(mpiJob.Namespace).Get(name)
		// If the worker Pod doesn't exist, we'll create it.
		if errors.IsNotFound(err) {
			idx, err := getIndexByPodName(name)
			if err != nil {
				return nil, err
			}
			replicas := c.newReplicas(mpiJob, idx, rtype)
			_, err = c.kubeClient.CoreV1().Pods(mpiJob.Namespace).Create(context.TODO(), replicas, metav1.CreateOptions{})
			klog.Infof("MPIJob pod %s/%s created from annotation: %v", mpiJob.Namespace, name, err)
		} else if err != nil {
			klog.Errorf("MPIJob %s/%s: get pod %s failed: %v", mpiJob.Namespace, mpiJob.Name, name, err)
			return nil, err
		}
	}

	selector, err := getSelector(mpiJob.Name, rtype)
	if err != nil {
		return nil, err
	}
	podFullList, err := c.podLister.Pods(mpiJob.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	var pods []*corev1.Pod
	// delete pods that are not in podNames
	for _, pod := range podFullList {
		found := false
		for _, name := range podNames {
			if pod.Name == name {
				found = true
				pods = append(pods, pod)
				break
			}
		}
		if !found {
			klog.Infof("MPIJob %s/%s: deleting pod %s not in hostlist annotation", mpiJob.Namespace, mpiJob.Name, pod.Name)
			err = c.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
			if err != nil {
				return nil, err
			}
		}
	}
	return pods, nil
}

func checkRTypeByPodName(podName string, rtype kubeflow.MPIReplicaType) bool {
	rtStr := strings.ToLower(string(rtype))
	if rtype == kubeflow.MPIReplicaTypeLauncher {
		return strings.HasSuffix(podName, rtStr)
	} else {
		parts := strings.Split(podName, "-")
		if len(parts) < 3 {
			return false
		} else {
			return parts[len(parts)-2] == rtStr
		}
	}
}

func getIndexByPodName(podName string) (int, error) {
	parts := strings.Split(podName, "-")
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid pod name %s", podName)
	}
	indexStr := parts[len(parts)-1]
	index, err := strconv.Atoi(indexStr)
	if err != nil {
		return 0, fmt.Errorf("invalid pod index %s in pod name %s", indexStr, podName)
	}
	if indexStr != strconv.Itoa(index) {
		return 0, fmt.Errorf("pod name %s has invalid index %s", podName, indexStr)
	}
	return index, nil
}

// deletePodsFromAnnotation deletes pods from the host list annotation
func (c *MPIJobController) deletePodsFromAnnotation(mpiJob *kubeflow.MPIJob, hosts []string) error {
	for _, name := range hosts {
		pod, err := c.podLister.Pods(mpiJob.Namespace).Get(name)
		if errors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("getting pod %s/%s: %w", mpiJob.Namespace, name, err)
		}
		if !metav1.IsControlledBy(pod, mpiJob) {
			msg := fmt.Sprintf(MessageResourceExists, pod.Name, pod.Kind)
			c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
			return fmt.Errorf(msg)
		}
		klog.Infof("Deleting pod %s/%s from host list annotation", mpiJob.Namespace, name)
		err = c.kubeClient.CoreV1().Pods(mpiJob.Namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("deleting pod %s/%s: %w", mpiJob.Namespace, name, err)
		}
	}
	return nil
}

func getPodFromListByName(podName string, launcher *corev1.Pod, workers []*corev1.Pod, horkers []*corev1.Pod) *corev1.Pod {
	if launcher.Name == podName {
		return launcher
	}
	for i, pod := range workers {
		if pod.Name == podName {
			return workers[i]
		}
	}
	for i, pod := range horkers {
		if pod.Name == podName {
			return horkers[i]
		}
	}
	return nil
}

func updateHostfileFromAnnotation(configMap *corev1.ConfigMap, mpiJob *kubeflow.MPIJob, launcher *corev1.Pod, workers []*corev1.Pod, horkers []*corev1.Pod, hosts []string) {
	if !allContainerRunning(mpiJob, launcher, workers, horkers) {
		return
	}
	klog.Infof("MPIJob %s/%s update hostfile from annotation: %v", mpiJob.Namespace, mpiJob.Name, hosts)

	var hostfile bytes.Buffer
	var ips bytes.Buffer
	slots := 1
	if mpiJob.Spec.SlotsPerWorker != nil {
		slots = int(*mpiJob.Spec.SlotsPerWorker)
	}
	for _, name := range hosts {
		p := getPodFromListByName(name, launcher, workers, horkers)
		if p == nil {
			klog.Errorf("MPIJob %s/%s: pod %s not found in hosts", mpiJob.Namespace, mpiJob.Name, name)
			return
		}
		hostfile.WriteString(fmt.Sprintf("%s slots=%d\n", p.Status.PodIP, slots))
		ips.WriteString(fmt.Sprintf("%s:%d,", p.Status.PodIP, slots))
	}

	var env bytes.Buffer
	env.WriteString(fmt.Sprintf("export CHIEF_IP=%s\n", launcher.Status.PodIP))
	env.WriteString(fmt.Sprintf("export NODE_IP_LIST=%s\n", strings.TrimSuffix(ips.String(), ",")))
	env.WriteString(fmt.Sprintf("export NODE_LIST=%s\n", ""))
	env.WriteString(fmt.Sprintf("export MPI_HOST_NUM=%d\n", len(hosts)))
	env.WriteString(fmt.Sprintf("export MPI_NODE_NUM=%d\n", len(hosts)*slots))
	env.WriteString(fmt.Sprintf("export OMPI_MCA_orte_default_hostfile=%s/%s\n", configMountPath, hostfileName))
	env.WriteString("export OMPI_MCA_orte_keep_fqdn_hostnames=true\n")

	configMap.Data[hostfileName] = hostfile.String()
	configMap.Data[envConfig] = env.String()
}
