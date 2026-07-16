package controller

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"path/filepath"
	"strings"

	kubeflow "github.com/kuizhiqing/resilient-training-operator/pkg/apis/kubeflow/v2beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"
)

//go:embed mpirun-wrapper.sh
var MPIRunWrapperScript string

//go:embed mpirun-recover.sh
var MPIRunRecoverScript string

func (c *ResilientJobController) setupMPIRunWrapperOnPod(podSpec *corev1.PodSpec, job *kubeflow.ResilientJob) {
	mainContainer := &podSpec.Containers[0]
	podSpec.Volumes = append(podSpec.Volumes,
		corev1.Volume{
			Name: mpirunWrapper,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					DefaultMode: newInt32(0755),
					LocalObjectReference: corev1.LocalObjectReference{
						Name: fmt.Sprintf("%s-%s", job.Name, mpirunWrapper),
					},
				},
			},
		})

	mainContainer.VolumeMounts = append(mainContainer.VolumeMounts,
		corev1.VolumeMount{
			Name:      mpirunWrapper,
			MountPath: filepath.Join(mpirunWrapperPath, mpirunWrapper),
			SubPath:   mpirunWrapper,
		})
}

func (c *ResilientJobController) getOrCreateMPIRunWrapperConfigMap(mpiJob *kubeflow.ResilientJob) (*corev1.ConfigMap, error) {
	name := fmt.Sprintf("%s-%s", mpiJob.Name, mpirunWrapper)

	newCM := newMPIRunWrapperConfig(mpiJob, name)

	cm, err := c.configMapLister.ConfigMaps(mpiJob.Namespace).Get(name)
	// If the ConfigMap doesn't exist, we'll create it.
	if errors.IsNotFound(err) {
		return c.kubeClient.CoreV1().ConfigMaps(mpiJob.Namespace).Create(context.TODO(), newCM, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}

	// If the ConfigMap is not controlled by this ResilientJob resource, we
	// should log a warning to the event recorder and return.
	if !metav1.IsControlledBy(cm, mpiJob) {
		msg := fmt.Sprintf(MessageResourceExists, cm.Name, cm.Kind)
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	return cm, nil
}

func newMPIRunWrapperConfig(mpiJob *kubeflow.ResilientJob, name string) *corev1.ConfigMap {
	script := MPIRunWrapperScript
	if _, ok := mpiJob.Annotations[enableRecover]; ok {
		script = MPIRunRecoverScript
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: mpiJob.Namespace,
			Labels: map[string]string{
				"app": mpiJob.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(mpiJob, kubeflow.SchemeGroupVersionKind),
			},
		},
		Data: map[string]string{
			mpirunWrapper: script,
		},
	}
}

// getOrCreateConfigMap gets the ConfigMap controlled by this ResilientJob, or creates
// one if it doesn't exist.
func (c *ResilientJobController) getOrCreateConfigMap(mpiJob *kubeflow.ResilientJob) (*corev1.ConfigMap, error) {
	newCM := newConfigMap(mpiJob)
	workers, err := c.getRunningWorkerPods(mpiJob)
	if err != nil {
		return nil, err
	}
	horkers, err := c.getRunningHorkerPods(mpiJob)
	if err != nil {
		return nil, err
	}
	launcher, err := c.getLauncherPod(mpiJob)
	if err != nil {
		return nil, err
	}

	hosts := getFullHostListFromAnnotation(mpiJob)
	if len(hosts) > 0 {
		updateHostfileFromAnnotation(newCM, mpiJob, launcher, workers, horkers, hosts)
	} else {
		updateServiceWithIP(newCM, mpiJob, launcher, workers, horkers)
	}

	if err := updateRecoverStateFromAnnotation(newCM, mpiJob); err != nil {
		klog.Errorf("update drop hostfile from annotation error: %v", err)
	}

	cm, err := c.configMapLister.ConfigMaps(mpiJob.Namespace).Get(mpiJob.Name + configSuffix)
	// If the ConfigMap doesn't exist, we'll create it.
	if errors.IsNotFound(err) {
		return c.kubeClient.CoreV1().ConfigMaps(mpiJob.Namespace).Create(context.TODO(), newCM, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}

	// If the ConfigMap is not controlled by this ResilientJob resource, we
	// should log a warning to the event recorder and return.
	if !metav1.IsControlledBy(cm, mpiJob) {
		msg := fmt.Sprintf(MessageResourceExists, cm.Name, cm.Kind)
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	// If the ConfigMap is changed, update it
	if !equality.Semantic.DeepEqual(cm.Data, newCM.Data) {
		cm = cm.DeepCopy()
		cm.Data = newCM.Data
		cm, err = c.kubeClient.CoreV1().ConfigMaps(mpiJob.Namespace).Update(context.TODO(), cm, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}

		// update pod annotation to apply cm update
		err = c.updatePodAnnotation4CM(mpiJob, cm)
		if err != nil {
			return nil, err
		}
	}

	return cm, nil
}

// newConfigMap creates a new ConfigMap containing configurations for an ResilientJob
// resource. It also sets the appropriate OwnerReferences on the resource so
// handleObject can discover the ResilientJob resource that 'owns' it.
func newConfigMap(mpiJob *kubeflow.ResilientJob) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mpiJob.Name + configSuffix,
			Namespace: mpiJob.Namespace,
			Labels: map[string]string{
				"app": mpiJob.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(mpiJob, kubeflow.SchemeGroupVersionKind),
			},
		},
		Data: map[string]string{
			sshConfigName:           getSSHConfig(mpiJob),
			sshdConfigName:          getSSHDConfig(mpiJob),
			hostfileName:            getHostfile(mpiJob), // auto update
			envConfig:               "",
			discoverHostsScriptName: getDiscoverHosts(),
			recoverFileName:         "", // empty file filled from outside
		},
	}
}

func (c *ResilientJobController) updatePodAnnotation4CM(mpiJob *kubeflow.ResilientJob, cm *corev1.ConfigMap) error {
	launcher, err := c.getLauncherPod(mpiJob)
	if err != nil {
		return err
	}
	if launcher != nil {
		if len(launcher.Annotations) == 0 {
			launcher.Annotations = make(map[string]string)
		}
		launcher.Annotations[configMapVolumeVersion] = cm.ResourceVersion
		_, err := c.kubeClient.CoreV1().Pods(mpiJob.Namespace).Update(context.TODO(), launcher, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		klog.Infof("Update launcher cm %s/%s to %s", cm.Namespace, cm.Name, cm.ResourceVersion)
	}

	return nil
}

func isPodContainerRunning(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.State.Running == nil {
			return false
		}
	}
	return true
}

func allContainerRunning(mpiJob *kubeflow.ResilientJob, launcher *corev1.Pod, workers []*corev1.Pod, horkers []*corev1.Pod) bool {
	if hasLauncher(mpiJob) {
		if launcher == nil || !isPodContainerRunning(launcher) {
			return false
		}
	}
	for _, pod := range workers {
		if !isPodContainerRunning(pod) {
			return false
		}
	}
	for _, pod := range horkers {
		if !isPodContainerRunning(pod) {
			return false
		}
	}
	return true
}

func updateServiceWithIP(configMap *corev1.ConfigMap, mpiJob *kubeflow.ResilientJob, launcher *corev1.Pod, workers []*corev1.Pod, horkers []*corev1.Pod) {
	klog.Infof("Job %s/%s worker %d/%d horker %d/%d", mpiJob.Namespace, mpiJob.Name, len(workers), workerReplicas(mpiJob), len(horkers), horkerReplicas(mpiJob))
	if int(workerReplicas(mpiJob)) != len(workers) {
		return
	}
	if int(horkerReplicas(mpiJob)) != len(horkers) {
		return
	}

	if !allContainerRunning(mpiJob, launcher, workers, horkers) {
		return
	}

	var hostfile bytes.Buffer
	var ips bytes.Buffer
	slots := 1
	if mpiJob.Spec.SlotsPerWorker != nil {
		slots = int(*mpiJob.Spec.SlotsPerWorker)
	}

	ipList := getIPList(mpiJob, slots)
	hostList := getHostList(mpiJob, slots)
	jobRole := getAnnotation(mpiJob, heterRoleKey)
	if jobRole != "launcher" && ipList != "" && hostList != "" {
		ips.WriteString(ipList)
		hostfile.WriteString(hostList)
	}

	chiefIP := ""
	if enableLauncherAsWorker(mpiJob) && launcher != nil {
		ip := launcher.Status.PodIP
		chiefIP = ip
		if len(strings.Split(ip, ".")) == 4 {
			ips.WriteString(fmt.Sprintf("%s:%d,", ip, slots))
			switch mpiJob.Spec.MPIImplementation {
			case kubeflow.MPIImplementationOpenMPI:
				hostfile.WriteString(fmt.Sprintf("%s slots=%d\n", ip, slots))
			case kubeflow.MPIImplementationIntel, kubeflow.MPIImplementationMPICH:
				hostfile.WriteString(fmt.Sprintf("%s:%d\n", ip, slots))
			}
		} else {
			return
		}
	}

	ipMap := make(map[string]string, workerReplicas(mpiJob)+horkerReplicas(mpiJob))
	for _, pod := range workers {
		ip := pod.Status.PodIP
		if len(strings.Split(ip, ".")) == 4 {
			ipMap[pod.Name] = ip
		} else {
			return
		}
	}
	for _, pod := range horkers {
		ip := pod.Status.PodIP
		if len(strings.Split(ip, ".")) == 4 {
			ipMap[pod.Name] = ip
		} else {
			return
		}
	}

	for i := 0; i < int(workerReplicas(mpiJob)); i++ {
		name := workerName(mpiJob, i)
		if ip, ok := ipMap[name]; ok {
			if chiefIP == "" && i == 0 {
				chiefIP = ip
			}
			ips.WriteString(fmt.Sprintf("%s:%d,", ip, slots))
			switch mpiJob.Spec.MPIImplementation {
			case kubeflow.MPIImplementationOpenMPI:
				hostfile.WriteString(fmt.Sprintf("%s slots=%d\n", ip, slots))
			case kubeflow.MPIImplementationIntel, kubeflow.MPIImplementationMPICH:
				hostfile.WriteString(fmt.Sprintf("%s:%d\n", ip, slots))
			default:
				hostfile.WriteString(fmt.Sprintf("%s slots=%d\n", ip, slots))
			}
		} else {
			return
		}
	}
	for i := 0; i < int(horkerReplicas(mpiJob)); i++ {
		name := horkerName(mpiJob, i)
		if ip, ok := ipMap[name]; ok {
			if chiefIP == "" && i == 0 {
				chiefIP = ip
			}
			ips.WriteString(fmt.Sprintf("%s:%d,", ip, slots))
			switch mpiJob.Spec.MPIImplementation {
			case kubeflow.MPIImplementationOpenMPI:
				hostfile.WriteString(fmt.Sprintf("%s slots=%d\n", ip, slots))
			case kubeflow.MPIImplementationIntel, kubeflow.MPIImplementationMPICH:
				hostfile.WriteString(fmt.Sprintf("%s:%d\n", ip, slots))
			default:
				hostfile.WriteString(fmt.Sprintf("%s slots=%d\n", ip, slots))
			}
		} else {
			return
		}
	}

	if jobRole == "launcher" && ipList != "" && hostList != "" {
		ips.WriteString(ipList)
		hostfile.WriteString(hostList)
	}

	nodeList := getNodeList(mpiJob)
	hostNum := int(workerReplicas(mpiJob)) + int(horkerReplicas(mpiJob))
	if enableLauncherAsWorker(mpiJob) && launcher != nil {
		hostNum += 1
	}
	nodeNum := hostNum * slots

	var env bytes.Buffer
	env.WriteString(fmt.Sprintf("export CHIEF_IP=%s\n", chiefIP))
	env.WriteString(fmt.Sprintf("export NODE_IP_LIST=%s\n", strings.TrimSuffix(ips.String(), ",")))
	env.WriteString(fmt.Sprintf("export NODE_LIST=%s\n", nodeList))
	env.WriteString(fmt.Sprintf("export MPI_HOST_NUM=%d\n", hostNum))
	env.WriteString(fmt.Sprintf("export MPI_NODE_NUM=%d\n", nodeNum))
	env.WriteString(fmt.Sprintf("export OMPI_MCA_orte_default_hostfile=%s/%s\n", configMountPath, hostfileName))
	env.WriteString("export OMPI_MCA_orte_keep_fqdn_hostnames=true\n")

	configMap.Data[hostfileName] = hostfile.String()
	configMap.Data[envConfig] = env.String()
}

func getIPList(mpiJob *kubeflow.ResilientJob, slots int) string {
	var ips bytes.Buffer
	ipList := getAnnotation(mpiJob, heterIPListKey)
	if ipList != "" {
		for _, ip := range strings.Split(ipList, ",") {
			ips.WriteString(fmt.Sprintf("%s:%d,", ip, slots))
		}
		return ips.String()
	}
	return ""
}

func getHostList(mpiJob *kubeflow.ResilientJob, slots int) string {
	var hostfile bytes.Buffer
	ipList := getAnnotation(mpiJob, heterIPListKey)
	if ipList != "" {
		for _, ip := range strings.Split(ipList, ",") {
			switch mpiJob.Spec.MPIImplementation {
			case kubeflow.MPIImplementationOpenMPI:
				hostfile.WriteString(fmt.Sprintf("%s slots=%d\n", ip, slots))
			case kubeflow.MPIImplementationIntel, kubeflow.MPIImplementationMPICH:
				hostfile.WriteString(fmt.Sprintf("%s:%d\n", ip, slots))
			}
		}
		return hostfile.String()
	}
	return ""
}

func getDiscoverHosts() string {
	var buffer bytes.Buffer
	buffer.WriteString("#!/bin/sh\n")
	buffer.WriteString(fmt.Sprintf("cat %s/%s | awk '{print $1}'\n", configMountPath, hostfileName))

	return buffer.String()
}

func getHostfile(mpiJob *kubeflow.ResilientJob) string {
	if workerReplicas(mpiJob)+horkerReplicas(mpiJob) >= largeScaleThold {
		return ""
	}

	var buffer bytes.Buffer
	slots := 1
	if mpiJob.Spec.SlotsPerWorker != nil {
		slots = int(*mpiJob.Spec.SlotsPerWorker)
	}

	if enableLauncherAsWorker(mpiJob) {
		switch mpiJob.Spec.MPIImplementation {
		case kubeflow.MPIImplementationOpenMPI:
			buffer.WriteString(fmt.Sprintf("%s slots=%d\n", launcherService(mpiJob), slots))
		case kubeflow.MPIImplementationIntel, kubeflow.MPIImplementationMPICH:
			buffer.WriteString(fmt.Sprintf("%s:%d\n", launcherService(mpiJob), slots))
		}
	}

	for i := 0; i < int(workerReplicas(mpiJob)); i++ {
		switch mpiJob.Spec.MPIImplementation {
		case kubeflow.MPIImplementationOpenMPI:
			buffer.WriteString(fmt.Sprintf("%s slots=%d\n", workerService(mpiJob, i), slots))
		case kubeflow.MPIImplementationIntel, kubeflow.MPIImplementationMPICH:
			buffer.WriteString(fmt.Sprintf("%s:%d\n", workerService(mpiJob, i), slots))
		}
	}
	for i := 0; i < int(horkerReplicas(mpiJob)); i++ {
		switch mpiJob.Spec.MPIImplementation {
		case kubeflow.MPIImplementationOpenMPI:
			buffer.WriteString(fmt.Sprintf("%s slots=%d\n", horkerService(mpiJob, i), slots))
		case kubeflow.MPIImplementationIntel, kubeflow.MPIImplementationMPICH:
			buffer.WriteString(fmt.Sprintf("%s:%d\n", horkerService(mpiJob, i), slots))
		}
	}

	return buffer.String()
}

// getConfigSSHPort set ssh port by env MPI_PORT
func getConfigSSHPort(mpiJob *kubeflow.ResilientJob) string {
	if ok, v := getJobEnv(mpiJob, "MPI_PORT"); ok {
		return v
	}
	return "36000"
}

func disablePasswordLogin(mpiJob *kubeflow.ResilientJob) bool {
	if ok, v := getJobEnv(mpiJob, "PASSWORD"); !ok || v == "" {
		return true
	}
	return false
}

func getSSHDConfig(mpiJob *kubeflow.ResilientJob) string {
	var sshdBuffer bytes.Buffer
	sshdBuffer.WriteString("StrictModes no\n")
	sshdBuffer.WriteString(fmt.Sprintf("HostKey %s\n", filepath.Join(sshConfigPath, sshPrivateHostRsaKey)))
	sshdBuffer.WriteString("PermitUserEnvironment yes\n")
	sshdBuffer.WriteString("AcceptEnv *\n")
	sshdBuffer.WriteString("UsePAM yes\n")
	sshdBuffer.WriteString("AllowUsers root\n")

	sshdBuffer.WriteString("PermitRootLogin yes\n")
	if disablePasswordLogin(mpiJob) {
		sshdBuffer.WriteString("PasswordAuthentication no\n")
	}
	portConf := fmt.Sprintf("Port %s\n", getConfigSSHPort(mpiJob))
	sshdBuffer.WriteString(portConf)

	sshdBuffer.WriteString("ListenAddress 0.0.0.0\n")

	return sshdBuffer.String()
}

func getSSHConfig(mpiJob *kubeflow.ResilientJob) string {
	portConf := fmt.Sprintf("    Port %s\n", getConfigSSHPort(mpiJob))

	var sshBuffer bytes.Buffer
	sshBuffer.WriteString("LogLevel ERROR\n")
	sshBuffer.WriteString("Host *\n")
	sshBuffer.WriteString(portConf)
	sshBuffer.WriteString("    StrictHostKeyChecking no\n")
	sshBuffer.WriteString("    UserKnownHostsFile /dev/null\n")

	return sshBuffer.String()
}

// getNodeList return hostname list
func getNodeList(mpiJob *kubeflow.ResilientJob) string {
	if workerReplicas(mpiJob)+horkerReplicas(mpiJob) >= largeScaleThold {
		return ""
	}

	var buffer bytes.Buffer
	slots := 1
	if mpiJob.Spec.SlotsPerWorker != nil {
		slots = int(*mpiJob.Spec.SlotsPerWorker)
	}

	if enableLauncherAsWorker(mpiJob) && hasLauncher(mpiJob) {
		buffer.WriteString(fmt.Sprintf("%s:%d,", launcherService(mpiJob), slots))
	}

	for i := 0; i < int(workerReplicas(mpiJob)); i++ {
		buffer.WriteString(fmt.Sprintf("%s:%d,", workerService(mpiJob, i), slots))
	}
	for i := 0; i < int(horkerReplicas(mpiJob)); i++ {
		buffer.WriteString(fmt.Sprintf("%s:%d,", horkerService(mpiJob, i), slots))
	}

	return strings.TrimSuffix(buffer.String(), ",")
}

func updateRecoverStateFromAnnotation(configMap *corev1.ConfigMap, mpiJob *kubeflow.ResilientJob) error {
	recoverState := getAnnotation(mpiJob, enableRecover)
	configMap.Data[recoverFileName] = recoverState
	return nil
}
