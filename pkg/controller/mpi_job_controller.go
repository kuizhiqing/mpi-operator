// Copyright 2020 The Kubeflow Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	schedulinginformers "k8s.io/client-go/informers/scheduling/v1"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	"k8s.io/utils/clock"
	"k8s.io/utils/pointer"
	schedclientset "sigs.k8s.io/scheduler-plugins/pkg/generated/clientset/versioned"
	volcanoclient "volcano.sh/apis/pkg/client/clientset/versioned"

	"github.com/kubeflow/mpi-operator/cmd/mpi-operator/app/options"
	kubeflow "github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/v2beta1"
	"github.com/kubeflow/mpi-operator/pkg/apis/kubeflow/validation"
	clientset "github.com/kubeflow/mpi-operator/pkg/client/clientset/versioned"
	"github.com/kubeflow/mpi-operator/pkg/client/clientset/versioned/scheme"
	informers "github.com/kubeflow/mpi-operator/pkg/client/informers/externalversions/kubeflow/v2beta1"
	listers "github.com/kubeflow/mpi-operator/pkg/client/listers/kubeflow/v2beta1"
)

const (
	controllerAgentName     = "mpi-job-controller"
	configSuffix            = "-config"
	configVolumeName        = "mpi-job-config"
	configMountPath         = "/etc/mpi"
	mpirunWrapper           = "mpirun-wrapper"
	mpirunWrapperPath       = "/usr/bin"
	hostfileName            = "hostfile"
	recoverFileName         = "recover"
	sshdConfig              = "sshdconfig"
	envConfig               = "environ"
	sshConfig               = "sshconfig"
	sshConfigPath           = "/etc/ssh/"
	sshConfigName           = "ssh_config"
	sshdConfigName          = "sshd_config"
	discoverHostsScriptName = "discover_hosts.sh"
	sshAuthSecretSuffix     = "-ssh"
	sshAuthVolume           = "ssh-auth"
	hostKeyVolume           = "host-key"
	rootSSHPath             = "/root/.ssh"
	launcher                = "launcher"
	worker                  = "worker"
	horker                  = "horker"
	labelGroupName          = "group-name"
	labelMPIJobName         = "mpi-job-name"
	labelMPIRoleType        = "mpi-job-role"
	sshPublicKey            = "ssh-publickey"
	sshPrivateKeyFile       = "id_rsa"
	sshPublicKeyFile        = sshPrivateKeyFile + ".pub"
	sshPrivateHostRsaKey    = "ssh_host_rsa_key"
	sshPublicHostRsaKey     = sshPrivateHostRsaKey + ".pub"
	sshAuthorizedKeysFile   = "authorized_keys"
	configMapVolumeVersion  = "configmap-volume-version"

	elasticLableName  = "kubeflow.org/elastic"
	enableRecover     = "kubeflow.org/recover"
	launcherAsWorker  = "kubeflow.org/launcher-as-worker"
	frozenAnnotation  = "kubeflow.org/frozen"
	noRestartExitCode = 222
	largeScaleThold   = 200
	restartLimitThold = 100
)

const (
	// ErrResourceExists is used as part of the Event 'reason' when an MPIJob
	// fails to sync due to dependent resources of the same name already
	// existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to dependent resources already existing.
	MessageResourceExists = "Resource %q of Kind %q already exists and is not managed by MPIJob"

	// ValidationError is used as part of the Event 'reason' when failed to
	// validate an MPIJob.
	ValidationError = "ValidationError"

	// podTemplateRestartPolicyReason is the warning reason when the restart
	// policy is set in pod template.
	podTemplateRestartPolicyReason = "SetPodTemplateRestartPolicy"

	// eventMessageLimit is the maximum size of an Event's message.
	// From: k8s.io/kubernetes/pkg/apis/core/validation/events.go
	eventMessageLimit = 1024

	openMPISlotsEnv  = "OMPI_MCA_orte_set_default_slots"
	intelMPISlotsEnv = "I_MPI_PERHOST"
)

var (
	mpiJobsCreatedCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mpi_operator_jobs_created_total",
		Help: "Counts number of MPI jobs created",
	})
	mpiJobsSuccessCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mpi_operator_jobs_successful_total",
		Help: "Counts number of MPI jobs successful",
	})
	mpiJobsFailureCount = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mpi_operator_jobs_failed_total",
		Help: "Counts number of MPI jobs failed",
	})
	mpiJobInfoGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "mpi_operator_job_info",
		Help: "Information about MPIJob",
	}, []string{"launcher", "namespace"})

	configVolumeItems = []corev1.KeyToPath{
		{
			Key:  recoverFileName,
			Path: recoverFileName,
			Mode: newInt32(0444),
		},
		{
			Key:  hostfileName,
			Path: hostfileName,
			Mode: newInt32(0444),
		},
		{
			Key:  discoverHostsScriptName,
			Path: discoverHostsScriptName,
			Mode: newInt32(0555),
		},
		{
			Key:  envConfig,
			Path: envConfig,
			Mode: newInt32(0555),
		},
	}
	launcherEnvVars = []corev1.EnvVar{
		{
			Name:  "K_MPI_JOB_ROLE",
			Value: launcher,
		},
	}
	workerEnvVars = []corev1.EnvVar{
		{
			Name:  "K_MPI_JOB_ROLE",
			Value: worker,
		},
	}
	ompiEnvVars = []corev1.EnvVar{
		// Allows driver to reach workers through the Service.
		{
			Name:  "OMPI_MCA_orte_keep_fqdn_hostnames",
			Value: "true",
		},
		{
			Name:  "OMPI_MCA_orte_default_hostfile",
			Value: fmt.Sprintf("%s/%s", configMountPath, hostfileName),
		},
		{
			Name:  "OMPI_MCA_plm_rsh_args",
			Value: "-o ConnectionAttempts=10",
		},
		{
			Name:  "OMPI_ALLOW_RUN_AS_ROOT",
			Value: "1",
		},
		{
			Name:  "OMPI_ALLOW_RUN_AS_ROOT_CONFIRM",
			Value: "1",
		},
	}
	intelEnvVars = []corev1.EnvVar{
		{
			Name:  "I_MPI_HYDRA_HOST_FILE",
			Value: fmt.Sprintf("%s/%s", configMountPath, hostfileName),
		},
		{
			Name:  "I_MPI_HYDRA_BOOTSTRAP_EXEC_EXTRA_ARGS",
			Value: "-o ConnectionAttempts=10",
		},
	}
	mpichEnvVars = []corev1.EnvVar{
		{
			Name:  "HYDRA_HOST_FILE",
			Value: fmt.Sprintf("%s/%s", configMountPath, hostfileName),
		},
		{
			Name:  "HYDRA_LAUNCH_EXTRA_ARGS",
			Value: "-o ConnectionAttempts=10",
		},
	}
)

// MPIJobController is the controller implementation for MPIJob resources.
type MPIJobController struct {
	// kubeClient is a standard kubernetes clientset.
	kubeClient kubernetes.Interface
	// kubeflowClient is a clientset for our own API group.
	kubeflowClient clientset.Interface
	// PodGroupCtrl is a client for PodGroups (volcano and scheduler-plugins).
	PodGroupCtrl PodGroupControl

	eventLister         corelisters.EventLister
	eventSynced         cache.InformerSynced
	configMapLister     corelisters.ConfigMapLister
	configMapSynced     cache.InformerSynced
	secretLister        corelisters.SecretLister
	secretSynced        cache.InformerSynced
	serviceLister       corelisters.ServiceLister
	serviceSynced       cache.InformerSynced
	podLister           corelisters.PodLister
	podSynced           cache.InformerSynced
	podGroupSynced      cache.InformerSynced
	priorityClassLister schedulinglisters.PriorityClassLister
	priorityClassSynced cache.InformerSynced
	mpiJobLister        listers.MPIJobLister
	mpiJobSynced        cache.InformerSynced

	// queue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	queue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder

	// To allow injection of updateStatus for testing.
	updateStatusHandler func(mpijob *kubeflow.MPIJob) error

	// Clock for internal use of unit-testing
	clock clock.WithTicker

	excludeNamespaces map[string]bool
	includeNamespaces map[string]bool

	maxBackoffLimit int
}

// NewMPIJobController returns a new MPIJob controller.
func NewMPIJobController(
	kubeClient kubernetes.Interface,
	kubeflowClient clientset.Interface,
	volcanoClient volcanoclient.Interface,
	schedClient schedclientset.Interface,
	eventInformer coreinformers.EventInformer,
	configMapInformer coreinformers.ConfigMapInformer,
	secretInformer coreinformers.SecretInformer,
	serviceInformer coreinformers.ServiceInformer,
	podInformer coreinformers.PodInformer,
	priorityClassInformer schedulinginformers.PriorityClassInformer,
	mpiJobInformer informers.MPIJobInformer,
	namespace, gangSchedulingName, exNamespaces, inNamespaces string,
	maxBackoffLimit int) *MPIJobController {
	return NewMPIJobControllerWithClock(kubeClient, kubeflowClient, volcanoClient, schedClient,
		eventInformer, configMapInformer, secretInformer, serviceInformer, podInformer,
		priorityClassInformer, mpiJobInformer, &clock.RealClock{}, namespace, gangSchedulingName, exNamespaces, inNamespaces, maxBackoffLimit)
}

// NewMPIJobControllerWithClock returns a new MPIJob controller.
func NewMPIJobControllerWithClock(
	kubeClient kubernetes.Interface,
	kubeflowClient clientset.Interface,
	volcanoClient volcanoclient.Interface,
	schedClient schedclientset.Interface,
	eventInformer coreinformers.EventInformer,
	configMapInformer coreinformers.ConfigMapInformer,
	secretInformer coreinformers.SecretInformer,
	serviceInformer coreinformers.ServiceInformer,
	podInformer coreinformers.PodInformer,
	priorityClassInformer schedulinginformers.PriorityClassInformer,
	mpiJobInformer informers.MPIJobInformer,
	clock clock.WithTicker,
	namespace, gangSchedulingName, exNamespaces, inNamespaces string,
	maxBackoffLimit int) *MPIJobController {

	// Create event broadcaster.
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	// For the gang scheduling.
	var (
		podGroupCtrl        PodGroupControl
		podGroupSynced      cache.InformerSynced
		priorityClassLister schedulinglisters.PriorityClassLister
		priorityClassSynced cache.InformerSynced
	)
	priorityClassLister = priorityClassInformer.Lister()
	priorityClassSynced = priorityClassInformer.Informer().HasSynced
	if gangSchedulingName == options.GangSchedulerVolcano {
		podGroupCtrl = NewVolcanoCtrl(volcanoClient, namespace, priorityClassLister)
	} else if len(gangSchedulingName) != 0 {
		// Use scheduler-plugins as a default gang-scheduler.
		podGroupCtrl = NewSchedulerPluginsCtrl(schedClient, namespace, gangSchedulingName, priorityClassLister)
	}
	if podGroupCtrl != nil {
		podGroupSynced = podGroupCtrl.PodGroupSharedIndexInformer().HasSynced
	}

	excludeNamespaces := splitStringToMapByComma(exNamespaces)
	includeNamespaces := splitStringToMapByComma(inNamespaces)

	qps := 200
	burst := 2000
	controller := &MPIJobController{
		kubeClient:          kubeClient,
		kubeflowClient:      kubeflowClient,
		PodGroupCtrl:        podGroupCtrl,
		eventLister:         eventInformer.Lister(),
		eventSynced:         eventInformer.Informer().HasSynced,
		configMapLister:     configMapInformer.Lister(),
		configMapSynced:     configMapInformer.Informer().HasSynced,
		secretLister:        secretInformer.Lister(),
		secretSynced:        secretInformer.Informer().HasSynced,
		serviceLister:       serviceInformer.Lister(),
		serviceSynced:       serviceInformer.Informer().HasSynced,
		podLister:           podInformer.Lister(),
		podSynced:           podInformer.Informer().HasSynced,
		podGroupSynced:      podGroupSynced,
		priorityClassLister: priorityClassLister,
		priorityClassSynced: priorityClassSynced,
		mpiJobLister:        mpiJobInformer.Lister(),
		mpiJobSynced:        mpiJobInformer.Informer().HasSynced,
		queue: workqueue.NewNamedRateLimitingQueue(
			workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 10*time.Second),
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(qps), burst)},
			), "MPIJobs"),
		recorder:          recorder,
		clock:             clock,
		excludeNamespaces: excludeNamespaces,
		includeNamespaces: includeNamespaces,
		maxBackoffLimit:   maxBackoffLimit,
	}

	controller.updateStatusHandler = controller.doUpdateJobStatus

	klog.Info("Setting up event handlers")
	// Set up an event handler for when MPIJob resources change.
	mpiJobInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.filterMPIJob,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: controller.addMPIJob,
			UpdateFunc: func(old, new interface{}) {
				controller.enqueueMPIJob(new)
			},
		},
	})

	// Set up an event handler for when dependent resources change. This
	// handler will lookup the owner of the given resource, and if it is
	// owned by an MPIJob resource will enqueue that MPIJob resource for
	// processing. This way, we don't need to implement custom logic for
	// handling dependent resources. More info on this pattern:
	// https://github.com/kubernetes/community/blob/8cafef897a22026d42f5e5bb3f104febe7e29830/contributors/devel/controllers.md
	configMapInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleObject,
		UpdateFunc: controller.handleObjectUpdate,
		DeleteFunc: controller.handleObject,
	})
	secretInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleObject,
		UpdateFunc: controller.handleObjectUpdate,
		DeleteFunc: controller.handleObject,
	})
	serviceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleObject,
		UpdateFunc: controller.handleObjectUpdate,
		DeleteFunc: controller.handleObject,
	})
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.handleObject,
		UpdateFunc: controller.handleObjectUpdate,
		DeleteFunc: controller.handleObject,
	})
	if podGroupCtrl != nil {
		podGroupCtrl.PodGroupSharedIndexInformer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    controller.handleObject,
			UpdateFunc: controller.handleObjectUpdate,
			DeleteFunc: controller.handleObject,
		})
		priorityClassInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    controller.handleObject,
			UpdateFunc: controller.handleObjectUpdate,
			DeleteFunc: controller.handleObject,
		})
	}
	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the work queue and wait for
// workers to finish processing their current work items.
func (c *MPIJobController) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	// Start the informer factories to begin populating the informer caches.
	klog.Info("Starting MPIJob controller")

	// Wait for the caches to be synced before starting workers.
	klog.Info("Waiting for informer caches to sync")
	synced := []cache.InformerSynced{
		c.eventSynced,
		c.configMapSynced,
		c.secretSynced,
		c.serviceSynced,
		c.podSynced,
		c.mpiJobSynced,
	}
	if c.PodGroupCtrl != nil {
		synced = append(synced, c.podGroupSynced, c.priorityClassSynced)
	}
	if ok := cache.WaitForCacheSync(stopCh, synced...); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	// Launch workers to process MPIJob resources.
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// work queue.
func (c *MPIJobController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the work queue and
// attempt to process it, by calling the syncHandler.
func (c *MPIJobController) processNextWorkItem() bool {
	obj, shutdown := c.queue.Get()

	if shutdown {
		return false
	}

	klog.Infof("Current queue length %d", c.queue.Len())

	// We wrap this block in a func so we can defer c.queue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the work queue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the work queue and attempted again after a back-off
		// period.
		defer c.queue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the work queue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// work queue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// work queue.
		if key, ok = obj.(string); !ok {
			// As the item in the work queue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.queue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// MPIJob resource to be synced.
		if err := c.syncHandler(key); err != nil {
			c.queue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.queue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the MPIJob resource
// with the current status of the resource.
func (c *MPIJobController) syncHandler(key string) error {
	startTime := c.clock.Now()

	// Convert the namespace/name string into a distinct namespace and name.
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	defer func() {
		klog.Infof("Finished syncing job %q (%v)", key, c.clock.Since(startTime))
	}()

	// Get the MPIJob with this namespace/name.
	sharedJob, err := c.mpiJobLister.MPIJobs(namespace).Get(name)
	if err != nil {
		// The MPIJob may no longer exist, in which case we stop processing.
		if errors.IsNotFound(err) {
			klog.V(4).Infof("MPIJob has been deleted: %v", key)
			return nil
		}
		return fmt.Errorf("obtaining job: %w", err)
	}

	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance
	mpiJob := sharedJob.DeepCopy()
	// Set default for the new mpiJob.
	scheme.Scheme.Default(mpiJob)

	// for mpi job that is terminating, just return.
	if mpiJob.DeletionTimestamp != nil {
		return nil
	}

	// If the MPIJob is marked as frozen via the `kubeflow.org/frozen=true`
	// annotation, pause it: do not change status, do not check for
	// failures, do not clean up or create pods. The previous behavior
	// resumes once the annotation is removed or set back to "false".
	if isFrozen(mpiJob) {
		klog.V(4).Infof("MPIJob %s/%s is marked frozen; skipping reconciliation", mpiJob.Namespace, mpiJob.Name)
		return nil
	}

	if errs := validation.ValidateMPIJob(mpiJob); len(errs) != 0 {
		msg := truncateMessage(fmt.Sprintf("Found validation errors: %v", errs.ToAggregate()))
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ValidationError, msg)
		// Do not requeue
		return nil
	}

	if len(mpiJob.Status.Conditions) == 0 {
		msg := fmt.Sprintf("MPIJob %s/%s is created.", mpiJob.Namespace, mpiJob.Name)
		updateMPIJobConditions(mpiJob, kubeflow.JobCreated, corev1.ConditionTrue, mpiJobCreatedReason, msg)
		c.recorder.Event(mpiJob, corev1.EventTypeNormal, "MPIJobCreated", msg)
		mpiJobsCreatedCount.Inc()
	}

	// CompletionTime is only filled when the launcher Job succeeded or stopped
	// retrying (it reached .spec.backoffLimit). If it's filled, we want to
	// cleanup and stop retrying the MPIJob.
	if isFinished(mpiJob.Status) && mpiJob.Status.CompletionTime != nil {
		c.cleanLauncherIfNeed(mpiJob) // nolint: errcheck
		if isCleanUpPods(mpiJob.Spec.RunPolicy.CleanPodPolicy) {
			if err := c.cleanUpPods(mpiJob); err != nil {
				return err
			}
			return c.updateStatusHandler(mpiJob)
		}
		return nil
	}

	// first set StartTime.
	if mpiJob.Status.StartTime == nil && !isMPIJobSuspended(mpiJob) {
		now := metav1.Now()
		mpiJob.Status.StartTime = &now
	}

	// Get the launcher Pod for this MPIJob.
	launcher, err := c.getLauncherPod(mpiJob)
	if err != nil {
		return err
	}

	var worker []*corev1.Pod
	var horker []*corev1.Pod
	// We're done if the launcher either succeeded or failed.
	done := launcher != nil && isJobFinished(launcher)
	if !done || noLauncher(mpiJob) {
		_, err := c.getOrCreateService(mpiJob, newJobService(mpiJob))
		if err != nil {
			return fmt.Errorf("getting or creating Service to front workers: %w", err)
		}

		if config, err := c.getOrCreateConfigMap(mpiJob); config == nil || err != nil {
			return fmt.Errorf("getting or creating ConfigMap: %w", err)
		}

		if secret, err := c.getOrCreateSSHAuthSecret(mpiJob); secret == nil || err != nil {
			return fmt.Errorf("creating SSH auth secret: %w", err)
		}

		if config, err := c.getOrCreateMPIRunWrapperConfigMap(mpiJob); config == nil || err != nil {
			return fmt.Errorf("creating mpirun-wrapper: %w", err)
		}

		if !isMPIJobSuspended(mpiJob) {
			// Get the PodGroup for this MPIJob
			if c.PodGroupCtrl != nil {
				if podGroup, err := c.getOrCreatePodGroups(mpiJob); podGroup == nil || err != nil {
					return err
				}
			}
			worker, err = c.getOrCreateWorker(mpiJob)
			if err != nil {
				return err
			}
			horker, err = c.getOrCreateHorker(mpiJob)
			if err != nil {
				return err
			}
		}

		if launcher == nil && !noLauncher(mpiJob) {
			if isAlreadyRun(mpiJob.Status) && !elasticEnabled(mpiJob) {
				klog.Infof("MPIJob %s/%s: skip creating launcher", mpiJob.Namespace, mpiJob.Name)
			} else {
				if mpiJob.Spec.LauncherCreationPolicy == kubeflow.LauncherCreationPolicyAtStartup ||
					(c.countReadyPods(worker) == len(worker) && c.countReadyPods(horker) == len(horker)) {
					launcher, err = c.kubeClient.CoreV1().Pods(namespace).Create(context.TODO(), c.newLauncherPod(mpiJob), metav1.CreateOptions{})
					if err != nil {
						c.recorder.Eventf(mpiJob, corev1.EventTypeWarning, mpiJobFailedReason, "launcher pod created failed: %v", err)
						return fmt.Errorf("creating launcher Pod: %w", err)
					}
				} else {
					klog.V(4).Infof("Waiting for workers %s/%s to start.", mpiJob.Namespace, mpiJob.Name)
				}
			}
		}
	}

	if launcher != nil || noLauncher(mpiJob) {
		if err := c.cleanFailedPods(mpiJob, launcher, worker); err != nil {
			return err
		}
		if err := c.cleanFailedPods(mpiJob, launcher, horker); err != nil {
			return err
		}
		if err := c.syncPodImage(mpiJob, launcher, worker, horker); err != nil {
			return err
		}
	}

	// cleanup the running worker pods if the MPI job is suspended
	if isMPIJobSuspended(mpiJob) {
		if err := c.cleanUpPods(mpiJob); err != nil {
			return err
		}
	}

	// Finally, we update the status block of the MPIJob resource to reflect the
	// current state of the world.
	err = c.updateMPIJobStatus(mpiJob, launcher, worker, horker)
	if err != nil {
		return err
	}

	return nil
}

func (c *MPIJobController) cleanUpPods(mpiJob *kubeflow.MPIJob) error {
	if err := c.deletePods(mpiJob); err != nil {
		return err
	}
	initializeMPIJobStatuses(mpiJob, kubeflow.MPIReplicaTypeWorker)
	initializeMPIJobStatuses(mpiJob, kubeflow.MPIReplicaTypeHorker)
	if c.PodGroupCtrl != nil {
		if err := c.deletePodGroups(mpiJob); err != nil {
			return err
		}
	}
	mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeWorker].Active = 0
	mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeHorker].Active = 0
	return nil
}

// cleanLauncherIfNeed always delete launcher pod after job completed: failed or succeed
func (c *MPIJobController) cleanLauncherIfNeed(mpiJob *kubeflow.MPIJob) error {
	launcher, err := c.getLauncherPod(mpiJob)
	if launcher == nil || err != nil {
		return err
	}
	restarts := getRestartCount(launcher)
	msg := fmt.Sprintf("Launcher %s/%s is deleted with restat count %d.", mpiJob.Namespace, launcher.Name, restarts)
	c.recorder.Event(mpiJob, corev1.EventTypeWarning, "LauncherDeleted", msg)
	klog.Info(msg)
	if err := c.kubeClient.CoreV1().Pods(launcher.Namespace).Delete(context.TODO(), launcher.Name, metav1.DeleteOptions{}); err != nil {
		return err
	}
	return nil
}

func (c *MPIJobController) cleanFailedPods(mpiJob *kubeflow.MPIJob, launcher *corev1.Pod, worker []*corev1.Pod) error {
	if !elasticEnabled(mpiJob) {
		return nil
	}

	if launcher != nil && isPodEvicted(launcher) {
		msg := fmt.Sprintf("Pod %s/%s is evicted then deleted.", mpiJob.Namespace, launcher.Name)
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, "EvictedPodDeleted", msg)
		klog.Info(msg)
		if err := c.kubeClient.CoreV1().Pods(launcher.Namespace).Delete(context.TODO(), launcher.Name, metav1.DeleteOptions{}); err != nil {
			return err
		}
	}

	// delete pod to perform elastic feature on
	// launcher is running or some worker is running
	if hasLauncher(mpiJob) {
		if launcher == nil || !isPodRunning(launcher) {
			return nil
		}
	} else {
		running := 0
		for _, pod := range worker {
			if pod != nil && isPodRunning(pod) {
				running += 1
			}
		}
		if running == 0 {
			return nil
		}
	}

	for _, pod := range worker {
		if pod != nil && isPodFailed(pod) {
			msg := fmt.Sprintf("Pod %s/%s is failed then deleted.", mpiJob.Namespace, pod.Name)
			c.recorder.Event(mpiJob, corev1.EventTypeWarning, "FailedPodDeleted", msg)
			klog.Info(msg)
			if err := c.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{}); err != nil {
				return err
			}
		}
	}

	return nil
}

func getRestartCount(pod *corev1.Pod) int32 {
	if cs := getMainContainerStatus(pod); cs != nil {
		return cs.RestartCount
	}
	return 0
}

// nolint: unused
// func getCurrentDuation(cs *corev1.ContainerStatus) (time.Duration, error) {
// 	state := cs.State
// 	if state.Running != nil {
// 		start := state.Running.StartedAt
// 		return time.Since(start.Time), nil
// 	}
// 	return 0, fmt.Errorf("no running")
// }

// getPodDuration return the duration of pod since scheduled, including pulling image
func getPodDuration(pod *corev1.Pod) (time.Duration, error) {
	startTime := pod.Status.StartTime
	if startTime != nil {
		return time.Since(startTime.Time), nil
	}
	return 0, fmt.Errorf("no start")
}

// noRestartError return true if no restart is needed
func noRestartError(cs *corev1.ContainerStatus) bool {
	if cs.State.Terminated != nil {
		if cs.State.Terminated.ExitCode == noRestartExitCode {
			return true
		}
		if cs.State.Terminated.Reason == "OOMKilled" {
			return true
		}
	}

	lastState := cs.LastTerminationState
	if lastState.Terminated != nil {
		if lastState.Terminated.ExitCode == noRestartExitCode {
			return true
		}
		if lastState.Terminated.Reason == "OOMKilled" {
			return true
		}
	}
	return false
}

// backoffLimitExceeded return wether to mark the launcher as failed depends on restartCount and running time
func backoffLimitExceeded(pod *corev1.Pod) bool {
	if pod == nil || len(pod.Status.ContainerStatuses) < 1 {
		return false
	}

	total, err := getPodDuration(pod)
	if err != nil {
		return false
	}

	cstatus := getMainContainerStatus(pod)
	if cstatus == nil {
		return false
	}
	if cstatus.State.Running != nil {
		return false
	}

	if noRestartError(cstatus) {
		return true
	}

	restarts := cstatus.RestartCount
	if restarts < 1 {
		return false
	} else if total < time.Duration(1*time.Hour) && restarts >= 3 {
		klog.Info(fmt.Sprintf("backoffLimitExceeded %s, restarts %d, %v", pod.Name, restarts, cstatus.State))
		return true
	} else if total < time.Duration(24*time.Hour) && restarts >= 5 {
		klog.Info(fmt.Sprintf("backoffLimitExceeded %s, restarts %d, %v", pod.Name, restarts, cstatus.State))
		return true
	} else if restarts >= 8 {
		klog.Info(fmt.Sprintf("backoffLimitExceeded %s, restarts %d, %v", pod.Name, restarts, cstatus.State))
		return true
	}
	return false
}

func isLauncherSucceeded(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}

	if pod.Status.Phase == corev1.PodSucceeded {
		return true
	}

	if cs := getMainContainerStatus(pod); cs != nil {
		terminated := cs.State.Terminated
		if terminated != nil && terminated.ExitCode == 0 {
			return true
		}
	}
	return false
}

func isLauncherFailed(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}

	if pod.Status.Phase == corev1.PodFailed {
		return true
	}

	if cs := getMainContainerStatus(pod); cs != nil {
		terminated := cs.State.Terminated
		if terminated != nil && terminated.ExitCode != 0 {
			return true
		}
	}
	return false
}

func isImageEquals(img1, img2 string) bool {
	if img1 == img2 {
		return true
	}
	if strings.HasSuffix(img1, img2) || strings.HasSuffix(img2, img1) {
		return true
	}
	return false
}

func (c *MPIJobController) syncReplicasImage(mpiJob *kubeflow.MPIJob, pods []*corev1.Pod, rtype kubeflow.MPIReplicaType) error {
	if pods != nil {
		workerImg := mpiJob.Spec.MPIReplicaSpecs[rtype].Template.Spec.Containers[0].Image
		for idx := range pods {
			pod := pods[idx]
			if len(pod.Spec.Containers) < 1 {
				continue
			}
			podImg := pod.Spec.Containers[0].Image
			if !isImageEquals(workerImg, podImg) && isPodRunning(pod) {
				// reset pod image
				pod.Spec.Containers[0].Image = workerImg
				_, err := c.kubeClient.CoreV1().Pods(mpiJob.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{})
				if err != nil {
					return err
				}
				klog.Infof("Replace worker pod image from %s to %s", podImg, workerImg)
			}
		}
	}
	return nil
}

func (c *MPIJobController) syncLauncherImage(mpiJob *kubeflow.MPIJob, launcher *corev1.Pod) error {
	if launcher != nil {
		launcherImg := mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.Spec.Containers[0].Image

		if len(launcher.Spec.Containers) < 1 {
			return fmt.Errorf("no container in launcher")
		}
		podImg := launcher.Spec.Containers[0].Image
		if !isImageEquals(launcherImg, podImg) && isPodRunning(launcher) {
			// reset pod image
			launcher.Spec.Containers[0].Image = launcherImg
			_, err := c.kubeClient.CoreV1().Pods(mpiJob.Namespace).Update(context.TODO(), launcher, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
			klog.Infof("Replace launcher pod image from %s to %s", podImg, launcherImg)
		}
	}
	return nil
}

// syncPodImage try to update image of pods according to job image change
func (c *MPIJobController) syncPodImage(mpiJob *kubeflow.MPIJob, launcher *corev1.Pod, worker []*corev1.Pod, horker []*corev1.Pod) error {
	if err := c.syncReplicasImage(mpiJob, worker, kubeflow.MPIReplicaTypeWorker); err != nil {
		return err
	}
	if err := c.syncReplicasImage(mpiJob, horker, kubeflow.MPIReplicaTypeHorker); err != nil {
		return err
	}
	if err := c.syncLauncherImage(mpiJob, launcher); err != nil {
		return err
	}

	return nil
}

// getLauncherPod gets the launcher Job controlled by this MPIJob.
func (c *MPIJobController) getLauncherPod(mpiJob *kubeflow.MPIJob) (*corev1.Pod, error) {
	launcher, err := c.podLister.Pods(mpiJob.Namespace).Get(launcherName(mpiJob))
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		// If an error occurs during Get, we'll requeue the item so we can
		// attempt processing again later. This could have been caused by a
		// temporary network failure, or any other transient reason.
		return nil, err
	}

	// If the launcher is not controlled by this MPIJob resource, we should log
	// a warning to the event recorder and return.
	if !metav1.IsControlledBy(launcher, mpiJob) {
		msg := fmt.Sprintf(MessageResourceExists, launcher.Name, launcher.Kind)
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return launcher, fmt.Errorf(msg)
	}

	return launcher, nil
}

// getOrCreatePodGroups will create a PodGroup for gang scheduling by volcano.
func (c *MPIJobController) getOrCreatePodGroups(mpiJob *kubeflow.MPIJob) (metav1.Object, error) {
	newPodGroup := c.PodGroupCtrl.newPodGroup(mpiJob)
	podGroup, err := c.PodGroupCtrl.getPodGroup(newPodGroup.GetNamespace(), newPodGroup.GetName())
	// If the PodGroup doesn't exist, we'll create it.
	if errors.IsNotFound(err) {
		return c.PodGroupCtrl.createPodGroup(context.TODO(), newPodGroup)
	}
	// If an error occurs during Get/Create, we'll requeue the item so we
	// can attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return nil, err
	}
	// If the PodGroup is not controlled by this MPIJob resource, we
	// should log a warning to the event recorder and return.
	if !metav1.IsControlledBy(podGroup, mpiJob) {
		msg := fmt.Sprintf(MessageResourceExists, podGroup.GetName(), "PodGroup")
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	if !c.PodGroupCtrl.pgSpecsAreEqual(podGroup, newPodGroup) {
		return c.PodGroupCtrl.updatePodGroup(context.TODO(), podGroup, newPodGroup)
	}
	return podGroup, nil
}

// deletePodGroups will delete a PodGroup when MPIJob have done.
func (c *MPIJobController) deletePodGroups(mpiJob *kubeflow.MPIJob) error {
	podGroup, err := c.PodGroupCtrl.getPodGroup(mpiJob.Namespace, mpiJob.Name)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// If the PodGroup is not controlled by this MPIJob resource, we
	// should log a warning to the event recorder and return.
	if !metav1.IsControlledBy(podGroup, mpiJob) {
		msg := fmt.Sprintf(MessageResourceExists, podGroup.GetName(), "PodGroup")
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return fmt.Errorf(msg)
	}

	// If the PodGroup exist, we'll delete it.
	err = c.PodGroupCtrl.deletePodGroup(context.TODO(), mpiJob.Namespace, mpiJob.Name)
	// If an error occurs during Delete, we'll requeue the item so we
	// can attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return err
	}

	return nil
}

// getRunningWorkerPods get all worker Pods with Running phase controlled by this MPIJob.
func (c *MPIJobController) getRunningWorkerPods(mpiJob *kubeflow.MPIJob) ([]*corev1.Pod, error) {
	return c.getRunningPods(mpiJob, kubeflow.MPIReplicaTypeWorker)
}

func (c *MPIJobController) getRunningHorkerPods(mpiJob *kubeflow.MPIJob) ([]*corev1.Pod, error) {
	return c.getRunningPods(mpiJob, kubeflow.MPIReplicaTypeHorker)
}

func (c *MPIJobController) getRunningPods(mpiJob *kubeflow.MPIJob, rtype kubeflow.MPIReplicaType) ([]*corev1.Pod, error) {
	selector, err := getSelector(mpiJob.Name, rtype)
	if err != nil {
		return nil, err
	}
	podFullList, err := c.podLister.Pods(mpiJob.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	// Only running Pods should be included within the `discover_hosts.sh` script.
	var podList []*corev1.Pod
	for idx, pod := range podFullList {
		if pod.Status.Phase == corev1.PodRunning {
			podList = append(podList, podFullList[idx])
		}
	}

	return podList, nil
}

func (c *MPIJobController) countReadyPods(pods []*corev1.Pod) int {
	ready := 0
	for _, pod := range pods {
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
				ready++
				break
			}
		}
	}
	return ready
}

func (c *MPIJobController) getOrCreateService(job *kubeflow.MPIJob, newSvc *corev1.Service) (*corev1.Service, error) {
	svc, err := c.serviceLister.Services(job.Namespace).Get(newSvc.Name)
	if errors.IsNotFound(err) {
		return c.kubeClient.CoreV1().Services(job.Namespace).Create(context.TODO(), newSvc, metav1.CreateOptions{})
	}
	if err != nil {
		return nil, err
	}
	if !metav1.IsControlledBy(svc, job) {
		msg := fmt.Sprintf(MessageResourceExists, svc.Name, svc.Kind)
		c.recorder.Event(job, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	// If the Service selector is changed, update it.
	if !equality.Semantic.DeepEqual(svc.Spec.Selector, newSvc.Spec.Selector) {
		svc = svc.DeepCopy()
		svc.Spec.Selector = newSvc.Spec.Selector
		return c.kubeClient.CoreV1().Services(svc.Namespace).Update(context.TODO(), svc, metav1.UpdateOptions{})
	}

	return svc, nil
}

func (c *MPIJobController) getOrCreateWorker(mpiJob *kubeflow.MPIJob) ([]*corev1.Pod, error) {
	return c.getOrCreateReplicas(mpiJob, kubeflow.MPIReplicaTypeWorker)
}

func (c *MPIJobController) getOrCreateHorker(mpiJob *kubeflow.MPIJob) ([]*corev1.Pod, error) {
	return c.getOrCreateReplicas(mpiJob, kubeflow.MPIReplicaTypeHorker)
}

func (c *MPIJobController) getOrCreateReplicas(mpiJob *kubeflow.MPIJob, rtype kubeflow.MPIReplicaType) ([]*corev1.Pod, error) {
	hosts := getHostListFromAnnotation(mpiJob, rtype)
	if len(hosts) > 0 {
		klog.Infof("MPIJob %s/%s create replicas from annotation", mpiJob.Namespace, mpiJob.Name)
		return c.getOrCreateReplicasByNames(mpiJob, rtype, hosts)
	}

	var pods []*corev1.Pod
	replicas := mpiJob.Spec.MPIReplicaSpecs[rtype]
	if replicas == nil {
		return pods, nil
	}

	// Remove Pods when replicas are scaled down
	selector, err := getSelector(mpiJob.Name, rtype)
	if err != nil {
		return nil, err
	}
	podFullList, err := c.podLister.Pods(mpiJob.Namespace).List(selector)
	if err != nil {
		return nil, err
	}
	if len(podFullList) > int(*replicas.Replicas) {
		for _, pod := range podFullList {
			indexStr, ok := pod.Labels[kubeflow.ReplicaIndexLabel]
			if !ok {
				return nil, err
			}
			index, err := strconv.Atoi(indexStr)
			if err == nil {
				if index >= int(*replicas.Replicas) {
					err = c.kubeClient.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}

	for i := 0; i < int(*replicas.Replicas); i++ {
		pod, err := c.podLister.Pods(mpiJob.Namespace).Get(replicasName(mpiJob, i, rtype))

		// If the worker Pod doesn't exist, we'll create it.
		if errors.IsNotFound(err) {
			if isAlreadyRun(mpiJob.Status) && !elasticEnabled(mpiJob) {
				klog.Infof("MPIJob %s/%s: skip create %s-%d", mpiJob.Namespace, mpiJob.Name, rtype, i)
				continue
			}
			replicas := c.newReplicas(mpiJob, i, rtype)
			pod, err = c.kubeClient.CoreV1().Pods(mpiJob.Namespace).Create(context.TODO(), replicas, metav1.CreateOptions{})
			klog.Infof("MPIJob pod %s/%s created.", mpiJob.Namespace, pod.Name)
		}
		// If an error occurs during Get/Create, we'll requeue the item so we
		// can attempt processing again later. This could have been caused by a
		// temporary network failure, or any other transient reason.
		if err != nil {
			c.recorder.Eventf(mpiJob, corev1.EventTypeWarning, mpiJobFailedReason, "pod created failed: %v", err)
			return nil, err
		}
		// If the worker is not controlled by this MPIJob resource, we should log
		// a warning to the event recorder and return.
		if pod != nil && !metav1.IsControlledBy(pod, mpiJob) {
			msg := fmt.Sprintf(MessageResourceExists, pod.Name, pod.Kind)
			c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
			return nil, fmt.Errorf(msg)
		}
		pods = append(pods, pod)
	}

	return pods, nil
}

func isMPIJobSuspended(mpiJob *kubeflow.MPIJob) bool {
	return pointer.BoolDeref(mpiJob.Spec.RunPolicy.Suspend, false)
}

func (c *MPIJobController) deletePods(mpiJob *kubeflow.MPIJob) error {
	hosts := getFullHostListFromAnnotation(mpiJob)
	if len(hosts) > 0 {
		return c.deletePodsFromAnnotation(mpiJob, hosts)
	}

	if err := c.deleteReplicasPods(mpiJob, kubeflow.MPIReplicaTypeWorker); err != nil {
		return err
	}
	if err := c.deleteReplicasPods(mpiJob, kubeflow.MPIReplicaTypeHorker); err != nil {
		return err
	}
	return nil
}

func (c *MPIJobController) deleteReplicasPods(mpiJob *kubeflow.MPIJob, rtype kubeflow.MPIReplicaType) error {
	worker := mpiJob.Spec.MPIReplicaSpecs[rtype]
	if worker == nil {
		return nil
	}

	for i := 0; i < int(*worker.Replicas); i++ {
		name := replicasName(mpiJob, i, rtype)
		pod, err := c.podLister.Pods(mpiJob.Namespace).Get(name)

		// If the worker Pod doesn't exist, no need to remove it.
		if errors.IsNotFound(err) {
			continue
		}
		// If the worker is not controlled by this MPIJob resource, we should log
		// a warning to the event recorder and return.
		if pod != nil && !metav1.IsControlledBy(pod, mpiJob) {
			msg := fmt.Sprintf(MessageResourceExists, pod.Name, pod.Kind)
			c.recorder.Event(mpiJob, corev1.EventTypeWarning, ErrResourceExists, msg)
			return fmt.Errorf(msg)
		}
		// If the worker pod is not running and cleanupPolicy is
		// set to CleanPodPolicyRunning, keep the pod.
		// Note that pending pod should still be removed under this
		// situation, since it may turn to running in the future.
		if *mpiJob.Spec.RunPolicy.CleanPodPolicy == kubeflow.CleanPodPolicyRunning && !isPodRunning(pod) && !isPodPending(pod) {
			// Keep the worker pod
			continue
		}
		err = c.kubeClient.CoreV1().Pods(mpiJob.Namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			klog.Errorf("Failed to delete pod[%s/%s]: %v", mpiJob.Namespace, name, err)
			return err
		}
	}
	return nil
}

func getFailedMessage(pod *corev1.Pod, prefix string) string {
	reason := prefix
	reasons, details := getFailedInfo(pod)
	if len(reasons) > 0 {
		reason += fmt.Sprintf(": %s.", reasons[0])
	}
	if len(details) > 0 {
		reason += fmt.Sprintf(" %s.", details[0])
	}
	return reason
}

func getFailedInfo(pod *corev1.Pod) ([]string, []string) {
	reasons := []string{}
	details := []string{}

	if pod.Status.Reason != "" {
		reasons = append(reasons, pod.Status.Reason)
	}
	if pod.Status.Message != "" {
		details = append(details, pod.Status.Message)
	}

	for _, is := range pod.Status.InitContainerStatuses {
		terminated := is.State.Terminated
		if terminated != nil && terminated.ExitCode != 0 {
			if terminated.Reason != "" {
				reasons = append(reasons, terminated.Reason)
				if terminated.Message != "" {
					details = append(details, terminated.Message)
				}
			}
		}
	}

	if cs := getMainContainerStatus(pod); cs != nil {
		terminated := cs.State.Terminated
		if terminated != nil && terminated.ExitCode != 0 {
			if terminated.Reason != "" {
				reasons = append(reasons, terminated.Reason)
				if terminated.Message != "" {
					details = append(details, terminated.Message)
				}
			}
		}
		terminated = cs.LastTerminationState.Terminated
		if terminated != nil && terminated.ExitCode != 0 {
			if terminated.Reason != "" {
				reasons = append(reasons, terminated.Reason)
				if terminated.Message != "" {
					details = append(details, terminated.Message)
				}
			}
		}
	}

	return reasons, details
}

// checkJobFailedWithReason check if the job fall into failed phase
// Failed cases:
// 1. elastic + backofflimit reach
// 2. no-elastic + pod failed
func (c *MPIJobController) checkJobFailedWithReason(mpiJob *kubeflow.MPIJob, launcher *corev1.Pod, worker []*corev1.Pod, horker []*corev1.Pod) (bool, string, string) {
	if launcher == nil && hasLauncher(mpiJob) && isAlreadyRun(mpiJob.Status) && !elasticEnabled(mpiJob) {
		return true, "LaucherEvicted", "Launcher missing"
	}

	if launcher != nil && isPodFailed(launcher) {
		if c.isPodPreempted(launcher) {
			return true, "LaucherEvicted", "Launcher preempted"
		} else {
			return true, "LauncherFailed", getFailedMessage(launcher, "Launcher failed")
		}
	}

	if elasticEnabled(mpiJob) {
		// never failed if elastic enable and no launcher
		if noLauncher(mpiJob) {
			return false, "", ""
		}
		if int(workerReplicas(mpiJob))+int(horkerReplicas(mpiJob)) < restartLimitThold {
			if backoffLimitExceeded(launcher) {
				return true, "LauncherFailed", getFailedMessage(launcher, "Launcher restart failed")
			}
		}
	} else {
		if isLauncherFailed(launcher) {
			return true, "LauncherFailed", getFailedMessage(launcher, "Launcher failed")
		}
		if isAlreadyRun(mpiJob.Status) {
			if len(worker) < int(workerReplicas(mpiJob)) {
				return true, "WorkerEvicted", "Worker missing"
			}
			for i, pod := range worker {
				if pod != nil && isPodFailed(pod) {
					if c.isPodPreempted(pod) {
						return true, "WorkerEvicted", fmt.Sprintf("Worker-%d preempted", i)
					} else {
						return true, "WorkerFailed", getFailedMessage(worker[i], fmt.Sprintf("Worker-%d failed", i))
					}
				}
			}
			if len(horker) < int(horkerReplicas(mpiJob)) {
				return true, "HorkerEvicted", "Horker missing"
			}
			for i, pod := range horker {
				if pod != nil && isPodFailed(pod) {
					if c.isPodPreempted(pod) {
						return true, "HorkerEvicted", fmt.Sprintf("Horker-%d preempted", i)
					} else {
						return true, "HorkerFailed", getFailedMessage(horker[i], fmt.Sprintf("Horker-%d failed", i))
					}
				}
			}
		}
	}
	return false, "", ""
}

func (c *MPIJobController) isPodPreempted(pod *corev1.Pod) bool {
	if c.eventLister == nil {
		return false
	}
	events, _ := c.eventLister.Events(pod.Namespace).List(labels.NewSelector())
	for _, event := range events {
		if event.InvolvedObject.Kind == "Pod" &&
			event.InvolvedObject.Name == pod.Name &&
			event.InvolvedObject.UID == pod.UID {
			if event.Reason == "Preempted" {
				klog.Infof("Event %s/%s: %s: %s", event.Namespace, event.Name, event.Reason, event.Message)
				return true
			}
		}
	}
	return false
}

func (c *MPIJobController) updateMPIJobStatus(mpiJob *kubeflow.MPIJob, launcher *corev1.Pod, worker []*corev1.Pod, horker []*corev1.Pod) error {
	oldStatus := mpiJob.Status.DeepCopy()
	if isMPIJobSuspended(mpiJob) {
		// it is suspended now
		if updateMPIJobConditions(mpiJob, kubeflow.JobSuspended, corev1.ConditionTrue, mpiJobSuspendedReason, "MPIJob suspended") {
			c.recorder.Event(mpiJob, corev1.EventTypeNormal, "MPIJobSuspended", "MPIJob suspended")
		}
	} else if getCondition(mpiJob.Status, kubeflow.JobSuspended) != nil {
		// it is not suspended now, consider resumed if the condition was set before
		if updateMPIJobConditions(mpiJob, kubeflow.JobSuspended, corev1.ConditionFalse, mpiJobResumedReason, "MPIJob resumed") {
			c.recorder.Event(mpiJob, corev1.EventTypeNormal, "MPIJobResumed", "MPIJob resumed")
			now := metav1.NewTime(c.clock.Now())
			mpiJob.Status.StartTime = &now
		}
	}
	initializeMPIJobStatuses(mpiJob, kubeflow.MPIReplicaTypeLauncher)
	if launcher != nil {
		if isLauncherSucceeded(launcher) {
			msg := fmt.Sprintf("MPIJob %s/%s successfully completed.", mpiJob.Namespace, mpiJob.Name)
			c.recorder.Event(mpiJob, corev1.EventTypeNormal, mpiJobSucceededReason, msg)
			if mpiJob.Status.CompletionTime == nil {
				now := metav1.Now()
				mpiJob.Status.CompletionTime = &now
			}
			updateMPIJobConditions(mpiJob, kubeflow.JobSucceeded, corev1.ConditionTrue, mpiJobSucceededReason, msg)
			mpiJobsSuccessCount.Inc()
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeLauncher].Succeeded = 1
		} else if isPodRunning(launcher) {
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeLauncher].Active += 1
		} else if isPodFailed(launcher) {
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeLauncher].Failed = 1
		}
		mpiJobInfoGauge.WithLabelValues(launcher.Name, mpiJob.Namespace).Set(1)
	}
	if !isFinished(mpiJob.Status) {
		if failed, reason, message := c.checkJobFailedWithReason(mpiJob, launcher, worker, horker); failed {
			msg := fmt.Sprintf("MPIJob %s/%s failed: %s", mpiJob.Namespace, mpiJob.Name, message)
			c.recorder.Event(mpiJob, corev1.EventTypeWarning, mpiJobFailedReason, truncateMessage(msg))
			if mpiJob.Status.CompletionTime == nil {
				now := metav1.Now()
				mpiJob.Status.CompletionTime = &now
			}
			updateMPIJobConditions(mpiJob, kubeflow.JobFailed, corev1.ConditionTrue, reason, msg)
			mpiJobsFailureCount.Inc()
		}
	}

	initializeMPIJobStatuses(mpiJob, kubeflow.MPIReplicaTypeWorker)
	//spec := mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeWorker]
	workerSucc := 0
	for i := 0; i < len(worker); i++ {
		switch worker[i].Status.Phase {
		case corev1.PodFailed:
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeWorker].Failed += 1
		case corev1.PodSucceeded:
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeWorker].Succeeded += 1
			workerSucc += 1
		case corev1.PodRunning:
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeWorker].Active += 1
		}
	}

	initializeMPIJobStatuses(mpiJob, kubeflow.MPIReplicaTypeHorker)
	horkerSucc := 0
	for i := 0; i < len(horker); i++ {
		switch horker[i].Status.Phase {
		case corev1.PodFailed:
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeHorker].Failed += 1
		case corev1.PodSucceeded:
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeHorker].Succeeded += 1
			horkerSucc += 1
		case corev1.PodRunning:
			mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeHorker].Active += 1
		}
	}

	if mpiJob.Status.CompletionTime == nil {
		if noLauncher(mpiJob) && workerSucc == len(worker) && horkerSucc == len(horker) {
			msg := fmt.Sprintf("MPIJob %s/%s successfully completed.", mpiJob.Namespace, mpiJob.Name)
			c.recorder.Event(mpiJob, corev1.EventTypeNormal, mpiJobSucceededReason, msg)
			now := metav1.Now()
			mpiJob.Status.CompletionTime = &now
			updateMPIJobConditions(mpiJob, kubeflow.JobSucceeded, corev1.ConditionTrue, mpiJobSucceededReason, msg)
		}

		if isMPIJobSuspended(mpiJob) {
			msg := fmt.Sprintf("MPIJob %s/%s is suspended.", mpiJob.Namespace, mpiJob.Name)
			updateMPIJobConditions(mpiJob, kubeflow.JobRunning, corev1.ConditionFalse, mpiJobSuspendedReason, msg)
		} else if allContainerRunning(mpiJob, launcher, worker, horker) {
			msg := fmt.Sprintf("MPIJob %s/%s is running.", mpiJob.Namespace, mpiJob.Name)
			updateMPIJobConditions(mpiJob, kubeflow.JobRunning, corev1.ConditionTrue, mpiJobRunningReason, msg)
			c.recorder.Eventf(mpiJob, corev1.EventTypeNormal, mpiJobRunningReason, "MPIJob %s/%s is running", mpiJob.Namespace, mpiJob.Name)
			updateStatusSelector(mpiJob, launcher, worker, horker)
		}
	}

	// no need to update the mpijob if the status hasn't changed since last time.
	if !reflect.DeepEqual(*oldStatus, mpiJob.Status) {
		return c.updateStatusHandler(mpiJob)
	}
	return nil
}

func updateStatusSelector(mpiJob *kubeflow.MPIJob, launcher *corev1.Pod, workers []*corev1.Pod, horkers []*corev1.Pod) {
	if launcher != nil && launcher.Status.PodIP != "" {
		mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeLauncher].Selector = launcher.Status.PodIP
	}
	if len(workers) > 0 {
		var ips bytes.Buffer
		for _, pod := range workers {
			ip := pod.Status.PodIP
			if ip == "" {
				continue
			}
			ips.WriteString(ip)
			ips.WriteString(",")
		}
		mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeWorker].Selector = strings.TrimSuffix(ips.String(), ",")
	}
	if len(horkers) > 0 {
		var ips bytes.Buffer
		for _, pod := range horkers {
			ip := pod.Status.PodIP
			if ip == "" {
				continue
			}
			ips.WriteString(ip)
			ips.WriteString(",")
		}
		mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeHorker].Selector = strings.TrimSuffix(ips.String(), ",")
	}
}

func (c *MPIJobController) filterMPIJob(obj interface{}) bool {
	mpiJob, ok := obj.(*kubeflow.MPIJob)
	if !ok {
		return false
	}
	return c.filterObjNamespace(mpiJob.Namespace)
}

func (c *MPIJobController) filterObjNamespace(ns string) bool {
	if _, ok := c.excludeNamespaces[ns]; ok {
		return false
	}
	if _, ok := c.includeNamespaces[ns]; ok {
		return true
	}
	// defaults
	if len(c.includeNamespaces) == 0 {
		return true
	} else {
		return false
	}
}

// When a mpiJob is added, set the defaults and enqueue the current mpiJob.
func (c *MPIJobController) addMPIJob(obj interface{}) {
	mpiJob := obj.(*kubeflow.MPIJob)

	// Set default for the new mpiJob.
	scheme.Scheme.Default(mpiJob)
	c.enqueueMPIJob(mpiJob)
}

// enqueueMPIJob takes a MPIJob resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than MPIJob.
func (c *MPIJobController) enqueueMPIJob(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	klog.V(4).Infof("Enqueue object 1: %s", key)
	c.queue.AddRateLimited(key)
	klog.V(4).Infof("Enqueue object 2: %s", key)
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the MPIJob resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that MPIJob resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
func (c *MPIJobController) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		klog.V(4).Infof("Recovered deleted object '%s' from tombstone", object.GetName())
	}
	if !c.filterObjNamespace(object.GetNamespace()) {
		return
	}
	klog.V(4).Infof("Processing object: %s", object.GetName())
	ownerRef, ownerGVK, err := ownerReferenceAndGVK(object)
	if err != nil {
		runtime.HandleError(err)
		return
	}

	// Compare the OwnerReference Group and Kind against the OwnerType Group and Kind.
	// Since we do not support conversion webhook now, we do not deal with v1alpha1/v1alpha2/v1 resources in this operator.
	if ownerGVK.Kind != kubeflow.Kind || ownerGVK.Group != kubeflow.GroupName || ownerGVK.Version != kubeflow.GroupVersion {
		klog.V(4).Infof("invalid owner reference kind or group: %v", ownerRef)
		return
	}

	mpiJob, err := c.mpiJobLister.MPIJobs(object.GetNamespace()).Get(ownerRef.Name)
	if err != nil {
		klog.V(4).Infof("ignoring orphaned object '%s' of mpi job '%s'", object.GetSelfLink(), ownerRef.Name)
		return
	}

	c.enqueueMPIJob(mpiJob)
}

func (c *MPIJobController) handleObjectUpdate(old, new interface{}) {
	oldObj := old.(metav1.Object)
	newObj := new.(metav1.Object)
	if newObj.GetResourceVersion() == oldObj.GetResourceVersion() {
		// Periodic re-sync will send update events for all known
		// ConfigMaps. Two different versions of the same ConfigMap
		// will always have different RVs.
		return
	}
	c.handleObject(new)
}

// doUpdateJobStatus updates the status of the given MPIJob by call apiServer.
func (c *MPIJobController) doUpdateJobStatus(mpiJob *kubeflow.MPIJob) error {
	_, err := c.kubeflowClient.KubeflowV2beta1().MPIJobs(mpiJob.Namespace).UpdateStatus(context.TODO(), mpiJob, metav1.UpdateOptions{})
	return err
}

// newWorker creates a new worker Pod for an MPIJob resource. It also
// sets the appropriate OwnerReferences on the resource so handleObject can
// discover the MPIJob resource that 'owns' it.
func (c *MPIJobController) newReplicas(mpiJob *kubeflow.MPIJob, index int, rtype kubeflow.MPIReplicaType) *corev1.Pod {
	name := replicasName(mpiJob, index, rtype)

	podTemplate := mpiJob.Spec.MPIReplicaSpecs[rtype].Template.DeepCopy()

	setDefaultLabels(podTemplate, mpiJob.Name, rtype)
	podTemplate.Labels[kubeflow.ReplicaIndexLabel] = strconv.Itoa(index)
	podTemplate.Spec.Hostname = name
	podTemplate.Spec.Subdomain = mpiJob.Name
	if podTemplate.Spec.HostNetwork {
		// Allows resolution of worker hostnames without needing to include the
		// namespace or cluster domain.
		podTemplate.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}
	searche := fmt.Sprintf("%s.%s.svc.cluster.local", mpiJob.Name, mpiJob.Namespace)
	if podTemplate.Spec.DNSConfig == nil {
		podTemplate.Spec.DNSConfig = &corev1.PodDNSConfig{Searches: []string{searche}}
	} else {
		podTemplate.Spec.DNSConfig.Searches = append(podTemplate.Spec.DNSConfig.Searches, searche)
	}
	if elasticEnabled(mpiJob) {
		setRestartPolicy(podTemplate, mpiJob.Spec.MPIReplicaSpecs[rtype])
	} else {
		podTemplate.Spec.RestartPolicy = corev1.RestartPolicyNever
	}

	container := &podTemplate.Spec.Containers[0]
	if len(container.Command) == 0 && len(container.Args) == 0 {
		container.Command = []string{"/usr/sbin/sshd", "-De"}
	}
	container.Env = append(container.Env, workerEnvVars...)
	comEnvs := []corev1.EnvVar{
		{
			Name:  "CHIEF",
			Value: launcherService(mpiJob),
		},
		{
			Name:  "INDEX",
			Value: fmt.Sprintf("%d", getAbsIndex(mpiJob, index, rtype)),
		},
		{
			Name:  "LOCAL_HOSTNAME",
			Value: replicasService(mpiJob, index, rtype),
		},
	}
	container.Env = append(container.Env, comEnvs...)
	c.setupSSHOnPod(&podTemplate.Spec, mpiJob)
	c.setupMPIRunWrapperOnPod(&podTemplate.Spec, mpiJob)

	// add SchedulerName to podSpec
	if c.PodGroupCtrl != nil {
		c.PodGroupCtrl.decoratePodTemplateSpec(podTemplate, mpiJob.Name)
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   mpiJob.Namespace,
			Labels:      podTemplate.Labels,
			Annotations: podTemplate.Annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(mpiJob, kubeflow.SchemeGroupVersionKind),
			},
		},
		Spec: podTemplate.Spec,
	}
}

func (c *MPIJobController) newLauncherPod(mpiJob *kubeflow.MPIJob) *corev1.Pod {
	podTemplate := mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher].Template.DeepCopy()
	// copy the labels and annotations to pod from PodTemplate
	if len(podTemplate.Labels) == 0 {
		podTemplate.Labels = make(map[string]string)
	}
	for key, value := range defaultLabels(mpiJob.Name, launcher) {
		podTemplate.Labels[key] = value
	}
	// add SchedulerName to podSpec
	if c.PodGroupCtrl != nil {
		c.PodGroupCtrl.decoratePodTemplateSpec(podTemplate, mpiJob.Name)
	}
	podTemplate.Spec.Hostname = launcherName(mpiJob)
	podTemplate.Spec.Subdomain = mpiJob.Name
	if podTemplate.Spec.HostNetwork {
		// Allows resolution of worker hostnames without needing to include the
		// namespace or cluster domain.
		podTemplate.Spec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
	}
	searche := fmt.Sprintf("%s.%s.svc.cluster.local", mpiJob.Name, mpiJob.Namespace)
	if podTemplate.Spec.DNSConfig == nil {
		podTemplate.Spec.DNSConfig = &corev1.PodDNSConfig{Searches: []string{searche}}
	} else {
		podTemplate.Spec.DNSConfig.Searches = append(podTemplate.Spec.DNSConfig.Searches, searche)
	}
	container := &podTemplate.Spec.Containers[0]
	container.Env = append(container.Env, launcherEnvVars...)
	slotsStr := strconv.Itoa(int(*mpiJob.Spec.SlotsPerWorker))
	switch mpiJob.Spec.MPIImplementation {
	case kubeflow.MPIImplementationOpenMPI:
		container.Env = append(container.Env, ompiEnvVars...)
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  openMPISlotsEnv,
			Value: slotsStr,
		})
	case kubeflow.MPIImplementationIntel:
		container.Env = append(container.Env, intelEnvVars...)
		container.Env = append(container.Env, corev1.EnvVar{
			Name:  intelMPISlotsEnv,
			Value: slotsStr,
		})
	case kubeflow.MPIImplementationMPICH:
		container.Env = append(container.Env, mpichEnvVars...)
	}

	comEnvs := []corev1.EnvVar{
		{
			Name:  "CHIEF",
			Value: launcherService(mpiJob),
		},
		{
			Name:  "INDEX",
			Value: "0",
		},
		{
			Name:  "LOCAL_HOSTNAME",
			Value: launcherService(mpiJob),
		},
	}
	container.Env = append(container.Env, comEnvs...)
	c.setupSSHOnPod(&podTemplate.Spec, mpiJob)
	c.setupMPIRunWrapperOnPod(&podTemplate.Spec, mpiJob)

	// Submit a warning event if the user specifies restart policy for
	// the pod template. We recommend to set it from the replica level.
	if podTemplate.Spec.RestartPolicy != "" {
		errMsg := "Restart policy in pod template overridden by restart policy in replica spec"
		klog.Warning(errMsg)
		c.recorder.Event(mpiJob, corev1.EventTypeWarning, podTemplateRestartPolicyReason, errMsg)
	}
	if elasticEnabled(mpiJob) {
		setRestartPolicy(podTemplate, mpiJob.Spec.MPIReplicaSpecs[kubeflow.MPIReplicaTypeLauncher])
	} else {
		podTemplate.Spec.RestartPolicy = corev1.RestartPolicyNever
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        launcherName(mpiJob),
			Namespace:   mpiJob.Namespace,
			Labels:      podTemplate.Labels,
			Annotations: podTemplate.Annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(mpiJob, kubeflow.SchemeGroupVersionKind),
			},
		},
		Spec: podTemplate.Spec,
	}
}

func setDefaultLabels(podTemplateSpec *corev1.PodTemplateSpec, jobName string, rtype kubeflow.MPIReplicaType) {
	// keep the labels which are set in PodTemplate
	if len(podTemplateSpec.Labels) == 0 {
		podTemplateSpec.Labels = make(map[string]string)
	}
	var role string
	if rtype == kubeflow.MPIReplicaTypeLauncher {
		role = "launcher"
	} else if rtype == kubeflow.MPIReplicaTypeWorker {
		role = "worker"
	} else if rtype == kubeflow.MPIReplicaTypeHorker {
		role = "horker"
	}
	for key, value := range defaultLabels(jobName, role) {
		podTemplateSpec.Labels[key] = value
	}
}

func setRestartPolicy(podTemplateSpec *corev1.PodTemplateSpec, spec *kubeflow.ReplicaSpec) {
	if spec.RestartPolicy == kubeflow.RestartPolicyExitCode {
		podTemplateSpec.Spec.RestartPolicy = corev1.RestartPolicyNever
	} else {
		podTemplateSpec.Spec.RestartPolicy = corev1.RestartPolicy(spec.RestartPolicy)
	}
}

func isCleanUpPods(cleanPodPolicy *kubeflow.CleanPodPolicy) bool {
	if *cleanPodPolicy == kubeflow.CleanPodPolicyAll || *cleanPodPolicy == kubeflow.CleanPodPolicyRunning {
		return true
	}
	return false
}

func getMainContainerStatus(pod *corev1.Pod) *corev1.ContainerStatus {
	if pod == nil || len(pod.Status.ContainerStatuses) == 0 {
		return nil
	}
	if len(pod.Status.ContainerStatuses) == 1 {
		return &pod.Status.ContainerStatuses[0]
	}
	if len(pod.Spec.Containers) > 0 {
		cname := pod.Spec.Containers[0].Name
		for i, cs := range pod.Status.ContainerStatuses {
			if cs.Name == cname {
				return &pod.Status.ContainerStatuses[i]
			}
		}
	}
	return nil
}
