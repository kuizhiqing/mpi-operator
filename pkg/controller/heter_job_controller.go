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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	"k8s.io/utils/clock"

	kubeflow "github.com/kuizhiqing/resilient-training-operator/pkg/apis/kubeflow/v2beta1"
	clientset "github.com/kuizhiqing/resilient-training-operator/pkg/client/clientset/versioned"
	"github.com/kuizhiqing/resilient-training-operator/pkg/client/clientset/versioned/scheme"
	informers "github.com/kuizhiqing/resilient-training-operator/pkg/client/informers/externalversions/kubeflow/v2beta1"
	listers "github.com/kuizhiqing/resilient-training-operator/pkg/client/listers/kubeflow/v2beta1"
)

const (
	heterIPListKey = "kubeflow.org/heter-ip-list"
	heterRoleKey   = "kubeflow.org/heter-role" // launcher/worker/horker
	heterJobKey    = "kubeflow.org/heter-job"
)

var ()

// HeterJobController is the controller implementation for ResilientJob resources.
type HeterJobController struct {
	// kubeClient is a standard kubernetes clientset.
	kubeClient kubernetes.Interface
	// kubeflowClient is a clientset for our own API group.
	kubeflowClient clientset.Interface

	mpiJobLister listers.ResilientJobLister
	mpiJobSynced cache.InformerSynced

	// queue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	queue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder

	// Clock for internal use of unit-testing
	clock clock.WithTicker

	excludeNamespaces map[string]bool
	includeNamespaces map[string]bool
}

// NewHeterJobController returns a new ResilientJob controller.
func NewHeterJobController(
	kubeClient kubernetes.Interface,
	kubeflowClient clientset.Interface,
	mpiJobInformer informers.ResilientJobInformer,
	namespace, exNamespaces, inNamespaces string,
) *HeterJobController {
	return NewHeterJobControllerWithClock(
		kubeClient,
		kubeflowClient,
		mpiJobInformer,
		&clock.RealClock{},
		namespace,
		exNamespaces,
		inNamespaces)
}

// NewHeterJobControllerWithClock returns a new ResilientJob controller.
func NewHeterJobControllerWithClock(
	kubeClient kubernetes.Interface,
	kubeflowClient clientset.Interface,
	mpiJobInformer informers.ResilientJobInformer,
	clock clock.WithTicker,
	namespace, exNamespaces, inNamespaces string,
) *HeterJobController {

	// Create event broadcaster.
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	excludeNamespaces := splitStringToMapByComma(exNamespaces)
	includeNamespaces := splitStringToMapByComma(inNamespaces)

	controller := &HeterJobController{
		kubeClient:        kubeClient,
		kubeflowClient:    kubeflowClient,
		mpiJobLister:      mpiJobInformer.Lister(),
		mpiJobSynced:      mpiJobInformer.Informer().HasSynced,
		queue:             workqueue.NewNamedRateLimitingQueue(workqueue.DefaultItemBasedRateLimiter(), "ResilientJobs"),
		recorder:          recorder,
		clock:             clock,
		excludeNamespaces: excludeNamespaces,
		includeNamespaces: includeNamespaces,
	}

	klog.Info("Setting up event handlers")
	// Set up an event handler for when ResilientJob resources change.
	mpiJobInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.filterResilientJob,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: controller.addResilientJob,
			UpdateFunc: func(old, new interface{}) {
				controller.enqueueResilientJob(new)
			},
			DeleteFunc: controller.deleteResilientJob,
		},
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the work queue and wait for
// workers to finish processing their current work items.
func (c *HeterJobController) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	// Start the informer factories to begin populating the informer caches.
	klog.Info("Starting HeterJob controller")

	// Wait for the caches to be synced before starting workers.
	klog.Info("Waiting for informer caches to sync")
	synced := []cache.InformerSynced{c.mpiJobSynced}
	if ok := cache.WaitForCacheSync(stopCh, synced...); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	// Launch workers to process ResilientJob resources.
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
func (c *HeterJobController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the work queue and
// attempt to process it, by calling the syncHandler.
func (c *HeterJobController) processNextWorkItem() bool {
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
		// ResilientJob resource to be synced.
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
// converge the two. It then updates the Status block of the ResilientJob resource
// with the current status of the resource.
func (c *HeterJobController) syncHandler(key string) error {
	startTime := c.clock.Now()

	launcherJob, heterJob := c.getJobPair(key)
	if launcherJob == nil || heterJob == nil {
		return nil
	}

	// for mpi job that is terminating, just return.
	if launcherJob.DeletionTimestamp != nil {
		klog.Infof("Job %s is terminating", key)
		return nil
	}

	defer func() {
		klog.Infof("Finished syncing job %q (%v)", key, c.clock.Since(startTime))
	}()

	// get iplist from heter job status, fill in launcher job annotation
	ipList := getStatusIPList(heterJob)
	return c.updateLauncherJobAnnotation(launcherJob, ipList)
}

func (c *HeterJobController) updateLauncherJobAnnotation(launcherJob *kubeflow.ResilientJob, ipList string) error {
	if ipList != "" {
		launcherJob.Annotations[heterIPListKey] = ipList
		_, err := c.kubeflowClient.KubeflowV2beta1().ResilientJobs(launcherJob.Namespace).Update(context.TODO(), launcherJob, metav1.UpdateOptions{})
		if err != nil {
			klog.Errorf("Update job %s failed, err: %v", launcherJob.Name, err)
			return err
		}
	}

	return nil
}

func (c *HeterJobController) getJobPair(key string) (*kubeflow.ResilientJob, *kubeflow.ResilientJob) {
	mpiJob, err := c.getResilientJobByKey(key)
	if err != nil {
		return nil, nil
	}

	heterJobName := getAnnotation(mpiJob, heterJobKey)
	if heterJobName == "" {
		return nil, nil
	}
	heterJob, err := c.getResilientJobByKey(heterJobName)
	if err != nil {
		klog.Errorf("Get job %s failed", heterJobName)
		return nil, nil
	}
	// heter-job of the heterJob should be the job itself
	if tmpKey := getAnnotation(heterJob, heterJobKey); tmpKey != key {
		klog.Errorf("Job %s %s annotation do not match", key, tmpKey)
		return nil, nil
	}

	mpiJobRole := getAnnotation(mpiJob, heterRoleKey)
	heterJobRole := getAnnotation(heterJob, heterRoleKey)

	if mpiJobRole == launcher && heterJobRole != launcher {
		return mpiJob, heterJob
	} else if mpiJobRole != launcher && heterJobRole == launcher {
		return heterJob, mpiJob
	} else {
		klog.Errorf("Job %s %s %s annotation not consist", key, mpiJobRole, heterJobRole)
		return nil, nil
	}
}

func (c *HeterJobController) getResilientJobByKey(key string) (*kubeflow.ResilientJob, error) {
	// Convert the namespace/name string into a distinct namespace and name.
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil, err
	}

	// Get the ResilientJob with this namespace/name.
	sharedJob, err := c.mpiJobLister.ResilientJobs(namespace).Get(name)
	if err != nil {
		return nil, err
	}

	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance
	mpiJob := sharedJob.DeepCopy()
	// Set default for the new mpiJob.
	scheme.Scheme.Default(mpiJob)

	return mpiJob, nil
}

func getStatusIPList(mpiJob *kubeflow.ResilientJob) string {
	if mpiJob.Status.ReplicaStatuses == nil {
		return ""
	}

	var ips bytes.Buffer
	if mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeLauncher] != nil {
		ip := mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeLauncher].Selector
		if ip != "" {
			ips.WriteString(ip)
			ips.WriteString(",")
		}
	}
	if mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeWorker] != nil {
		ip := mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeWorker].Selector
		if ip != "" {
			ips.WriteString(ip)
			ips.WriteString(",")
		}
	}
	if mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeHorker] != nil {
		ip := mpiJob.Status.ReplicaStatuses[kubeflow.MPIReplicaTypeHorker].Selector
		if ip != "" {
			ips.WriteString(ip)
			ips.WriteString(",")
		}
	}
	return strings.TrimSuffix(ips.String(), ",")
}

func (c *HeterJobController) filterResilientJob(obj interface{}) bool {
	mpiJob, ok := obj.(*kubeflow.ResilientJob)
	if !ok {
		return false
	}
	return c.filterObjNamespace(mpiJob.Namespace)
}

func (c *HeterJobController) filterObjNamespace(ns string) bool {
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
func (c *HeterJobController) addResilientJob(obj interface{}) {
	mpiJob := obj.(*kubeflow.ResilientJob)

	// Set default for the new mpiJob.
	scheme.Scheme.Default(mpiJob)
	c.enqueueResilientJob(mpiJob)
}

// enqueueResilientJob takes a ResilientJob resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than ResilientJob.
func (c *HeterJobController) enqueueResilientJob(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	c.queue.AddRateLimited(key)
}

func (c *HeterJobController) deleteResilientJob(obj interface{}) {
	var mpiJob *kubeflow.ResilientJob
	var ok bool
	if mpiJob, ok = obj.(*kubeflow.ResilientJob); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		mpiJob, ok = tombstone.Obj.(*kubeflow.ResilientJob)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		klog.V(4).Infof("Recovered deleted object '%s' from tombstone", mpiJob.GetName())
	}

	key := fmt.Sprintf("%s/%s", mpiJob.Namespace, mpiJob.Name)

	klog.Infof("Processing delete resilientjob: %s", key)

	heterJobName := getAnnotation(mpiJob, heterJobKey)
	if heterJobName == "" {
		return
	}
	heterJob, err := c.getResilientJobByKey(heterJobName)
	if err != nil {
		klog.Errorf("Get job %s failed", heterJobName)
		return
	}
	// heter-job of the heterJob should be the job itself
	if tmpKey := getAnnotation(heterJob, heterJobKey); tmpKey != key {
		klog.Errorf("Job %s %s annotation do not match", key, tmpKey)
		return
	}

	if heterJob.DeletionTimestamp != nil {
		klog.Infof("Job %s is terminating", heterJobName)
		return
	}

	klog.Infof("Delete job %s by %s", heterJobName, key)
	if err = c.kubeflowClient.KubeflowV2beta1().ResilientJobs(heterJob.Namespace).Delete(context.TODO(), heterJob.Name, metav1.DeleteOptions{}); err != nil {
		klog.Errorf("Delete resilientjob %s failed", heterJobName)
		return
	}
}
