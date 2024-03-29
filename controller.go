package main

import (
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"

	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
)

const (
	controllerAgentName = "node-label-controller"
	osLinux             = "linux"
	labelLinux          = "kubermatic.io/uses-container-linux"
)

var (
	operatorReplicas = int32(1)
)

type Controller struct {
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface

	nodesLister corelisters.NodeLister
	nodesSynced cache.InformerSynced

	deploymentsLister appslisters.DeploymentLister
	deploymentsSynced cache.InformerSynced

	daemonSetsLister appslisters.DaemonSetLister
	daemonSetsSynced cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

func NewController(
	kubeclientset kubernetes.Interface,
	nodeInformer coreinformers.NodeInformer,
	deploymentInformer appsinformers.DeploymentInformer,
	daemonSetInformer appsinformers.DaemonSetInformer) *Controller {

	// Create event broadcaster
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &Controller{
		kubeclientset:     kubeclientset,
		nodesLister:       nodeInformer.Lister(),
		nodesSynced:       nodeInformer.Informer().HasSynced,
		deploymentsLister: deploymentInformer.Lister(),
		deploymentsSynced: deploymentInformer.Informer().HasSynced,
		daemonSetsLister:  daemonSetInformer.Lister(),
		daemonSetsSynced:  daemonSetInformer.Informer().HasSynced,

		workqueue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Foos"),
		recorder:  recorder,
	}

	klog.Info("Setting up event handlers")
	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueNode,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueNode(new)
		},
	})

	return controller
}

func (c *Controller) enqueueNode(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.workqueue.AddRateLimited(key)
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting %s", controllerAgentName)

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.nodesSynced, c.deploymentsSynced, c.daemonSetsSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	c.deployContainerLinuxUpdateOperatorAndAgent()

	klog.Info("Starting workers")
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
// workqueue.
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}

		if err := c.syncHandler(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

func (c *Controller) syncHandler(key string) error {
	fmt.Printf("Syncing %s\n", key)

	// Convert the namespace/name string into a distinct namespace and name
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the Node resource with this namespace/name
	node, err := c.nodesLister.Get(name)
	if err != nil {
		// The Node resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			utilruntime.HandleError(fmt.Errorf("node '%s' in work queue no longer exists", key))
			return nil
		}

		return err
	}

	if isLinux(node) {
		if err := c.setNodeLabel(node); err != nil {
			return err
		}
	}

	return nil
}

func isLinux(node *corev1.Node) bool {
	os := node.Status.NodeInfo.OperatingSystem
	return os == osLinux
}

func (c *Controller) setNodeLabel(node *corev1.Node) error {
	if isLabelSet(node.Labels) {
		fmt.Printf("Label already set\n")
		return nil
	}
	fmt.Printf("Setting label %s=true on node %s", labelLinux, node.Name)

	nodeCpy := node.DeepCopy()
	nodeCpy.Labels[labelLinux] = "true"
	var err error
	node, err = c.kubeclientset.CoreV1().Nodes().Update(nodeCpy)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Can't set the label: %s", err.Error()))
		return err
	}

	return nil
}

func isLabelSet(labels map[string]string) bool {
	v, isSet := labels[labelLinux]
	if !isSet {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false
	}
	return b
}

func (c *Controller) deployContainerLinuxUpdateOperator() error {
	d := newDeploymentForContainerLinuxUpdateOperator()
	_, err := c.deploymentsLister.Deployments(metav1.NamespaceDefault).Get(d.Name)
	if err == nil {
		_, err = c.kubeclientset.AppsV1().Deployments(metav1.NamespaceDefault).Update(d)
		return err
	}
	// If the resource doesn't exist, we'll create it
	if errors.IsNotFound(err) {
		_, err = c.kubeclientset.AppsV1().Deployments(metav1.NamespaceDefault).Create(d)
	}
	return err
}

func newDeploymentForContainerLinuxUpdateOperator() *appsv1.Deployment {
	labels := map[string]string{
		"app": "container-linux-update-operator",
	}

	// based on https://github.com/coreos/container-linux-update-operator/blob/master/examples/deploy/update-operator.yaml
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "container-linux-update-operator",
			Namespace: metav1.NamespaceDefault, //"reboot-coordinator",
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Replicas: &operatorReplicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						corev1.Container{
							Name:    "update-operator",
							Image:   "quay.io/coreos/container-linux-update-operator:v0.7.0",
							Command: []string{"/bin/update-operator"},
							Env: []corev1.EnvVar{
								corev1.EnvVar{
									Name: "POD_NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
							},
						},
					},
					Tolerations: []corev1.Toleration{
						corev1.Toleration{
							Key:      "node-role.kubernetes.io/master",
							Operator: corev1.TolerationOpExists,
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
				},
			},
		},
	}
}

func (c *Controller) deployUpdateAgentDaemonSet() error {
	ds := newUpdateAgentDaemonSet()
	_, err := c.daemonSetsLister.DaemonSets(metav1.NamespaceDefault).Get(ds.Name)
	if err == nil {
		_, err = c.kubeclientset.AppsV1().DaemonSets(metav1.NamespaceDefault).Update(ds)
		return err
	}
	// If the resource doesn't exist, we'll create it
	if errors.IsNotFound(err) {
		_, err = c.kubeclientset.AppsV1().DaemonSets(metav1.NamespaceDefault).Create(ds)
	}
	return err
}

func newUpdateAgentDaemonSet() *appsv1.DaemonSet {
	labels := map[string]string{
		"app": "container-linux-update-agent",
	}
	nodeSelector := map[string]string{
		labelLinux: "true",
	}

	maxAvailable := intstr.FromInt(1)
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "container-linux-update-agent",
			Namespace: metav1.NamespaceDefault, //"reboot-coordinator",
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
				Type: appsv1.RollingUpdateDaemonSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDaemonSet{
					MaxUnavailable: &maxAvailable,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						corev1.Container{
							Name:    "update-agent",
							Image:   "quay.io/coreos/container-linux-update-operator:v0.7.0",
							Command: []string{"/bin/update-agent"},
							VolumeMounts: []corev1.VolumeMount{
								corev1.VolumeMount{
									Name:      "var-run-dbus",
									MountPath: "/var/run/dbus",
								},
								corev1.VolumeMount{
									Name:      "etc-coreos",
									MountPath: "/etc/coreos",
								},
								corev1.VolumeMount{
									Name:      "usr-share-coreos",
									MountPath: "/usr/share/coreos",
								},
								corev1.VolumeMount{
									Name:      "etc-os-release",
									MountPath: "/etc/os-release",
								},
							},
							Env: []corev1.EnvVar{
								corev1.EnvVar{
									Name: "UPDATE_AGENT_NODE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
								},
								corev1.EnvVar{
									Name: "POD_NAMESPACE",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.namespace",
										},
									},
								},
							},
						},
					},
					NodeSelector: nodeSelector,
					Tolerations: []corev1.Toleration{
						corev1.Toleration{
							Key:      "node-role.kubernetes.io/master",
							Operator: corev1.TolerationOpExists,
							Effect:   corev1.TaintEffectNoSchedule,
						},
					},
					Volumes: []corev1.Volume{
						corev1.Volume{
							Name: "var-run-dbus",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/var/run/dbus",
								},
							},
						},
						corev1.Volume{
							Name: "etc-coreos",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/etc/coreos",
								},
							},
						},
						corev1.Volume{
							Name: "usr-share-coreos",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/usr/share/coreos",
								},
							},
						},
						corev1.Volume{
							Name: "etc-os-release",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/etc/os-release",
								},
							},
						},
					},
				},
			},
		},
	}
}

func (c *Controller) deployContainerLinuxUpdateOperatorAndAgent() {
	if err := c.deployContainerLinuxUpdateOperator(); err != nil {
		klog.Fatalf("Can't deploy ContainerLinuxUpdateOperator: %s", err.Error())
	}
	if err := c.deployUpdateAgentDaemonSet(); err != nil {
		klog.Fatalf("Can't deploy UpdateAgentDaemonSet: %s", err.Error())
	}
}
