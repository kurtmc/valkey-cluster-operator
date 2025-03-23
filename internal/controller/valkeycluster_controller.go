/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/kurtmc/valkey-cluster-operator/api/v1alpha1"
)

const valkeyClusterFinalizer = "cache.example.com/finalizer"

// Definitions to manage status conditions
const (
	// typeAvailableValkeyCluster represents the status of the Statefulset reconciliation
	typeAvailableValkeyCluster = "Available"
	// typeDegradedValkeyCluster represents the status used when the custom resource is deleted and the finalizer operations are yet to occur.
	typeDegradedValkeyCluster = "Degraded"
	// typeReshardingValkeyCluster represents the status used when the custom resource is in the process of resharding.
	typeReshardingValkeyCluster = "Resharding"
)

// ValkeyClusterReconciler reconciles a ValkeyCluster object
type ValkeyClusterReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=cache.example.com,resources=valkeyclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cache.example.com,resources=valkeyclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cache.example.com,resources=valkeyclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ValkeyCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.0/pkg/reconcile
func (r *ValkeyClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the ValkeyCluster instance
	// The purpose is check if the Custom Resource for the Kind ValkeyCluster
	// is applied on the cluster if not we return nil to stop the reconciliation
	valkeyCluster := &cachev1alpha1.ValkeyCluster{}
	err := r.Get(ctx, req.NamespacedName, valkeyCluster)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// If the custom resource is not found then it usually means that it was deleted or not created
			// In this way, we will stop the reconciliation
			log.Info("valkeyCluster resource not found. Ignoring since object must be deleted")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "Failed to get valkeyCluster")
		return ctrl.Result{}, err
	}

	// Let's just set the status as Unknown when no status is available
	if valkeyCluster.Status.Conditions == nil || len(valkeyCluster.Status.Conditions) == 0 {
		meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeAvailableValkeyCluster, Status: metav1.ConditionUnknown, Reason: "Reconciling", Message: "Starting reconciliation"})
		if err = r.Status().Update(ctx, valkeyCluster); err != nil {
			log.Error(err, "Failed to update ValkeyCluster status")
			return ctrl.Result{}, err
		}

		// Let's re-fetch the valkeyCluster Custom Resource after updating the status
		// so that we have the latest state of the resource on the cluster and we will avoid
		// raising the error "the object has been modified, please apply
		// your changes to the latest version and try again" which would re-trigger the reconciliation
		// if we try to update it again in the following operations
		if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
			log.Error(err, "Failed to re-fetch valkeyCluster")
			return ctrl.Result{}, err
		}
	}

	// Let's add a finalizer. Then, we can define some operations which should
	// occur before the custom resource is deleted.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/finalizers
	if !controllerutil.ContainsFinalizer(valkeyCluster, valkeyClusterFinalizer) {
		log.Info("Adding Finalizer for ValkeyCluster")
		if ok := controllerutil.AddFinalizer(valkeyCluster, valkeyClusterFinalizer); !ok {
			log.Error(err, "Failed to add finalizer into the custom resource")
			return ctrl.Result{Requeue: true}, nil
		}

		if err = r.Update(ctx, valkeyCluster); err != nil {
			log.Error(err, "Failed to update custom resource to add finalizer")
			return ctrl.Result{}, err
		}
	}

	// Check if the ValkeyCluster instance is marked to be deleted, which is
	// indicated by the deletion timestamp being set.
	isValkeyClusterMarkedToBeDeleted := valkeyCluster.GetDeletionTimestamp() != nil
	if isValkeyClusterMarkedToBeDeleted {
		if controllerutil.ContainsFinalizer(valkeyCluster, valkeyClusterFinalizer) {
			log.Info("Performing Finalizer Operations for ValkeyCluster before delete CR")

			// Let's add here a status "Downgrade" to reflect that this resource began its process to be terminated.
			meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeDegradedValkeyCluster,
				Status: metav1.ConditionUnknown, Reason: "Finalizing",
				Message: fmt.Sprintf("Performing finalizer operations for the custom resource: %s ", valkeyCluster.Name)})

			if err := r.Status().Update(ctx, valkeyCluster); err != nil {
				log.Error(err, "Failed to update ValkeyCluster status")
				return ctrl.Result{}, err
			}

			// Perform all operations required before removing the finalizer and allow
			// the Kubernetes API to remove the custom resource.
			r.doFinalizerOperationsForValkeyCluster(valkeyCluster)

			// TODO(user): If you add operations to the doFinalizerOperationsForValkeyCluster method
			// then you need to ensure that all worked fine before deleting and updating the Downgrade status
			// otherwise, you should requeue here.

			// Re-fetch the valkeyCluster Custom Resource before updating the status
			// so that we have the latest state of the resource on the cluster and we will avoid
			// raising the error "the object has been modified, please apply
			// your changes to the latest version and try again" which would re-trigger the reconciliation
			if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
				log.Error(err, "Failed to re-fetch valkeyCluster")
				return ctrl.Result{}, err
			}

			meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeDegradedValkeyCluster,
				Status: metav1.ConditionTrue, Reason: "Finalizing",
				Message: fmt.Sprintf("Finalizer operations for custom resource %s name were successfully accomplished", valkeyCluster.Name)})

			if err := r.Status().Update(ctx, valkeyCluster); err != nil {
				log.Error(err, "Failed to update ValkeyCluster status")
				return ctrl.Result{}, err
			}

			log.Info("Removing Finalizer for ValkeyCluster after successfully perform the operations")
			if ok := controllerutil.RemoveFinalizer(valkeyCluster, valkeyClusterFinalizer); !ok {
				log.Error(err, "Failed to remove finalizer for ValkeyCluster")
				return ctrl.Result{Requeue: true}, nil
			}

			if err := r.Update(ctx, valkeyCluster); err != nil {
				log.Error(err, "Failed to remove finalizer for ValkeyCluster")
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Check if the statefulset already exists, if not create a new one
	found := &appsv1.StatefulSet{}
	err = r.Get(ctx, types.NamespacedName{Name: valkeyCluster.Name, Namespace: valkeyCluster.Namespace}, found)
	if err != nil && apierrors.IsNotFound(err) {
		// Define a new statefulset
		sts, err := r.statefulsetForValkeyCluster(valkeyCluster)
		if err != nil {
			log.Error(err, "Failed to define new StatefulSet resource for ValkeyCluster")

			// The following implementation will update the status
			meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeAvailableValkeyCluster,
				Status: metav1.ConditionFalse, Reason: "Reconciling",
				Message: fmt.Sprintf("Failed to create StatefulSet for the custom resource (%s): (%s)", valkeyCluster.Name, err)})

			if err := r.Status().Update(ctx, valkeyCluster); err != nil {
				log.Error(err, "Failed to update ValkeyCluster status")
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, err
		}

		log.Info("Creating a new StatefulSet",
			"StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
		if err = r.Create(ctx, sts); err != nil {
			log.Error(err, "Failed to create new StatefulSet",
				"StatefulSet.Namespace", sts.Namespace, "StatefulSet.Name", sts.Name)
			return ctrl.Result{}, err
		}

		// StatefulSet created successfully
		// We will requeue the reconciliation so that we can ensure the state
		// and move forward for the next operations
		return ctrl.Result{RequeueAfter: time.Minute}, nil
	} else if err != nil {
		log.Error(err, "Failed to get StatefulSet")
		// Let's return the error for the reconciliation be re-trigged again
		return ctrl.Result{}, err
	}

	// The CRD API defines that the ValkeyCluster type have a ValkeyClusterSpec.Size field
	// to set the quantity of StatefulSet instances to the desired state on the cluster.
	// Therefore, the following code will ensure the StatefulSet size is the same as defined
	// via the Size spec of the Custom Resource which we are reconciling.
	size := statefulSetSizeForValkeyCluster(valkeyCluster)
	if *found.Spec.Replicas != size {
		found.Spec.Replicas = &size
		if err = r.Update(ctx, found); err != nil {
			log.Error(err, "Failed to update StatefulSet",
				"StatefulSet.Namespace", found.Namespace, "StatefulSet.Name", found.Name)

			// Re-fetch the valkeyCluster Custom Resource before updating the status
			// so that we have the latest state of the resource on the cluster and we will avoid
			// raising the error "the object has been modified, please apply
			// your changes to the latest version and try again" which would re-trigger the reconciliation
			if err := r.Get(ctx, req.NamespacedName, valkeyCluster); err != nil {
				log.Error(err, "Failed to re-fetch valkeyCluster")
				return ctrl.Result{}, err
			}

			// The following implementation will update the status
			meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeAvailableValkeyCluster,
				Status: metav1.ConditionFalse, Reason: "Resizing",
				Message: fmt.Sprintf("Failed to update the size for the custom resource (%s): (%s)", valkeyCluster.Name, err)})

			if err := r.Status().Update(ctx, valkeyCluster); err != nil {
				log.Error(err, "Failed to update ValkeyCluster status")
				return ctrl.Result{}, err
			}

			return ctrl.Result{}, err
		}

		// Now, that we update the size we want to requeue the reconciliation
		// so that we can ensure that we have the latest state of the resource before
		// update. Also, it will help ensure the desired state on the cluster
		return ctrl.Result{Requeue: true}, nil
	}

	// Check the status of the valkey cluster
	clusterNodes := []ClusterNode{}

	// get all the pods in the statefulset
	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(valkeyCluster.Namespace),
		client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name)),
	}
	if err = r.List(ctx, podList, listOpts...); err != nil {
		log.Error(err, "Failed to list pods", "ValkeyCluster.Namespace", valkeyCluster.Namespace, "ValkeyCluster.Name", valkeyCluster.Name)
		return ctrl.Result{}, err
	}
	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodRunning {
			log.Info("Pod not running", "Pod.Name", pod.Name, "Pod.Status", pod.Status.Phase)
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}

		log.Info("Pod running", "Pod.Name", pod.Name, "Pod.Status", pod.Status.Phase)

		isPodReady := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
				isPodReady = true
			}
		}
		if isPodReady {
			client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{pod.Status.PodIP + ":6379"}, ForceSingleClient: true})
			if err != nil {
				log.Error(err, "Failed to create Valkey client")
				return ctrl.Result{}, err
			}
			defer client.Close()
			clusterNodesTxt, err := client.Do(ctx, client.B().ClusterNodes().Build()).ToString()
			if err != nil {
				log.Error(err, "Failed to get cluster nodes")
				return ctrl.Result{}, err
			}

			clusterNodes = append(clusterNodes, parseClusterNode(clusterNodesTxt))
		} else {
			log.Info("Pod not ready", "Pod.Name", pod.Name)
			return ctrl.Result{RequeueAfter: time.Minute}, nil
		}
	}

	log.Info("Cluster nodes", "ClusterNodes", clusterNodes)

	// The following implementation will update the status
	meta.SetStatusCondition(&valkeyCluster.Status.Conditions, metav1.Condition{Type: typeAvailableValkeyCluster,
		Status: metav1.ConditionTrue, Reason: "Reconciling",
		Message: fmt.Sprintf("StatefulSet for custom resource (%s) with %d replicas created successfully", valkeyCluster.Name, size)})

	if err := r.Status().Update(ctx, valkeyCluster); err != nil {
		log.Error(err, "Failed to update ValkeyCluster status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ValkeyClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&cachev1alpha1.ValkeyCluster{}).
		Owns(&appsv1.StatefulSet{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 2}).
		Complete(r)
}

// finalizeValkeyCluster will perform the required operations before delete the CR.
func (r *ValkeyClusterReconciler) doFinalizerOperationsForValkeyCluster(cr *cachev1alpha1.ValkeyCluster) {
	// TODO(user): Add the cleanup steps that the operator
	// needs to do before the CR can be deleted. Examples
	// of finalizers include performing backups and deleting
	// resources that are not owned by this CR, like a PVC.

	// Note: It is not recommended to use finalizers with the purpose of deleting resources which are
	// created and managed in the reconciliation. These ones, such as the StatefulSet created on this reconcile,
	// are defined as dependent of the custom resource. See that we use the method ctrl.SetControllerReference.
	// to set the ownerRef which means that the StatefulSet will be deleted by the Kubernetes API.
	// More info: https://kubernetes.io/docs/tasks/administer-cluster/use-cascading-deletion/

	// The following implementation will raise an event
	r.Recorder.Event(cr, "Warning", "Deleting",
		fmt.Sprintf("Custom Resource %s is being deleted from the namespace %s",
			cr.Name,
			cr.Namespace))
}

func statefulSetSizeForValkeyCluster(valkeyCluster *cachev1alpha1.ValkeyCluster) int32 {
	return valkeyCluster.Spec.Shards + valkeyCluster.Spec.Shards*valkeyCluster.Spec.Replicas
}

// statefulsetForValkeyCluster returns a ValkeyCluster StatefulSet object
func (r *ValkeyClusterReconciler) statefulsetForValkeyCluster(valkeyCluster *cachev1alpha1.ValkeyCluster) (*appsv1.StatefulSet, error) {
	ls := labelsForValkeyCluster(valkeyCluster.Name)
	size := statefulSetSizeForValkeyCluster(valkeyCluster)

	// Get the Operand image
	image, err := imageForValkeyCluster()
	if err != nil {
		return nil, err
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      valkeyCluster.Name,
			Namespace: valkeyCluster.Namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &size,
			Selector: &metav1.LabelSelector{
				MatchLabels: ls,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: ls,
				},
				Spec: corev1.PodSpec{
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: &[]bool{true}[0],
						// IMPORTANT: seccomProfile was introduced with Kubernetes 1.19
						// If you are looking for to produce solutions to be supported
						// on lower versions you must remove this option.
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Image:           image,
						Name:            "valkey-cluster-node",
						ImagePullPolicy: corev1.PullIfNotPresent,
						// Ensure restrictive context for the container
						// More info: https://kubernetes.io/docs/concepts/security/pod-security-standards/#restricted
						SecurityContext: &corev1.SecurityContext{
							// WARNING: Ensure that the image used defines an UserID in the Dockerfile
							// otherwise the Pod will not run and will fail with "container has runAsNonRoot and image has non-numeric user"".
							// If you want your workloads admitted in namespaces enforced with the restricted mode in OpenShift/OKD vendors
							// then, you MUST ensure that the Dockerfile defines a User ID OR you MUST leave the "RunAsNonRoot" and
							// "RunAsUser" fields empty.
							RunAsNonRoot: &[]bool{true}[0],
							// The valkeyCluster image does not use a non-zero numeric user as the default user.
							// Due to RunAsNonRoot field being set to true, we need to force the user in the
							// container to a non-zero numeric user. We do this using the RunAsUser field.
							// However, if you are looking to provide solution for K8s vendors like OpenShift
							// be aware that you cannot run under its restricted-v2 SCC if you set this value.
							RunAsUser:                &[]int64{1001}[0],
							AllowPrivilegeEscalation: &[]bool{false}[0],
							Capabilities: &corev1.Capabilities{
								Drop: []corev1.Capability{
									"ALL",
								},
							},
						},
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: 6379,
								Name:          "valkey-tcp",
							},
							{
								ContainerPort: 16379,
								Name:          "valkey-bus",
							},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.FromInt(6379),
								},
							},
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{
									Port: intstr.FromInt(6379),
								},
							},
						},
						Env: []corev1.EnvVar{
							{
								Name: "POD_IP",
								ValueFrom: &corev1.EnvVarSource{
									FieldRef: &corev1.ObjectFieldSelector{
										FieldPath: "status.podIP",
									},
								},
							},
						},
						WorkingDir: "/data",
						Command:    []string{"sh", "-c", `echo -e "port 6379\ncluster-enabled yes\ncluster-config-file nodes.conf\ncluster-node-timeout 5000\nappendonly yes\nprotected-mode no" > valkey.conf; valkey-server ./valkey.conf --cluster-announce-ip $POD_IP`},
						VolumeMounts: []corev1.VolumeMount{
							{
								Name:      "valkey-data",
								MountPath: "/data",
							},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "valkey-data",
					Labels: ls,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName: &[]string{"local-path"}[0],
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("50Mi"),
						},
					},
				},
			}},
		},
	}

	// Set the ownerRef for the StatefulSet
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/
	if err := ctrl.SetControllerReference(valkeyCluster, sts, r.Scheme); err != nil {
		return nil, err
	}
	return sts, nil
}

// labelsForValkeyCluster returns the labels for selecting the resources
// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
func labelsForValkeyCluster(name string) map[string]string {
	var imageTag string
	image, err := imageForValkeyCluster()
	if err == nil {
		imageTag = strings.Split(image, ":")[1]
	}
	return map[string]string{
		"cache/name":                   name,
		"app.kubernetes.io/name":       "valkeyCluster-operator",
		"app.kubernetes.io/version":    imageTag,
		"app.kubernetes.io/managed-by": "ValkeyClusterController",
	}
}

// imageForValkeyCluster gets the Operand image which is managed by this controller
// from the VALKEYCLUSTER_IMAGE environment variable defined in the config/manager/manager.yaml
func imageForValkeyCluster() (string, error) {
	var imageEnvVar = "VALKEYCLUSTER_IMAGE"
	image, found := os.LookupEnv(imageEnvVar)
	if !found {
		return "ghcr.io/hyperspike/valkey:8.0.2", nil
	}
	return image, nil
}

type ClusterNode struct {
	IP           string
	ID           string
	MasterNodeID string
	Flags        []string
	SlotRange    string
}

func parseClusterNode(clusterNodesTxt string) ClusterNode {
	for _, line := range strings.Split(clusterNodesTxt, "\n") {
		if strings.Contains(line, "myself") {
			strings.Fields(line)
			fields := strings.Fields(line)
			flagsWithoutMyself := []string{}
			flags := strings.Split(fields[2], ",")
			for _, flag := range flags {
				if flag != "myself" {
					flagsWithoutMyself = append(flagsWithoutMyself, flag)
				}
			}
			slotRange := ""
			if len(fields) > 8 {
				slotRange = fields[8]
			}
			IP := strings.Split(fields[1], ":")[0]
			ID := strings.ReplaceAll(fields[0], "txt:", "")
			MasterNodeID := fields[3]
			if MasterNodeID == "-" {
				MasterNodeID = ""
			}
			return ClusterNode{
				IP:           IP,
				ID:           ID,
				MasterNodeID: MasterNodeID,
				Flags:        flagsWithoutMyself,
				SlotRange:    slotRange,
			}
		}
	}
	return ClusterNode{}
}
