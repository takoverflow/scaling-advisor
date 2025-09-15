package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"

	apiv1alpha1 "github.com/gardener/scaling-advisor/api/core/v1alpha1"
	mkapi "github.com/gardener/scaling-advisor/api/minkapi"
	svcapi "github.com/gardener/scaling-advisor/api/service"
	"github.com/gardener/scaling-advisor/common/nodeutil"
	"github.com/gardener/scaling-advisor/common/objutil"
	"github.com/gardener/scaling-advisor/common/podutil"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ShootCoordinate struct {
	Landscape string
	Project   string
	Shoot     string
}

var scenarioDir string
var shootCoords ShootCoordinate

// gardenerCmd represents the gardener sub-command for generating scaling scenario(s) for a gardener cluster.
var gardenerCmd = &cobra.Command{
	Use:   "gardener <scenario-dir>",
	Short: "generate scaling scenarios into <scenario-dir> for the gardener cluster manager",
	Args:  cobra.ExactArgs(1),
	// TODO: preRun, validate whether all 3 needed flags are passed!
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if shootCoords.Landscape == "" {
			return fmt.Errorf("landscape flag is required")
		}
		if shootCoords.Project == "" {
			return fmt.Errorf("project flag is required")
		}
		if shootCoords.Shoot == "" {
			return fmt.Errorf("shoot flag is required")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		scenarioDir = args[0]
		fmt.Printf("Generating scaling scenarios for shoot %s\n", constructFullyQualifiedName(shootCoords))

		ctx := context.Background()
		acc, err := createShootAccess(ctx)
		if err != nil {
			fmt.Printf("Error creating shoot access: %v\n", err)
			os.Exit(1)
		}

		// Generate cluster snapshot
		snap, err := createClusterSnapshot(ctx, acc)
		if err != nil {
			fmt.Printf("Error creating cluster snapshot: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Created cluster snapshot with %d nodes and %d pods\n", len(snap.Nodes), len(snap.Pods))

		// Generate node templates from worker configuration
		_, extensionWorkers, err := acc.GetShootWorkers(ctx)
		if err != nil {
			fmt.Printf("Error getting shoot workers: %v\n", err)
			os.Exit(1)
		}

		nodeTemplates, err := createNodeTemplate(extensionWorkers)
		if err != nil {
			fmt.Printf("Error creating node templates: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Created %d node templates\n", len(nodeTemplates))
		fmt.Printf("%#v\n", nodeTemplates)
	},
}

type ScalingScenario struct {
	constraintsPath string
	snapshotsPath   []string
	feedback        apiv1alpha1.ClusterScalingFeedback
}

func createClusterSnapshot(ctx context.Context, a *access) (svcapi.ClusterSnapshot, error) {
	var snap svcapi.ClusterSnapshot

	// Get all nodes
	nodes, err := a.ListNodes(ctx, mkapi.MatchCriteria{})
	if err != nil {
		return snap, fmt.Errorf("failed to list nodes: %w", err)
	}
	snap.Nodes = make([]svcapi.NodeInfo, 0, len(nodes))
	for _, node := range nodes {
		snap.Nodes = append(snap.Nodes, nodeutil.AsNodeInfo(node))
	}

	// Get all pods
	pods, err := a.ListPods(ctx, mkapi.MatchCriteria{})
	if err != nil {
		return snap, fmt.Errorf("failed to list pods: %w", err)
	}
	snap.Pods = make([]svcapi.PodInfo, 0, len(pods))
	for _, pod := range pods {
		snap.Pods = append(snap.Pods, podutil.AsPodInfo(pod))
	}

	// Get priority classes
	snap.PriorityClasses, err = a.ListPriorityClasses(ctx)
	if err != nil {
		return snap, fmt.Errorf("failed to list priority classes: %w", err)
	}

	// Get runtime classes
	snap.RuntimeClasses, err = a.ListRuntimeClasses(ctx)
	if err != nil {
		return snap, fmt.Errorf("failed to list runtime classes: %w", err)
	}

	return snap, nil
}

func createNodeTemplate(extensionWorkers []any) ([]apiv1alpha1.NodeTemplate, error) {
	var nodeTemplates []apiv1alpha1.NodeTemplate

	for i, worker := range extensionWorkers {
		workerObj, ok := worker.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("worker %d: invalid type %T", i, worker)
		}

		name, _, _ := unstructured.NestedString(workerObj, "metadata", "name")
		if name == "" {
			return nil, fmt.Errorf("worker %d: missing name", i)
		}

		pools, _, _ := unstructured.NestedSlice(workerObj, "spec", "pools")
		for j, poolInterface := range pools {
			pool, ok := poolInterface.(map[string]any)
			if !ok {
				continue
			}

			nT := apiv1alpha1.NodeTemplate{Name: fmt.Sprintf("%s-pool-%d", name, j)}

			if arch, found, _ := unstructured.NestedString(pool, "architecture"); found {
				nT.Architecture = arch
			}
			if machineType, found, _ := unstructured.NestedString(pool, "machineType"); found {
				nT.InstanceType = machineType
			}
			if capacity, found, _ := unstructured.NestedFieldCopy(pool, "nodeTemplate", "capacity"); found {
				if cap, ok := capacity.(map[string]any); ok {
					nT.Capacity = objutil.StringMapToResourceList(cap)
				}
			}
			if kubeReserved, found, _ := unstructured.NestedFieldNoCopy(pool, "kubeletConfig", "kubeReserved"); found {
				if kr, ok := kubeReserved.(map[string]any); ok {
					resourceList := objutil.StringMapToResourceList(kr)
					nT.KubeReserved = &resourceList
				}
			}

			nodeTemplates = append(nodeTemplates, nT)
		}
	}

	return nodeTemplates, nil
}

func createNodePool(extensionWorkers []unstructured.Unstructured) ([]apiv1alpha1.NodePool, error) {
	var nodePools []apiv1alpha1.NodePool

	for i, worker := range extensionWorkers {
		name, _, _ := unstructured.NestedString(worker.Object, "metadata", "name")
		if name == "" {
			return nil, fmt.Errorf("worker %d: missing name", i)
		}

		pools, _, _ := unstructured.NestedSlice(worker.Object, "spec", "pools")
		for j, poolInterface := range pools {
			pool, ok := poolInterface.(map[string]any)
			if !ok {
				continue
			}

			nP := apiv1alpha1.NodePool{Name: fmt.Sprintf("%s-pool-%d", name, j)}
			if region, found, _ := unstructured.NestedString(pool, "region"); found {
				nP.Region = region
			}
			nodePools = append(nodePools, nP)
		}
	}

	return nodePools, nil
}

func init() {
	genscenarioCmd.AddCommand(gardenerCmd)

	gardenerCmd.Flags().StringVarP(
		&shootCoords.Landscape,
		"landscape", "l",
		"",
		"gardener landscape name (required)",
	)
	gardenerCmd.MarkFlagRequired("landscape")

	gardenerCmd.Flags().StringVarP(
		&shootCoords.Project,
		"project", "p",
		"",
		"gardener project name (required)",
	)
	gardenerCmd.MarkFlagRequired("project")

	gardenerCmd.Flags().StringVarP(
		&shootCoords.Shoot,
		"shoot", "s",
		"",
		"gardener shoot name (required)",
	)
	gardenerCmd.MarkFlagRequired("shoot")
}

func constructFullyQualifiedName(shootCoords ShootCoordinate) string {
	return fmt.Sprintf("%s:%s:%s", shootCoords.Landscape, shootCoords.Project, shootCoords.Shoot)
}

type GardenerPlane int

const (
	DataPlane          GardenerPlane = 0
	ControlPlane       GardenerPlane = 1
	VirtualGardenPlane GardenerPlane = 2
)

type ShootAccess interface {
	GetClient(ctx context.Context, plane GardenerPlane) (client.Client, error)
	HasKubeconfigExpired() bool
	ListNodes(ctx context.Context, criteria mkapi.MatchCriteria) ([]corev1.Node, error)
	ListPods(ctx context.Context, criteria mkapi.MatchCriteria) ([]corev1.Pod, error) // TODO: when creating podInfo slice for CS, consider filtering pods having schedulingGates
	ListPriorityClasses(ctx context.Context) ([]schedulingv1.PriorityClass, error)
	ListRuntimeClasses(ctx context.Context) ([]nodev1.RuntimeClass, error)
	GetShootWorkers(ctx context.Context) ([]any, []any, error)
	ParseShoot(ctx context.Context) (runtime.Unstructured, error)
}

type access struct {
	shootCoord    ShootCoordinate
	scheme        *runtime.Scheme
	shootClient   client.Client
	controlClient client.Client
	gardenClient  client.Client
}

func createShootAccess(ctx context.Context) (*access, error) {
	clientScheme := registerSchemes()
	shootClient, err := GetClient(ctx, shootCoords, clientScheme, DataPlane)
	if err != nil {
		return nil, err
	}

	controlClient, err := GetClient(ctx, shootCoords, clientScheme, ControlPlane)
	if err != nil {
		return nil, err
	}

	gardenClient, err := GetClient(ctx, shootCoords, clientScheme, VirtualGardenPlane)
	if err != nil {
		return nil, err
	}

	return &access{
		shootCoord:    shootCoords,
		scheme:        clientScheme,
		shootClient:   shootClient,
		controlClient: controlClient,
		gardenClient:  gardenClient,
	}, nil
}

func registerSchemes() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(nodev1.AddToScheme(scheme))
	utilruntime.Must(schedulingv1.AddToScheme(scheme))
	return scheme
}

func GetClient(ctx context.Context, shootCoord ShootCoordinate, scheme *runtime.Scheme, plane GardenerPlane) (client.Client, error) {
	var targetStr string

	switch plane {
	case VirtualGardenPlane:
		targetStr = fmt.Sprintf("gardenctl target --garden %s", shootCoord.Landscape)
	case DataPlane:
		targetStr = fmt.Sprintf("gardenctl target --garden %s --project %s --shoot %s",
			shootCoord.Landscape, shootCoord.Project, shootCoord.Shoot)
	case ControlPlane:
		targetStr = fmt.Sprintf("gardenctl target --garden %s --project %s --shoot %s --control-plane",
			shootCoord.Landscape, shootCoord.Project, shootCoord.Shoot)
	default:
		return nil, fmt.Errorf("unsupported gardener plane: %d", plane)
	}

	cmdStr := fmt.Sprintf("eval $(gardenctl kubectl-env bash) && %s > /dev/null && gardenctl kubectl-env bash", targetStr)
	cmd := exec.CommandContext(ctx, "bash", "-c", cmdStr)
	cmd.Env = append(os.Environ(), "GCTL_SESSION_ID=dev")

	capturedOut, err := invokeCommand(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to execute gardenctl command: %w", err)
	}

	kubeConfigPath, err := extractKubeConfigPath(capturedOut)
	if err != nil {
		return nil, err
	}

	restCfg, err := clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create rest.Config from kubeconfig %q: %w", kubeConfigPath, err)
	}

	return client.New(restCfg, client.Options{Scheme: scheme})
}

// extractKubeConfigPath extracts the KUBECONFIG path from gardenctl output
func extractKubeConfigPath(output string) (string, error) {
	kubeConfigRe := regexp.MustCompile(`export KUBECONFIG='([^']+)'`)
	matches := kubeConfigRe.FindStringSubmatch(output)
	if len(matches) <= 1 {
		return "", fmt.Errorf("cannot extract kubeconfig path from gardenctl output: %s", output)
	}
	return matches[1], nil
}

func (a *access) ListNodes(ctx context.Context, criteria mkapi.MatchCriteria) (nodes []corev1.Node, err error) {
	var nodeList corev1.NodeList
	err = a.shootClient.List(ctx, &nodeList, &client.ListOptions{
		LabelSelector: criteria.LabelSelector,
	})
	if err != nil {
		return nil, err
	}
	if criteria.Names.Len() <= 0 {
		return nodeList.Items, nil
	}
	for _, n := range nodeList.Items {
		if criteria.Names.Has(n.Name) {
			nodes = append(nodes, n)
		}
	}
	return nodes, err
}

func (a *access) ListPods(ctx context.Context, criteria mkapi.MatchCriteria) (pods []corev1.Pod, err error) {
	var podList corev1.PodList
	err = a.shootClient.List(ctx, &podList, &client.ListOptions{
		Namespace:     criteria.Namespace,
		LabelSelector: criteria.LabelSelector,
	})
	if err != nil {
		return nil, err
	}
	if criteria.Names.Len() <= 0 {
		return podList.Items, nil
	}
	for _, p := range podList.Items {
		if criteria.Names.Has(p.Name) {
			pods = append(pods, p)
		}
	}
	return pods, err
}

func (a *access) ListPriorityClasses(ctx context.Context) ([]schedulingv1.PriorityClass, error) {
	var priorityClassList schedulingv1.PriorityClassList
	err := a.shootClient.List(ctx, &priorityClassList)
	return priorityClassList.Items, err
}

func (a *access) ListRuntimeClasses(ctx context.Context) ([]nodev1.RuntimeClass, error) {
	var runtimeClassList nodev1.RuntimeClassList
	err := a.shootClient.List(ctx, &runtimeClassList)
	return runtimeClassList.Items, err
}

func (a *access) GetShootWorkers(ctx context.Context) ([]any, []any, error) {
	// Get Shoot object from garden cluster
	shootObj := &unstructured.Unstructured{}
	shootObj.SetAPIVersion("core.gardener.cloud/v1beta1")
	shootObj.SetKind("Shoot")

	key := client.ObjectKey{
		Name:      a.shootCoord.Shoot,
		Namespace: fmt.Sprintf("garden-%s", a.shootCoord.Project),
	}

	if err := a.gardenClient.Get(ctx, key, shootObj); err != nil {
		return nil, nil, fmt.Errorf("failed to get Shoot %s/%s: %w", key.Namespace, key.Name, err)
	}

	// Extract shoot workers from spec.provider.workers
	shootWorkers, found, err := unstructured.NestedSlice(shootObj.Object, "spec", "provider", "workers")
	if err != nil {
		return nil, nil, fmt.Errorf("error accessing shoot workers field: %w", err)
	}
	if !found {
		return nil, nil, fmt.Errorf("shoot workers field not found in spec.provider.workers")
	}

	// Get Worker extension objects from control plane
	var workersList unstructured.UnstructuredList
	workersList.SetAPIVersion("extensions.gardener.cloud/v1alpha1")
	workersList.SetKind("WorkerList")

	listOpts := &client.ListOptions{
		Namespace: fmt.Sprintf("shoot--%s--%s", a.shootCoord.Project, a.shootCoord.Shoot),
	}

	if err := a.controlClient.List(ctx, &workersList, listOpts); err != nil {
		return nil, nil, fmt.Errorf("failed to list Worker extensions: %w", err)
	}

	// Convert UnstructuredList.Items to []any
	extensionWorkers := make([]any, len(workersList.Items))
	for i, item := range workersList.Items {
		extensionWorkers[i] = item.Object
	}

	// shootWorkers is already []any, just ensure it's properly typed
	shootWorkersAny := shootWorkers

	return shootWorkersAny, extensionWorkers, nil
}

// invokeCommand executes a command and returns its output
// TODO: Move to commons/toolutil.go
func invokeCommand(cmd *exec.Cmd) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	klog.V(2).Infof("Executing command: %s", cmd.String())

	if err := cmd.Run(); err != nil {
		capturedError := strings.TrimSpace(stderr.String())
		if capturedError != "" {
			return "", fmt.Errorf("command failed: %s (stderr: %s)", err, capturedError)
		}
		return "", fmt.Errorf("command failed: %w", err)
	}

	return stdout.String(), nil
}
