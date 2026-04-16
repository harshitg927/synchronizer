package incluster

import (
	"context"
	"fmt"
	"time"

	"github.com/kubescape/go-logger"
	"github.com/kubescape/go-logger/helpers"

	clouds "github.com/kubescape/k8s-interface/cloudsupport"
	"github.com/kubescape/k8s-interface/k8sinterface"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func GetApiServerGitVersionAndCloudProvider(ctx context.Context, k8sApi *k8sinterface.KubernetesApi) (string, string) {
	if k8sApi == nil {
		logger.L().Warning("no kubernetes client for api server info")
		return "", ""
	}

	cloudProvider, err := getCloudProvider(ctx, k8sApi)
	if err != nil {
		logger.L().Error("failed to set cloud provider", helpers.Error(err))
	} else {
		logger.L().Info("cloud provider", helpers.String("cloudProvider", cloudProvider))
	}

	gitVersion, err := getApiServerGitVersion(k8sApi)
	if err != nil {
		logger.L().Error("failed to get api server version", helpers.Error(err))
	} else {
		logger.L().Info("cluster api server", helpers.String("GitVersion", gitVersion))
	}

	return gitVersion, cloudProvider
}

// GetClusterUID retrieves the UID of the kube-system namespace to use as a stable cluster identifier.
func GetClusterUID(ctx context.Context, k8sApi *k8sinterface.KubernetesApi) string {
	if k8sApi == nil {
		logger.L().Ctx(ctx).Warning("no kubernetes client for ClusterUID")
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	namespace, err := k8sApi.KubernetesClient.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		logger.L().Ctx(ctx).Warning("failed to get kube-system namespace for ClusterUID", helpers.Error(err))
		return ""
	}
	clusterUID := string(namespace.UID)
	logger.L().Info("successfully retrieved ClusterUID", helpers.String("clusterUID", clusterUID))
	return clusterUID
}

func getCloudProvider(ctx context.Context, k8sApi *k8sinterface.KubernetesApi) (string, error) {
	// Fetch only a single node to extract the providerID, instead of listing all nodes.
	nodeList, err := k8sApi.KubernetesClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return "", fmt.Errorf("failed to list nodes: %w", err)
	}
	if len(nodeList.Items) == 0 {
		return "", fmt.Errorf("no nodes found in the cluster")
	}
	return clouds.GetCloudProvider(nodeList), nil
}

func getApiServerGitVersion(k8sApi *k8sinterface.KubernetesApi) (string, error) {
	serverVersion, err := k8sApi.KubernetesClient.Discovery().ServerVersion()
	if err != nil {
		return "Unknown", err
	}

	return serverVersion.GitVersion, nil
}
