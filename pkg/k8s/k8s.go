package k8s

// https://hackernoon.com/platforms-on-k8s-with-golang-watch-any-crd-0v2o3z1q

import (
	"context"
	mlopsv1 "github.com/AccessibleAI/cnvrg-operator/api/v1"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	v1core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"path/filepath"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var k8sClient client.Client

var (
	log = logrus.WithField("module", "k8s")
)


func init() {
	k8sClient = GetClient()
}

func GetClient() client.Client {

	scheme := runtime.NewScheme()
	// register cnvrg mlops gvr
	if err := mlopsv1.AddToScheme(scheme); err != nil {
		log.Fatal(err)
	}
	// register v1core gvr
	if err := v1core.AddToScheme(scheme); err != nil {
		log.Fatal(err)
	}

	kubeconfig := ctrl.GetConfigOrDie()
	controllerClient, err := client.New(kubeconfig, client.Options{Scheme: scheme})
	if err != nil {
		log.Fatal(err)
		return nil
	}
	return controllerClient
}

func KubeconfigDefaultLocation() string {
	kubeconfigDefaultLocation := ""
	if home := homedir.HomeDir(); home != "" {
		kubeconfigDefaultLocation = filepath.Join(home, ".kube", "config")
	}
	return kubeconfigDefaultLocation
}

func clientset() *kubernetes.Clientset {
	if _, err := os.Stat(viper.GetString("kubeconfig")); os.IsNotExist(err) {
		config, err := rest.InClusterConfig()
		if err != nil {
			panic(err.Error())
		}
		clientset, err := kubernetes.NewForConfig(config)
		if err != nil {
			panic(err.Error())
		}
		return clientset
	} else if err != nil {
		log.Fatalf("%s failed to check kubeconfig location", err)
	}

	config, err := clientcmd.BuildConfigFromFlags("", viper.GetString("kubeconfig"))
	if err != nil {
		panic(err.Error())
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	return clientset
}

func GetCnvrgApps() *mlopsv1.CnvrgAppList {
	l := mlopsv1.CnvrgAppList{}
	log.Info("fetching all cnvrgapps")
	if err := k8sClient.List(context.Background(), &l); err != nil {
		log.Error("failed to list CnvrgApps err: %v,", err)
	}
	return &l
}

func GetSecret(name types.NamespacedName) *v1core.Secret {

	secret := v1core.Secret{}
	log.Infof("fetching %s secret", name)
	if err := k8sClient.Get(context.Background(), name, &secret); err != nil {
		log.Errorf("error fetching pg scert, err: %s", err)
	}
	return &secret
}
