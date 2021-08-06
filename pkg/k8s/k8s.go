package k8s

// https://hackernoon.com/platforms-on-k8s-with-golang-watch-any-crd-0v2o3z1q

import (
	"context"
	"github.com/go-logr/zapr"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	v1core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

func initZapLog() *zap.Logger {
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.EncoderConfig.TimeKey = "timestamp"
	config.DisableStacktrace = true
	config.DisableCaller = true
	config.Encoding = "console"
	logger, _ := config.Build()
	return logger
}

func GetClient() client.Client {

	if k8sClient != nil {
		return k8sClient
	}

	ctrl.SetLogger(zapr.NewLogger(initZapLog()))
	scheme := runtime.NewScheme()

	if err := v1core.AddToScheme(scheme); err != nil {
		log.Error(err)
		os.Exit(1)
	}

	kubeconfig := ctrl.GetConfigOrDie()
	controllerClient, err := client.New(kubeconfig, client.Options{Scheme: scheme})
	if err != nil {
		log.Error(err)
		os.Exit(1)
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

func GetSecret(name types.NamespacedName) *v1core.Secret {

	secret := v1core.Secret{}
	log.Infof("fetching %s secret", name)
	if err := GetClient().Get(context.Background(), name, &secret); err != nil {
		log.Errorf("error fetching pg scert, err: %s", err)
	}
	return &secret
}

func GetPvcs() *v1core.PersistentVolumeClaimList {
	pvcList := v1core.PersistentVolumeClaimList{}
	if err := GetClient().List(context.Background(), &pvcList); err != nil {
		log.Errorf("error listing pvcs, err %s", err)
	}
	return &pvcList
}
