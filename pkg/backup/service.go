package types

import (
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	mlopsv1 "github.com/AccessibleAI/cnvrg-operator/api/v1"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/types"
	"strings"
)

func Run() {
	log.Info("starting backup service...")
	stopChan := make(chan bool)
	backupDiscovery()
	<-stopChan
}

func backupDiscovery() {

	apps := k8s.GetCnvrgApps()
	for _, app := range apps.Items {
		if !shouldBackup(app) {
			continue // backup not required, either backup disabled or the ns is blocked for backups
		}
		pgCreds := discoverPgCreds(app.Spec.Dbs.Pg.Backup.CredsRef, app.Namespace)
		log.Info(pgCreds)
	}
}

func shouldBackup(app mlopsv1.CnvrgApp) bool {
	nsWhitelist := viper.GetString("ns-whitelist")
	if *app.Spec.Dbs.Pg.Backup.Enabled {
		if nsWhitelist == "*" || strings.Contains(nsWhitelist, app.Namespace) {
			log.Info("backup required for: %s/%s", app.Namespace, app.Name)
			return true
		}
	} else {
		log.Info("skipping, backup is not required for: %s/%s", app.Namespace, app.Name)
		return false
	}
	return false
}

func discoverPgCreds(name, ns string) *PgCreds {
	n := types.NamespacedName{Namespace: ns, Name: name}
	pgSecret := k8s.GetPgCredsData(n)
	log.Info(pgSecret)
	return nil
}
