package backup

import (
	mlopsv1 "github.com/AccessibleAI/cnvrg-operator/api/v1"
	"github.com/spf13/viper"
	"strings"
)

func validateBucketSecret(secretName string, data map[string][]byte) error {
	if data == nil {
		return &RequiredKeyIsMissing{Key: "ALL_KEYS_ARE_MISSING", ObjectName: secretName}
	}

	if _, ok := data["CNVRG_STORAGE_ENDPOINT"]; !ok {
		return &RequiredKeyIsMissing{Key: "CNVRG_STORAGE_ENDPOINT", ObjectName: secretName}
	}

	if _, ok := data["CNVRG_STORAGE_BUCKET"]; !ok {
		return &RequiredKeyIsMissing{Key: "CNVRG_STORAGE_ENDPOINT", ObjectName: secretName}
	}

	if _, ok := data["CNVRG_STORAGE_ACCESS_KEY"]; !ok {
		return &RequiredKeyIsMissing{Key: "CNVRG_STORAGE_ENDPOINT", ObjectName: secretName}
	}

	if _, ok := data["CNVRG_STORAGE_SECRET_KEY"]; !ok {
		return &RequiredKeyIsMissing{Key: "CNVRG_STORAGE_ENDPOINT", ObjectName: secretName}
	}

	if _, ok := data["CNVRG_STORAGE_REGION"]; !ok {
		return &RequiredKeyIsMissing{Key: "CNVRG_STORAGE_ENDPOINT", ObjectName: secretName}
	}

	return nil
}

func validatePgCreds(secretName string, data map[string][]byte) error {
	if data == nil {
		return &RequiredKeyIsMissing{Key: "ALL_KEYS_ARE_MISSING", ObjectName: secretName}
	}
	if _, ok := data["POSTGRES_HOST"]; !ok {
		return &RequiredKeyIsMissing{Key: "POSTGRES_HOST", ObjectName: secretName}
	}
	if _, ok := data["POSTGRES_DB"]; !ok {
		return &RequiredKeyIsMissing{Key: "POSTGRES_DB", ObjectName: secretName}
	}
	if _, ok := data["POSTGRES_USER"]; !ok {
		return &RequiredKeyIsMissing{Key: "POSTGRES_USER", ObjectName: secretName}
	}
	if _, ok := data["POSTGRES_PASSWORD"]; !ok {
		return &RequiredKeyIsMissing{Key: "POSTGRES_PASSWORD", ObjectName: secretName}
	}
	return nil
}

func ShouldBackup(app mlopsv1.CnvrgApp) bool {
	nsWhitelist := viper.GetString("ns-whitelist")
	if *app.Spec.Dbs.Pg.Backup.Enabled {
		if nsWhitelist == "*" || strings.Contains(nsWhitelist, app.Namespace) {
			log.Infof("backup enabled for: %s/%s", app.Namespace, app.Name)
			return true
		}
	}
	log.Warnf("skipping, backup is not enabled (or whitelisted) for: %s/%s", app.Namespace, app.Name)
	return false
}