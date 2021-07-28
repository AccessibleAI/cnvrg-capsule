package backup

import (
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

func validatePvcAnnotations(pvcName string, annotations map[string]string) error {
	if annotations == nil {
		return &RequiredKeyIsMissing{Key: "ALL_KEYS_ARE_MISSING", ObjectName: pvcName}
	}

	// make sure all required annotations are presented
	annotationsChecks := []PvcAnnotation{
		BackupEnabledAnnotation,
		ServiceTypeAnnotation,
		BucketRefAnnotation,
		CredsRefAnnotation,
		RotationRefAnnotation,
		PeriodAnnotation,
	}
	for _, annotation := range annotationsChecks {
		if _, ok := annotations[string(annotation)]; !ok {
			return &RequiredKeyIsMissing{Key: string(annotation), ObjectName: pvcName}
		}
	}

	// make sure supported service type provided
	supportedServicesTypes := []ServiceType{PgService}
	for _, backupService := range supportedServicesTypes {
		if string(backupService) == annotations[string(ServiceTypeAnnotation)] {
			return nil
		}
	}

	return &UnsupportedBackupService{}
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

func ShouldBackup(ds *DiscoveryInputs) bool {
	nsWhitelist := viper.GetString("ns-whitelist")
	if ds.BackupEnabled {
		if nsWhitelist == "*" || strings.Contains(nsWhitelist, ds.PvcNamespace) {
			log.Infof("backup enabled for: %s/%s", ds.PvcNamespace, ds.PvcName)
			return true
		}
	}
	log.Warnf("skipping, backup is not enabled (or whitelisted) for: %s/%s", ds.PvcNamespace, ds.PvcName)
	return false
}
