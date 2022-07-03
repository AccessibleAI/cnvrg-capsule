package backup

import (
	"fmt"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/types"
)

type Status string

type BucketType string

type ScanBucketOptions struct {
	ServiceType      []ServiceType
	RequestType      []BackupRequestType
	StatefileVersion string
}

type Bucket interface {
	Ping() error
	BucketId() string
	GetDstDir() string
	BucketType() BucketType
	Remove(backupId string) error
	DownloadFile(objectName, localFile string) error
	UploadFile(path, objectName string) error
	ScanBucket(o *ScanBucketOptions) []*Backup
	SyncMetadataState(state, objectName string) error
}

const (
	Initialized    Status = "initialized"
	DumpingDB      Status = "dumpingdb"
	UploadingDB    Status = "uploadingdb"
	Failed         Status = "failed"
	Finished       Status = "finished"
	RestoreRequest Status = "restorerequest"
	Statefile      string = "statefile.json"
)

func (o *ScanBucketOptions) haveServiceType(serviceType ServiceType) bool {
	for _, st := range o.ServiceType {
		if st == serviceType {
			return true
		}
	}
	return false
}

func (o *ScanBucketOptions) haveRequestType(requestType BackupRequestType) bool {
	for _, rt := range o.RequestType {
		if rt == requestType {
			return true
		}
	}
	return false
}

func (o *ScanBucketOptions) matchStatefileVersion(statefileVersion string) bool {
	return o.StatefileVersion == statefileVersion
}

func (o *ScanBucketOptions) matchBackup(backup Backup) bool {
	return o.haveServiceType(backup.ServiceType) &&
		o.haveRequestType(backup.RequestType) &&
		o.matchStatefileVersion(backup.StatefileVersion)
}

func NewBucketWithAutoDiscovery(ns, bucketName string) (Bucket, error) {
	n := types.NamespacedName{Namespace: ns, Name: bucketName}
	bucketSecret := k8s.GetSecret(n)
	if err := validateBucketSecret(bucketName, bucketSecret.Data); err != nil {
		log.Errorf("backup bucket secret is invalid: err: %s", err.Error())
		return nil, err
	}

	bucketType := BucketType(bucketSecret.Data["CNVRG_STORAGE_TYPE"])

	if bucketType == MinioBucketType {
		return NewMinioBucket(
			string(bucketSecret.Data["CNVRG_STORAGE_ENDPOINT"]),
			string(bucketSecret.Data["CNVRG_STORAGE_REGION"]),
			string(bucketSecret.Data["CNVRG_STORAGE_ACCESS_KEY"]),
			string(bucketSecret.Data["CNVRG_STORAGE_SECRET_KEY"]),
			string(bucketSecret.Data["CNVRG_STORAGE_BUCKET"]),
			viper.GetString("dst-dir")), nil
	}

	if bucketType == AwsBucketType {
		if string(bucketSecret.Data["CNVRG_STORAGE_ACCESS_KEY"]) != "" && string(bucketSecret.Data["CNVRG_STORAGE_SECRET_KEY"]) != "" {
			return NewAwsBucket(
				string(bucketSecret.Data["CNVRG_STORAGE_REGION"]),
				string(bucketSecret.Data["CNVRG_STORAGE_ACCESS_KEY"]),
				string(bucketSecret.Data["CNVRG_STORAGE_SECRET_KEY"]),
				string(bucketSecret.Data["CNVRG_STORAGE_BUCKET"]),
				viper.GetString("dst-dir")), nil
		}

		if string(bucketSecret.Data["CNVRG_STORAGE_ACCESS_KEY"]) == "" && string(bucketSecret.Data["CNVRG_STORAGE_SECRET_KEY"]) == "" {
			return NewAwsIamBucket(
				string(bucketSecret.Data["CNVRG_STORAGE_REGION"]),
				string(bucketSecret.Data["CNVRG_STORAGE_BUCKET"]),
				viper.GetString("dst-dir")), nil
		}
	}
	if bucketType == AzureBucketType {
		return NewAzureBucket(
			string(bucketSecret.Data["CNVRG_STORAGE_AZURE_ACCOUNT_NAME"]),
			string(bucketSecret.Data["CNVRG_STORAGE_AZURE_ACCESS_KEY"]),
			string(bucketSecret.Data["CNVRG_STORAGE_AZURE_CONTAINER"]),
			viper.GetString("dst-dir"),
		), nil
	}
	if bucketType == GcpBucketType {
		gcpSecretRef := string(bucketSecret.Data["CNVRG_STORAGE_GCP_SECRET_REF"])
		n = types.NamespacedName{Namespace: ns, Name: gcpSecretRef}
		gcpBucketSecret := k8s.GetSecret(n)
		return NewGcpBucket(
			string(gcpBucketSecret.Data["key.json"]),
			string(bucketSecret.Data["CNVRG_STORAGE_PROJECT"]),
			string(bucketSecret.Data["CNVRG_STORAGE_BUCKET"]),
			viper.GetString("dst-dir")), nil

	}

	err := &UnsupportedBucketError{}
	log.Error(err)
	return nil, err
}

func NewPgPeriodicV1Alpha1ScanOptions() *ScanBucketOptions {
	return &ScanBucketOptions{
		ServiceType:      []ServiceType{PgService},
		RequestType:      []BackupRequestType{PeriodicBackupRequest},
		StatefileVersion: StatefileV1Alpha1,
	}
}

func NewPgAllV1Alpha1ScanOptions() *ScanBucketOptions {
	return &ScanBucketOptions{
		ServiceType:      []ServiceType{PgService},
		RequestType:      []BackupRequestType{PeriodicBackupRequest, ManualBackupRequest},
		StatefileVersion: StatefileV1Alpha1,
	}
}

func NewAllV1Alpha1ScanOptions() *ScanBucketOptions {
	return &ScanBucketOptions{
		ServiceType:      []ServiceType{PgService},
		RequestType:      []BackupRequestType{PeriodicBackupRequest, ManualBackupRequest},
		StatefileVersion: StatefileV1Alpha1,
	}
}

func getDestinationDir(dstDir string) string {
	if dstDir == "" {
		return "cnvrg-smart-backups"
	}
	return dstDir
}

func getBucketId(bucketType BucketType, endpoint, bucket string) string {
	return fmt.Sprintf("%s-%s-%s", bucketType, endpoint, bucket)
}
