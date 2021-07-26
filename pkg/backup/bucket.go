package backup

import (
	"fmt"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	"k8s.io/apimachinery/pkg/types"
)

type Status string

type BucketType string

type Bucket interface {
	Ping() error
	BucketId() string
	GetDstDir() string
	BucketType() BucketType
	Remove(backupId string) error
	DownloadFile(objectName, localFile string) error
	UploadFile(path, objectName string) error
	ScanBucket(serviceType ServiceType) []*Backup
	SyncMetadataState(state, objectName string) error
}

const (
	Initialized Status = "initialized"
	DumpingDB   Status = "dumpingdb"
	UploadingDB Status = "uploadingdb"
	Failed      Status = "failed"
	Finished    Status = "finished"

	Indexfile string = "indexfile.json"

	GcpBucket BucketType = "gcp"
)

func NewBucketWithAutoDiscovery(credsRef, ns string) (Bucket, error) {
	n := types.NamespacedName{Namespace: ns, Name: credsRef}
	bucketSecret := k8s.GetSecret(n)
	if err := validateBucketSecret(credsRef, bucketSecret.Data); err != nil {
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
			""), nil
	}

	if bucketType == AwsBucketType {
		if string(bucketSecret.Data["CNVRG_STORAGE_ACCESS_KEY"]) != "" && string(bucketSecret.Data["CNVRG_STORAGE_SECRET_KEY"]) != "" {
			return NewAwsBucket(
				string(bucketSecret.Data["CNVRG_STORAGE_REGION"]),
				string(bucketSecret.Data["CNVRG_STORAGE_ACCESS_KEY"]),
				string(bucketSecret.Data["CNVRG_STORAGE_SECRET_KEY"]),
				string(bucketSecret.Data["CNVRG_STORAGE_BUCKET"]),
				""), nil
		}

		if string(bucketSecret.Data["CNVRG_STORAGE_ACCESS_KEY"]) == "" && string(bucketSecret.Data["CNVRG_STORAGE_SECRET_KEY"]) == "" {
			return NewAwsIamBucket(
				string(bucketSecret.Data["CNVRG_STORAGE_REGION"]),
				string(bucketSecret.Data["CNVRG_STORAGE_BUCKET"]),
				""), nil
		}
	}
	if bucketType == AzureBucketType {
		return NewAzureBucket(
			string(bucketSecret.Data["CNVRG_STORAGE_AZURE_ACCOUNT_NAME"]),
			string(bucketSecret.Data["CNVRG_STORAGE_AZURE_ACCESS_KEY"]),
			string(bucketSecret.Data["CNVRG_STORAGE_AZURE_CONTAINER"]),
			"",
		), nil
	}

	err := &UnsupportedBucketError{}
	log.Error(err)
	return nil, err
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
