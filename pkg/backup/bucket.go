package backup

import (
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
	Remove(dirName string) error
	RotateBackups(backups []*Backup) bool
	UploadFile(path, objectName string) error
	ScanBucket(serviceType ServiceType) []*Backup
	SyncMetadataState(state, objectName string) error
}

type AzureBucket struct {
	Id          string `json:"id"`
	AccountName string `json:"accountName"`
	AccountKey  string `json:"accountKey"`
	Bucket      string `json:"bucket"`
	DstDir      string `json:"dstDir"`
}

const (
	Initialized Status = "initialized"
	DumpingDB   Status = "dumpingdb"
	UploadingDB Status = "uploadingdb"
	Failed      Status = "failed"
	Finished    Status = "finished"

	Indexfile string = "indexfile.json"

	AzureBucketType BucketType = "azure"
	GcpBucket       BucketType = "gcp"
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

	err := &UnsupportedBucketError{}
	log.Error(err)
	return nil, err
}

func setDefaultDestinationDir(dstDir string) string {
	if dstDir == "" {
		return "cnvrg-smart-backups"
	}
	return dstDir
}
