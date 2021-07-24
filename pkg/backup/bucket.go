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
	RotateBackups(serviceType ServiceType) bool
	ScanBucket(serviceType ServiceType) []*Backup
	Remove(objectName string) error
	UploadFile(path, objectName string) error
	SyncMetadataState(state, objectName string) error
}

//type MinioBucket struct {
//	Id        string `json:"id"`
//	Endpoint  string `json:"endpoint"`
//	Region    string `json:"region"`
//	AccessKey string `json:"accessKey"`
//	SecretKey string `json:"secretKey"`
//	UseSSL    bool   `json:"useSSL"`
//	Bucket    string `json:"bucket"`
//	DstDir    string `json:"dstDir"`
//}

type AwsBucket struct {
	Id        string `json:"id"`
	Region    string `json:"region"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	Bucket    string `json:"bucket"`
	DstDir    string `json:"dstDir"`
}

type AzureBucket struct {
	Id          string `json:"id"`
	AccountName string `json:"accountName"`
	AccountKey  string `json:"accountKey"`
	Bucket      string `json:"bucket"`
	DstDir      string `json:"dstDir"`
}

//type Bucket struct {
//	Id         string     `json:"id"`
//	Endpoint   string     `json:"endpoint"`
//	Region     string     `json:"region"`
//	AccessKey  string     `json:"accessKey"`
//	SecretKey  string     `json:"secretKey"`
//	UseSSL     bool       `json:"useSSL"`
//	Bucket     string     `json:"bucket"`
//	DstDir     string     `json:"dstDir"`
//	BucketType BucketType `json:"bucketType"`
//}

const (
	Initialized Status = "initialized"
	DumpingDB   Status = "dumpingdb"
	UploadingDB Status = "uploadingdb"
	Failed      Status = "failed"
	Finished    Status = "finished"

	Indexfile string = "indexfile.json"

	MinioBucketType BucketType = "minio"
	AwsBucketType   BucketType = "aws"
	AzureBucketType BucketType = "azure"
	GcpBucket       BucketType = "gcp"
)

func NewBackupBucketWithAutoDiscovery(credsRef, ns string) (Bucket, error) {
	n := types.NamespacedName{Namespace: ns, Name: credsRef}
	bucketSecret := k8s.GetSecret(n)
	if err := validateBucketSecret(credsRef, bucketSecret.Data); err != nil {
		log.Errorf("backup bucket secret is invalid: err: %s", err.Error())
		return nil, err
	}

	if BucketType(bucketSecret.Data["CNVRG_STORAGE_TYPE"]) == MinioBucketType {
		return NewMinioBackupBucket(
			string(bucketSecret.Data["CNVRG_STORAGE_ENDPOINT"]),
			string(bucketSecret.Data["CNVRG_STORAGE_REGION"]),
			string(bucketSecret.Data["CNVRG_STORAGE_ACCESS_KEY"]),
			string(bucketSecret.Data["CNVRG_STORAGE_SECRET_KEY"]),
			string(bucketSecret.Data["CNVRG_STORAGE_BUCKET"]),
			""), nil
	}

	return nil, nil
}
