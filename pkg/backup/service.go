package backup

type ServiceType string

type Service interface {
	Dump() error
	GetName() string
	DumpfileName() string
	DumpfileLocalPath() string
	ServiceType() ServiceType
	UploadBackupAssets(bucket Bucket, id string) error
	DownloadBackupAssets(bucket Bucket, id string) error
}

const (
	PgService ServiceType = "postgresql"
)
