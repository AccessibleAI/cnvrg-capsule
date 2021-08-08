package backup

type ServiceType string

type Service interface {
	Dump() error
	GetName() string
	DumpfileName() string
	DumpfileLocalPath() string
	Restore() error
	ServiceType() ServiceType
	UploadBackupAssets(bucket Bucket, id string) error
	DownloadBackupAssets(bucket Bucket, id string) error
	CleanupTempStorage() error
}

const (
	PgService ServiceType = "postgresql"
)
