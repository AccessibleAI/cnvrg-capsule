package backup

type ServiceType string

type Service interface {
	Dump() error
	DumpfileName() string
	DumpfileLocalPath() string
	ServiceType() ServiceType
	UploadBackupAssets(bucket Bucket, backupId string) error
}
