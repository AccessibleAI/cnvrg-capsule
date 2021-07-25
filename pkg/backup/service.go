package backup

type ServiceType string

type Service interface {
	Dump() error
	DumpfileName() string
	DumpfileLocalPath() string
	ServiceType() ServiceType
	UploadBackupAssets(bucket Bucket) error
	CredsAutoDiscovery(credsRef, ns string) error
}
