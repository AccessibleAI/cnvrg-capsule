package backup

type ServiceType string

const (
	PgServiceType ServiceType = "postgresql"
)

type Service interface {
	Backup()
	ServiceType() ServiceType
	CredsAutoDiscovery(credsRef, ns string) error
}
