package backup

type ServiceType string


type Service interface {
	Backup()
	ServiceType() ServiceType
	CredsAutoDiscovery(credsRef, ns string) error
}
