package backup

import (
	"fmt"
	"github.com/lithammer/shortuuid/v3"
	"github.com/spf13/viper"
)

type PgCreds struct {
	Host   string `json:"host,omitempty"`
	DbName string `json:"db,omitempty"`
	User   string `json:"user,omitempty"`
	Pass   string `json:"pass,omitempty"`
}

type PgBackupService struct {
	Creds    PgCreds  `json:"creds"`
	DumpCmd  []string `json:"backupCmd"`
	Dumpfile string   `json:"dumpfile"`
}

func (s *PgBackupService) ServiceType() ServiceType {
	return PgServiceType
}
func (s *PgBackupService) Backup() {

}

func (s *PgBackupService) CredsAutoDiscovery(credsRef, ns string) error {
	//n := types.NamespacedName{Namespace: ns, Name: credsRef}
	//pgSecret := k8s.GetSecret(n)
	//if err := validatePgCreds(n.Name, pgSecret.Data); err != nil {
	//	log.Errorf("pg creds secret invalid, err: %s", err)
	//	return err
	//}
	return nil
	//return &PgCreds{
	//	Host:   fmt.Sprintf("%s.%s", pgSecret.Data["POSTGRES_HOST"], ns),
	//	DbName: string(pgSecret.Data["POSTGRES_DB"]),
	//	User:   string(pgSecret.Data["POSTGRES_USER"]),
	//	Pass:   string(pgSecret.Data["POSTGRES_PASSWORD"]),
	//}, nil
}

func NewPgBackupService(creds PgCreds) *PgBackupService {
	dumpfile := fmt.Sprintf("%s-pgdump.tar", shortuuid.New())
	localDumpPath := fmt.Sprintf("%s/%s", viper.GetString("dumpdir"), dumpfile)
	dumpCmd := []string{
		"2>&1", // for some reason pg_dump with verbose mode outputs to stderr (wtf?)
		"pg_dump",
		fmt.Sprintf("--dbname=postgresql://%s:%s@%s:5432/%s", creds.User, creds.Pass, creds.Host, creds.DbName),
		fmt.Sprintf("--file=%s", localDumpPath),
		"--format=t",
		"--verbose",
	}
	return &PgBackupService{
		Creds:    creds,
		Dumpfile: dumpfile,
		DumpCmd:  dumpCmd,
	}
}
