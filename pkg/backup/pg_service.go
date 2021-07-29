package backup

import (
	"bufio"
	"fmt"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	"github.com/lithammer/shortuuid/v3"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/types"
	"os/exec"
	"strings"
)

type PgCreds struct {
	Host   string `json:"host,omitempty"`
	DbName string `json:"db,omitempty"`
	User   string `json:"user,omitempty"`
	Pass   string `json:"pass,omitempty"`
}

type PgBackupService struct {
	Name       string   `json:"name"`
	Creds      PgCreds  `json:"creds"`
	DumpCmd    []string `json:"backupCmd"`
	RestoreCmd []string `json:"restoreCmd"`
	Dumpfile   string   `json:"dumpfile"`
}

func (pgs *PgBackupService) ServiceType() ServiceType {
	return PgService
}

func (pgs *PgBackupService) Dump() error {
	log.Infof("starting backup: %s", pgs.Dumpfile)
	cmdParams := append([]string{"-lc"}, strings.Join(pgs.DumpCmd, " "))
	log.Debugf("pg backup cmd: %s ", cmdParams)
	cmd := exec.Command("/bin/bash", cmdParams...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Error(err)
		return err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Error(err)
		return err
	}

	err = cmd.Start()
	if err != nil {
		log.Error(err)
		return err
	}

	stdoutScanner := bufio.NewScanner(stdout)
	for stdoutScanner.Scan() {
		m := stdoutScanner.Text()
		log.Infof("|%s| %s", pgs.Dumpfile, m)
	}

	stderrScanner := bufio.NewScanner(stderr)
	for stderrScanner.Scan() {
		m := stderrScanner.Text()
		log.Errorf("|%s| %s", pgs.Dumpfile, m)
		return err
	}

	if err := cmd.Wait(); err != nil {
		log.Error(err)
		return err
	}

	log.Infof("backup %s is finished", pgs.Dumpfile)
	return nil
}

func (pgs *PgBackupService) DumpfileLocalPath() string {
	return fmt.Sprintf("%s/%s", viper.GetString("dumpdir"), pgs.Dumpfile)
}

func (pgs *PgBackupService) DumpfileName() string {
	return pgs.Dumpfile
}

func (pgs *PgBackupService) GetName() string {
	return pgs.Name
}

func (pgs *PgBackupService) UploadBackupAssets(bucket Bucket, id string) error {
	objectName := fmt.Sprintf("%s/%s", id, pgs.Dumpfile)
	return bucket.UploadFile(pgs.DumpfileLocalPath(), objectName)
}

func (pgs *PgBackupService) DownloadBackupAssets(bucket Bucket, id string) error {
	objectName := fmt.Sprintf("%s/%s", id, pgs.Dumpfile)
	return bucket.DownloadFile(objectName, pgs.DumpfileLocalPath())
}

func (pgs *PgBackupService) Restore() error {

	log.Infof("starting restore: %s", pgs.Dumpfile)
	cmdParams := append([]string{"-lc"}, strings.Join(pgs.RestoreCmd, " "))
	log.Debugf("pg backup cmd: %s ", cmdParams)
	cmd := exec.Command("/bin/bash", cmdParams...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Error(err)
		return err
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Error(err)
		return err
	}

	err = cmd.Start()
	if err != nil {
		log.Error(err)
		return err
	}

	stdoutScanner := bufio.NewScanner(stdout)
	for stdoutScanner.Scan() {
		m := stdoutScanner.Text()
		log.Infof("|%s| %s", pgs.Dumpfile, m)
	}

	stderrScanner := bufio.NewScanner(stderr)
	for stderrScanner.Scan() {
		m := stderrScanner.Text()
		log.Errorf("|%s| %s", pgs.Dumpfile, m)
		return err
	}

	if err := cmd.Wait(); err != nil {
		log.Error(err)
		return err
	}

	log.Infof("restore %s is finished", pgs.Dumpfile)

	return nil
}

func NewPgCredsWithAutoDiscovery(ns, pgSecretName string) (*PgCreds, error) {
	n := types.NamespacedName{Namespace: ns, Name: pgSecretName}
	pgSecret := k8s.GetSecret(n)
	if err := validatePgCreds(n.Name, pgSecret.Data); err != nil {
		log.Errorf("pg creds secret invalid, err: %s", err)
		return nil, err
	}

	return &PgCreds{
		Host:   fmt.Sprintf("%s.%s", pgSecret.Data["POSTGRES_HOST"], ns),
		DbName: string(pgSecret.Data["POSTGRES_DB"]),
		User:   string(pgSecret.Data["POSTGRES_USER"]),
		Pass:   string(pgSecret.Data["POSTGRES_PASSWORD"]),
	}, nil
}

func NewPgBackupService(name string, creds PgCreds) *PgBackupService {
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
	restoreCmd := []string{
		"2>&1", // for some reason pg_dump with verbose mode outputs to stderr (wtf?)
		"pg_restore",
		fmt.Sprintf("--dbname=postgresql://%s:%s@%s:5432/%s", creds.User, creds.Pass, creds.Host, creds.DbName),
		"--clean",
		"--create",
		"--exit-on-error",
		"--format=t",
		"--verbose",
		localDumpPath,
	}
	return &PgBackupService{
		Name:       name,
		Creds:      creds,
		Dumpfile:   dumpfile,
		DumpCmd:    dumpCmd,
		RestoreCmd: restoreCmd,
	}
}
