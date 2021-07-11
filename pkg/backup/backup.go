package types

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	mlopsv1 "github.com/AccessibleAI/cnvrg-operator/api/v1"
	"github.com/lithammer/shortuuid/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/types"
	"os/exec"
	"strings"
	"time"
)

type BackupStatus string

type BackupBucket struct {
	Endpoint  string `json:"endpoint"`
	Region    string `json:"region"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	UseSSL    bool   `json:"useSSL"`
	Bucket    string `json:"bucket"`
	DstDir    string `json:"dstDir"`
}

type PgCreds struct {
	Host   string `json:"host,omitempty"`
	DbName string `json:"db,omitempty"`
	User   string `json:"user,omitempty"`
	Pass   string `json:"pass,omitempty"`
}

type RedisCreds struct {
	Host string `json:"host,omitempty"`
	User string `json:"user,omitempty"`
	Pass string `json:"pass,omitempty"`
}

type PgBackup struct {
	BackupId     string       `json:"backupId"`
	BackupBucket BackupBucket `json:"backupBucket"`
	BackupStatus BackupStatus `json:"status"`
	BackupDate   time.Time    `json:"backupDate"`
	Done         bool         `json:"done"`
	PgCreds      PgCreds      `json:"pgCreds,omitempty"`
	BackupCmd    []string     `json:"backupCmd"`
}

const (
	BackupInitializing BackupStatus = "backup is initializing"
	BackupRunning      BackupStatus = "backup is running"
	BackupFailed       BackupStatus = "backup is failed"
	BackupFinished     BackupStatus = "backup is successfully finished"
)

var log = logrus.WithField("module", "backup-engine")

func NewBackupBucket(endpoint, region, accessKey, secretKey, bucket, dstDir string) *BackupBucket {

	if strings.Contains(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
	}
	if strings.Contains(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
	}
	if dstDir == "" {
		dstDir = "cnvrg-smart-backups"
	}

	return &BackupBucket{
		Endpoint:  endpoint,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    strings.Contains(endpoint, "https://"),
		Bucket:    bucket,
		DstDir:    dstDir,
	}
}

func NewBackupBucketWithAutoDiscovery(credsRef, ns string) (*BackupBucket, error) {
	n := types.NamespacedName{Namespace: ns, Name: credsRef}
	bucketSecret := k8s.GetSecret(n)
	if err := validateBucketSecret(credsRef, bucketSecret.Data); err != nil {
		log.Errorf("backup bucket secret is invalid: err: %s", err.Error())
		return nil, err
	}

	backupBucket := NewBackupBucket(
		string(bucketSecret.Data["CNVRG_STORAGE_ENDPOINT"]),
		string(bucketSecret.Data["CNVRG_STORAGE_REGION"]),
		string(bucketSecret.Data["CNVRG_STORAGE_ACCESS_KEY"]),
		string(bucketSecret.Data["CNVRG_STORAGE_SECRET_KEY"]),
		string(bucketSecret.Data["CNVRG_STORAGE_BUCKET"]),
		"",
	)
	return backupBucket, nil
}

func NewPgCredsWithAutoDiscovery(credsRef, ns string) (*PgCreds, error) {
	n := types.NamespacedName{Namespace: ns, Name: credsRef}
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

func NewPgBackup(idPrefix string, bucket BackupBucket, creds PgCreds) *PgBackup {
	backupId := fmt.Sprintf("%s-%s", idPrefix, shortuuid.New())
	backupTime := time.Now()
	backupCmd := []string{
		"pg_dump",
		fmt.Sprintf("--dbname=postgresql://%s:%s@%s:5432/%s", creds.User, creds.Pass, creds.Host, creds.DbName),
		"--file=cnvrg-app-cnvrg-QEP3bkBooAFTuH8sEQQusn.tar",
		"--format=t",
		"--verbose",
	}
	b := &PgBackup{
		BackupId:     backupId,
		BackupBucket: bucket,
		PgCreds:      creds,
		BackupStatus: BackupInitializing,
		BackupDate:   backupTime,
		Done:         false,
		BackupCmd:    backupCmd,
	}
	log.Debugf("new PG Backup initiated: %#v", b)
	log.Infof("new PG backup initiated: %s", b.BackupId)
	return b
}

func Run() {
	log.Info("starting backup service...")
	pgBackupsChan := make(chan *PgBackup, viper.GetInt("pg-backup-queue-depth"))
	go RunDiscovery(pgBackupsChan)
	go RunPgBackups(pgBackupsChan)
}

func RunDiscovery(pgBackupsChan chan<- *PgBackup) {

	stopChan := make(chan bool)
	go discoverAndTriggerPgBackups(pgBackupsChan)
	<-stopChan
}

func RunPgBackups(pgBackupsChan <-chan *PgBackup) {

	for backup := range pgBackupsChan {
		log.Infof("received new backup request:  %s", backup.BackupId)
		backup.backup()
	}
}

func discoverAndTriggerPgBackups(pgBackupsChan chan<- *PgBackup) {

	// if auto-discovery is true
	if viper.GetBool("auto-discovery") {
		// get all cnvrg apps
		apps := k8s.GetCnvrgApps()
		for _, app := range apps.Items {
			// make sure backups enabled
			if !shouldBackup(app) {
				continue // backup not required, either backup disabled or the ns is blocked for backups
			}
			// discover pg creds
			pgCreds, err := NewPgCredsWithAutoDiscovery(app.Spec.Dbs.Pg.Backup.CredsRef, app.Namespace)
			if err != nil {
				return
			}
			// discover destination bucket
			bucket, err := NewBackupBucketWithAutoDiscovery(app.Spec.Dbs.Pg.Backup.BucketRef, app.Namespace)
			if err != nil {
				return
			}
			// send backup execution
			pgBackupsChan <- NewPgBackup(fmt.Sprintf("%s-%s", app.Name, app.Namespace), *bucket, *pgCreds)
		}
	}
}

func shouldBackup(app mlopsv1.CnvrgApp) bool {
	nsWhitelist := viper.GetString("ns-whitelist")
	if *app.Spec.Dbs.Pg.Backup.Enabled {
		if nsWhitelist == "*" || strings.Contains(nsWhitelist, app.Namespace) {
			log.Infof("backup required for: %s/%s", app.Namespace, app.Name)
			return true
		}
	} else {
		log.Info("skipping, backup is not required for: %s/%s", app.Namespace, app.Name)
		return false
	}
	return false
}

func validateBucketSecret(secretName string, data map[string][]byte) error {
	if data == nil {
		return &RequiredKeyIsMissing{Key: "ALL_KEYS_ARE_MISSING", ObjectName: secretName}
	}

	if _, ok := data["CNVRG_STORAGE_ENDPOINT"]; !ok {
		return &RequiredKeyIsMissing{Key: "CNVRG_STORAGE_ENDPOINT", ObjectName: secretName}
	}

	if _, ok := data["CNVRG_STORAGE_BUCKET"]; !ok {
		return &RequiredKeyIsMissing{Key: "CNVRG_STORAGE_ENDPOINT", ObjectName: secretName}
	}

	if _, ok := data["CNVRG_STORAGE_ACCESS_KEY"]; !ok {
		return &RequiredKeyIsMissing{Key: "CNVRG_STORAGE_ENDPOINT", ObjectName: secretName}
	}

	if _, ok := data["CNVRG_STORAGE_SECRET_KEY"]; !ok {
		return &RequiredKeyIsMissing{Key: "CNVRG_STORAGE_ENDPOINT", ObjectName: secretName}
	}

	if _, ok := data["CNVRG_STORAGE_REGION"]; !ok {
		return &RequiredKeyIsMissing{Key: "CNVRG_STORAGE_ENDPOINT", ObjectName: secretName}
	}

	return nil
}

func validatePgCreds(secretName string, data map[string][]byte) error {
	if data == nil {
		return &RequiredKeyIsMissing{Key: "ALL_KEYS_ARE_MISSING", ObjectName: secretName}
	}
	if _, ok := data["POSTGRES_HOST"]; !ok {
		return &RequiredKeyIsMissing{Key: "POSTGRES_HOST", ObjectName: secretName}
	}
	if _, ok := data["POSTGRES_DB"]; !ok {
		return &RequiredKeyIsMissing{Key: "POSTGRES_DB", ObjectName: secretName}
	}
	if _, ok := data["POSTGRES_USER"]; !ok {
		return &RequiredKeyIsMissing{Key: "POSTGRES_USER", ObjectName: secretName}
	}
	if _, ok := data["POSTGRES_PASSWORD"]; !ok {
		return &RequiredKeyIsMissing{Key: "POSTGRES_PASSWORD", ObjectName: secretName}
	}
	return nil
}

func (pb *PgBackup) jsonify() (string, error) {
	jsonStr, err := json.Marshal(pb)
	if err != nil {
		log.Errorf("can't marshal struct, err: %v", err)
		return "", nil
	}
	return string(jsonStr), nil
}

//
//func ValidateBackupType(backupType BackupType) bool {
//	if backupType == PostgreSQL {
//		return true
//	}
//	if backupType == Redis {
//		return true
//	}
//	return false
//}
//

func (bb *BackupBucket) getMinioClient() *minio.Client {

	connOptions := &minio.Options{Creds: credentials.NewStaticV4(bb.AccessKey, bb.SecretKey, ""), Secure: bb.UseSSL}
	mc, err := minio.New(bb.Endpoint, connOptions)
	if err != nil {
		log.Fatal(err)
	}
	return mc
}

func (pb *PgBackup) backup() {
	cmdParams := append([]string{"-lc"}, strings.Join(pb.BackupCmd, " "))
	log.Debugf("pg backup cmd: %s ", cmdParams)
	cmd := exec.Command("/bin/bash", cmdParams...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Error(err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Error(err)
	}

	err = cmd.Start()
	if err != nil {
		log.Error(err)
	}

	stdoutScanner := bufio.NewScanner(stdout)
	for stdoutScanner.Scan() {
		m := stdoutScanner.Text()
		log.Info(m)
	}

	stderrScanner := bufio.NewScanner(stderr)
	for stderrScanner.Scan() {
		m := stderrScanner.Text()
		log.Error(m)
	}

	if err := cmd.Wait(); err != nil {
		log.Error(err)
	}

}

//
//func (b *Backup) CreateBackupRequest() error {
//	_, err := ensureBackupBucketExists()
//	if err != nil {
//		return err
//	}
//	jsonStr, err := b.jsonify()
//	if err != nil {
//		return err
//	}
//	f := strings.NewReader(jsonStr)
//	backupBucket := viper.GetString("backup-bucket")
//	objectName := fmt.Sprintf("%s/%d-%d-%d-%d-%d-%d-%s.%s/%s.json", b.Dir, b.BackupDate.Day(), int(b.BackupDate.Month()), b.BackupDate.Year(), b.BackupDate.Hour(), b.BackupDate.Minute(), b.BackupDate.Second(), b.BackupId, b.BackupType, b.BackupId)
//	uploadInfo, err := getMinioClient().PutObject(context.Background(), backupBucket, objectName, f, f.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
//	if err != nil {
//		log.Errorf("error during putting objcet ot S3, %s", err)
//		return err
//	}
//	log.Infof("successfully uploaded: %v", uploadInfo)
//	return nil
//}
//
//func ensureBackupBucketExists() (exists bool, err error) {
//	backupBucket := viper.GetString("backup-bucket")
//	exists, err = getMinioClient().BucketExists(context.Background(), viper.GetString("backup-bucket"))
//	if err != nil {
//		log.Errorf("can't check if %s exists, err: %s", backupBucket, err)
//	}
//	if exists {
//		log.Infof("backup bucket %s exists", backupBucket)
//	} else {
//		log.Errorf("backup bucket %s does not exists", backupBucket)
//		return false, &BucketDoesNotExists{BucketName: backupBucket, Message: "bucket does not exists"}
//	}
//	return
//}
//

//
//func (pgCreds *PgCreds) autoDiscoverPgCreds() {
//
//}
