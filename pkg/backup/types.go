package backup

import (
	"fmt"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	"github.com/lithammer/shortuuid/v3"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/types"
	"strings"
	"time"
)

type Status string

type Bucket struct {
	Id        string `json:"id"`
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
	BackupId       string    `json:"backupId"`
	Bucket         Bucket    `json:"backupBucket"`
	Status         Status    `json:"status"`
	BackupDate     time.Time `json:"backupDate"`
	PgCreds        PgCreds   `json:"pgCreds,omitempty"`
	LocalDumpPath  string    `json:"localDumpPath"`
	RemoteDumpPath string    `json:"remoteDumpPath"`
	BackupCmd      []string  `json:"backupCmd"`
	Period         int       `json:"period"`
	Rotation       int       `json:"rotation"`
}

const (
	Initialized       Status = "initialized"
	DumpingDB         Status = "dumpingdb"
	UploadingDB       Status = "uploadingdb"
	Failed            Status = "failed"
	Finished          Status = "finished"
	MarkedForRotation Status = "markedforrotation"
	IndexfileTag      string = "Indexfile"
)

func NewBackupBucket(endpoint, region, accessKey, secretKey, bucket, dstDir string) *Bucket {

	if strings.Contains(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
	}
	if strings.Contains(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
	}
	if dstDir == "" {
		dstDir = "cnvrg-smart-backups"
	}

	id := fmt.Sprintf("%s-%s-%s", endpoint, bucket, dstDir)

	return &Bucket{
		Id:        id,
		Endpoint:  endpoint,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    strings.Contains(endpoint, "https://"),
		Bucket:    bucket,
		DstDir:    dstDir,
	}
}

func NewBackupBucketWithAutoDiscovery(credsRef, ns string) (*Bucket, error) {
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

func NewPgBackup(idPrefix string, period int, rotation int, bucket Bucket, creds PgCreds) *PgBackup {
	backupId := fmt.Sprintf("%s-%s", idPrefix, shortuuid.New())
	backupTime := time.Now()
	localDumpPath := fmt.Sprintf("%s/%s.tar", viper.GetString("dumpdir"), backupId)
	backupCmd := []string{
		"2>&1", // for some reason pg_dump with verbose mode outputs to stderr (wtf?)
		"pg_dump",
		fmt.Sprintf("--dbname=postgresql://%s:%s@%s:5432/%s", creds.User, creds.Pass, creds.Host, creds.DbName),
		fmt.Sprintf("--file=%s", localDumpPath),
		"--format=t",
		"--verbose",
	}
	b := &PgBackup{
		BackupId:       backupId,
		Bucket:         bucket,
		PgCreds:        creds,
		Status:         Initialized,
		BackupDate:     backupTime,
		BackupCmd:      backupCmd,
		LocalDumpPath:  localDumpPath,
		RemoteDumpPath: fmt.Sprintf("%s/%s-pg", bucket.DstDir, backupId),
		Period:         period,
		Rotation:       rotation,
	}
	log.Debugf("new PG Backup initiated: %#v", b)
	return b
}
