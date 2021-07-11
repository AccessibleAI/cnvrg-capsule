package types

import (
	"fmt"
	"github.com/lithammer/shortuuid/v3"
	"github.com/sirupsen/logrus"
	"time"
)

type BackupType string
type BackupStatus string

const (
	PostgreSQL BackupType = "pg"
	Redis      BackupType = "redis"
)

const (
	BackupInitializing BackupStatus = "backup is initializing"
	BackupRunning      BackupStatus = "backup is running"
	BackupFailed       BackupStatus = "backup is failed"
	BackupFinished     BackupStatus = "backup is successfully finished"
)

type BackupBucket struct {
	Endpoint  string `json:"endpoint"`
	Region    string `json:"region"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	Tls       bool   `json:"tls"`
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
}

//type Backup struct {
//	BackupId     string       `json:"backupId"`
//	BackupType   BackupType   `json:"backupType"`
//	BackupStatus BackupStatus `json:"status"`
//	BackupDate   time.Time    `json:"backupDate"`
//	Dir          string       `json:"dir"`
//	Done         bool         `json:"done"`
//	PgCreds      PgCreds      `json:"pgCreds,omitempty"`
//	RedisCreds   RedisCreds   `json:"redisCreds,omitempty"`
//}

var (
	log = logrus.WithField("module", "backup-engine")
)


func NewPgBackup(bucket BackupBucket, creds PgCreds) *PgBackup {
	backupId := fmt.Sprintf("%s", shortuuid.New())
	backupTime := time.Now()
	b := &PgBackup{
		BackupId:     backupId,
		BackupBucket: bucket,
		PgCreds:      creds,
		BackupStatus: BackupInitializing,
		BackupDate:   backupTime,
		Done:         false,
	}
	log.Infof("new PG backup initiated: %v", *b)
	return b
}

//func NewPgBackup(backupType BackupType, creds PgCreds) *Backup {
//	backupTime := time.Now()
//	b := &Backup{
//		BackupId:     fmt.Sprintf("%s", shortuuid.New()),
//		BackupType:   backupType,
//		BackupStatus: BackupInitializing,
//		BackupDate:   backupTime,
//		Dir:          strings.TrimSuffix(viper.GetString("dst-dir"), "/"),
//		Done:         false,
//		PgCreds:      creds,
//	}
//	log.Infof("new bucakup initiated: %v", *b)
//	return b
//}

//func NewPgCreds(host, dbName, user, pass string) *PgCreds {
//	return &PgCreds{
//		Host:   host,
//		DbName: dbName,
//		User:   user,
//		Pass:   pass,
//	}
//}
//
//func NewPgCredsWithAutoDiscovery() *PgCreds {
//	k8s.GetCnvrgApps()
//	return &PgCreds{}
//}
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
//func getMinioClient() *minio.Client {
//
//	endpoint := viper.GetString("endpoint")
//	accessKeyID := viper.GetString("access-key")
//	secretAccessKey := viper.GetString("secret-key")
//	useSSL := viper.GetBool("s3-tls")
//	mc, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKeyID, secretAccessKey, ""), Secure: useSSL})
//	if err != nil {
//		log.Fatal(err)
//	}
//	return mc
//}
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
//		return false, &BucketDoeNotExists{BucketName: backupBucket, Message: "bucket does not exists"}
//	}
//	return
//}
//
//func (b *Backup) jsonify() (string, error) {
//	jsonStr, err := json.Marshal(b)
//	if err != nil {
//		log.Errorf("can't marshal struct, err: %v", err)
//		return "", nil
//	}
//	return string(jsonStr), nil
//
//}
//
//func (pgCreds *PgCreds) autoDiscoverPgCreds() {
//
//}
