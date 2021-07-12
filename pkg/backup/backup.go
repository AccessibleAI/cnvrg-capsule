package backup

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	mlopsv1 "github.com/AccessibleAI/cnvrg-operator/api/v1"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"os/exec"
	"strings"
	"time"
)

var (
	log                = logrus.WithField("module", "backup-engine")
	BucketsToWatchChan = make(chan *Bucket, viper.GetInt("pg-backup-queue-depth"))
)

func Run() {
	log.Info("starting backup service...")
	go discoverPgBackups()
	go discoverCnvrgAppBackupBucketConfiguration(BucketsToWatchChan)
	go scanBucketForBackupRequests(BucketsToWatchChan)

	//pgBackupsChan := make(chan *PgBackup, viper.GetInt("pg-backup-queue-depth"))
	//go RunDiscovery(pgBackupsChan)
	//go RunPgBackups(pgBackupsChan)
}

func RunDiscovery(pgBackupsChan chan<- *PgBackup) {

	stopChan := make(chan bool)
	go discoverPgBackups()
	<-stopChan
}

func RunPgBackups(pgBackupsChan <-chan *PgBackup) {

	for backup := range pgBackupsChan {
		log.Infof("received new backup request:  %s", backup.BackupId)
		backup.dumpDb()
		backup.uploadDbDump()
	}
}

func WatchBackupRequest() {

}

func discoverPgBackups() {

	// if auto-discovery is true
	if viper.GetBool("auto-discovery") {
		for {
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

				// create backup request
				idPrefix := fmt.Sprintf("%s-%s", app.Name, app.Namespace)
				period := app.Spec.Dbs.Pg.Backup.Period
				rotation := app.Spec.Dbs.Pg.Backup.Rotation
				backup := NewPgBackup(idPrefix, period, rotation, *bucket, *pgCreds)
				if err := backup.createBackupRequest(); err != nil {
					log.Errorf("error creating backup request, err: %s", err)
				}
			}
			time.Sleep(10 * time.Second)
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

func (b *Bucket) getMinioClient() *minio.Client {

	connOptions := &minio.Options{Creds: credentials.NewStaticV4(b.AccessKey, b.SecretKey, ""), Secure: b.UseSSL}
	mc, err := minio.New(b.Endpoint, connOptions)
	if err != nil {
		log.Fatal(err)
	}
	return mc
}

func (pb *PgBackup) uploadDbDump() {
	exists, err := pb.ensureBackupBucketExists()
	if err != nil || !exists {
		log.Errorf("can't upload DB dump: %s, error during checking if bucket exists", pb.BackupId)
		return
	}

	file, err := os.Open(pb.LocalDumpPath)
	if err != nil {
		log.Errorf("can't open dump file: %s, err: %s", pb.BackupId, err)
		return
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		log.Errorf("can't open dump file: %s, err: %s", pb.BackupId, err)
		return
	}
	mc := pb.Bucket.getMinioClient()

	uploadInfo, err := mc.PutObject(context.Background(), pb.Bucket.Bucket, pb.RemoteDumpPath, file, fileStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		log.Error(err)
		return
	}
	log.Infof("successfully uploaded DB dump: %s, size: %d", pb.BackupId, uploadInfo.Size)

}

func (pb *PgBackup) dumpDb() {
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

	log.Infof("backup %s is finished", pb.BackupId)

}

func (pb *PgBackup) createBackupRequest() error {

	exists, err := pb.ensureBackupBucketExists()

	if err != nil || !exists {
		log.Errorf("can't upload DB dump: %s, error during checking if bucket exists", pb.BackupId)
		return err
	}

	jsonStr, err := pb.jsonify()
	if err != nil {
		return err
	}
	f := strings.NewReader(jsonStr)

	objectName := fmt.Sprintf("%s/%s.json", pb.RemoteDumpPath, pb.BackupId)
	userTags := map[string]string{IndexfileTag: "true", "foo": "bar"}
	po := minio.PutObjectOptions{ContentType: "application/octet-stream", UserMetadata: userTags}
	_, err = pb.Bucket.getMinioClient().PutObject(context.Background(), pb.Bucket.Bucket, objectName, f, f.Size(), po)
	if err != nil {
		log.Errorf("error during putting objcet: %s to S3, err: %s", objectName, err)
		return err
	}
	log.Infof("successfully uploaded: %v", objectName)
	return nil
}

func (pb *PgBackup) ensureBackupBucketExists() (exists bool, err error) {
	backupBucket := viper.GetString("backup-bucket")
	exists, err = pb.Bucket.getMinioClient().BucketExists(context.Background(), pb.Bucket.Bucket)
	if err != nil {
		log.Errorf("can't check if %s exists, err: %s", backupBucket, err)
	}
	if exists {
		log.Infof("backup bucket %s exists", backupBucket)
	} else {
		log.Errorf("backup bucket %s does not exists", backupBucket)
		return false, &BucketDoesNotExists{BucketName: backupBucket, Message: "bucket does not exists"}
	}
	return
}

func discoverCnvrgAppBackupBucketConfiguration(bb chan<- *Bucket) {

	if viper.GetBool("auto-discovery") { // auto-discovery is true
		for {
			// get all cnvrg apps
			apps := k8s.GetCnvrgApps()
			for _, app := range apps.Items {
				// make sure backups enabled
				if !shouldBackup(app) {
					continue // backup not required, either backup disabled or the ns is blocked for backups
				}
				// discover destination bucket
				bucket, err := NewBackupBucketWithAutoDiscovery(app.Spec.Dbs.Pg.Backup.BucketRef, app.Namespace)
				if err != nil {
					return
				}
				bb <- bucket
			}
			time.Sleep(5 * time.Second)
		}
	}
}

func scanBucketForBackupRequests(bb <-chan *Bucket) {
	for bucket := range bb {
		bucket.scanBucket()
	}
}

func (b *Bucket) scanBucket() {
	lo := minio.ListObjectsOptions{Prefix: b.DstDir, Recursive: true, WithMetadata: true}
	objectCh := b.getMinioClient().ListObjects(context.Background(), b.Bucket, lo)
	for object := range objectCh {
		if object.Err != nil {
			log.Errorf("error listing backups in: %s , err: %s ", b.Id, object.Err)
			return
		}
		log.Info(object.UserMetadata)
		log.Info(object.Metadata)
		log.Info(object.Key)
	}
}

//
//func (pgCreds *PgCreds) autoDiscoverPgCreds() {
//
//}
