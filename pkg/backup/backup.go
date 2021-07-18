package backup

import (
	"bufio"
	"bytes"
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
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	log                = logrus.WithField("module", "backup-engine")
	mutex              = sync.Mutex{}
	BucketsToWatchChan = make(chan *Bucket, viper.GetInt("pg-backup-queue-depth"))
	activeBackups      = map[string]bool{}
)

func Run() {
	log.Info("starting backup service...")
	go discoverPgBackups()
	go discoverCnvrgAppBackupBucketConfiguration(BucketsToWatchChan)
	go scanBucketForBackupRequests(BucketsToWatchChan)
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
			time.Sleep(60 * time.Second)
		}
	}
}

func shouldBackup(app mlopsv1.CnvrgApp) bool {
	nsWhitelist := viper.GetString("ns-whitelist")
	if *app.Spec.Dbs.Pg.Backup.Enabled {
		if nsWhitelist == "*" || strings.Contains(nsWhitelist, app.Namespace) {
			log.Infof("backup enabled for: %s/%s", app.Namespace, app.Name)
			return true
		}
	} else {
		log.Info("skipping, backup is not enabled (or whitelisted) for: %s/%s", app.Namespace, app.Name)
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

func (pb *PgBackup) Jsonify() (string, error) {
	jsonStr, err := json.Marshal(pb)
	if err != nil {
		log.Errorf("can't marshal struct, err: %v", err)
		return "", nil
	}
	return string(jsonStr), nil
}

func (pb *PgBackup) uploadDbDump() error {
	exists, err := pb.ensureBackupBucketExists()
	if err != nil || !exists {
		log.Errorf("can't upload DB dump: %s, error during checking if bucket exists", pb.BackupId)
		return err
	}

	file, err := os.Open(pb.LocalDumpPath)
	if err != nil {
		log.Errorf("can't open dump file: %s, err: %s", pb.BackupId, err)
		return err
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		log.Errorf("can't open dump file: %s, err: %s", pb.BackupId, err)
		return err
	}
	mc := pb.Bucket.getMinioClient()
	uploadInfo, err := mc.PutObject(context.Background(), pb.Bucket.Bucket, pb.getDbDumpFileName(), file, fileStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		log.Error(err)
		return err
	}
	log.Infof("successfully uploaded DB dump: %s, size: %d", pb.BackupId, uploadInfo.Size)
	return nil
}

func (pb *PgBackup) dumpDb() error {
	log.Infof("starting backup: %s", pb.BackupId)
	cmdParams := append([]string{"-lc"}, strings.Join(pb.BackupCmd, " "))
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
		log.Infof("|%s| %s", pb.BackupId, m)
	}

	stderrScanner := bufio.NewScanner(stderr)
	for stderrScanner.Scan() {
		m := stderrScanner.Text()
		log.Errorf("|%s| %s", pb.BackupId, m)
		return err
	}

	if err := cmd.Wait(); err != nil {
		log.Error(err)
		return err
	}

	log.Infof("backup %s is finished", pb.BackupId)
	return nil

}

func (pb *PgBackup) backup() error {

	// if backups status is Finished - all good, backup is ready
	if pb.Status == Finished {
		log.Infof("backup: %s status is finished, skipping backup", pb.BackupId)
		return nil
	}
	// check if current backups is not active in another backup go routine
	if pb.active() {
		log.Infof("backup %s is active, skipping", pb.BackupId)
		return nil
	}
	// activate backup in runtime - so other go routine won't initiate a backup process again
	pb.activate()
	// deactivate backup
	defer pb.deactivate()
	// dump db
	pb.Status = DumpingDB
	if err := pb.syncBackupState(); err != nil {
		return err
	}
	if err := pb.dumpDb(); err != nil {
		pb.Status = Failed
		pb.syncBackupState()
		return err
	}
	// upload db dump to s3
	pb.Status = UploadingDB
	if err := pb.syncBackupState(); err != nil {
		return err
	}
	if err := pb.uploadDbDump(); err != nil {
		pb.Status = Failed
		pb.syncBackupState()

		return err
	}

	// finish backup
	pb.Status = Finished
	if err := pb.syncBackupState(); err != nil {
		return err
	}

	return nil
}

func (pb *PgBackup) createBackupRequest() error {

	exists, err := pb.ensureBackupBucketExists()

	if err != nil || !exists {
		log.Errorf("can't upload DB dump: %s, error during checking if bucket exists", pb.BackupId)
		return err
	}

	if !pb.ensureBackupRequestIsNeeded() {
		log.Infof("backup %s is not needed, skipping", pb.BackupId)
		return nil
	}

	pb.syncBackupState()

	return nil
}

func (pb *PgBackup) ensureBackupBucketExists() (exists bool, err error) {
	backupBucket := viper.GetString("backup-bucket")
	exists, err = pb.Bucket.getMinioClient().BucketExists(context.Background(), pb.Bucket.Bucket)
	if err != nil {
		log.Errorf("can't check if %s exists, err: %s", backupBucket, err)
	}
	if exists {
		log.Debugf("backup bucket %s exists", backupBucket)
	} else {
		log.Errorf("backup bucket %s does not exists", backupBucket)
		return false, &BucketDoesNotExists{BucketName: backupBucket, Message: "bucket does not exists"}
	}
	return
}

func (pb *PgBackup) ensureBackupRequestIsNeeded() bool {
	pgBackups := pb.Bucket.ScanBucket()
	// backup is needed if backups list is empty
	if len(pgBackups) == 0 {
		log.Info("no backups has been done so far, backup is required")
		return true
	}

	// make sure if period for the next backup has been reached
	diff := time.Now().Sub(pgBackups[0].BackupDate).Minutes()
	if int(diff) < pgBackups[0].Period {
		log.Infof("latest backup not reached expiration period (left: %dm), backup is not required", pgBackups[0].Period-int(diff))
		return false
	}

	// period has been expired, make sure max rotation didn't reached
	if len(pgBackups) <= pb.Rotation {
		log.Info("latest backup is old enough and max rotation didn't reached yet, backup is required")
		return true
	}

	log.Warnf("max rotation has been reached (how come? this shouldn't happen?! 🙀) bucketId: %s, cleanup backups manually, and ask Dima wtf?", pb.Bucket.Id)
	return false
}

func (pb *PgBackup) syncBackupState() error {
	jsonStr, err := pb.Jsonify()
	if err != nil {
		return err
	}
	f := strings.NewReader(jsonStr)
	userTags := map[string]string{IndexfileTag: "true"}
	po := minio.PutObjectOptions{ContentType: "application/octet-stream", UserMetadata: userTags}
	_, err = pb.Bucket.getMinioClient().PutObject(context.Background(), pb.Bucket.Bucket, pb.getBackupIndexFileName(), f, f.Size(), po)
	if err != nil {
		log.Errorf("error saving object: %s to S3, err: %s", pb.getBackupIndexFileName(), err)
		return err
	}
	log.Infof("backup state synchronized: %v", pb.getBackupIndexFileName())
	return nil

}

func (pb *PgBackup) getBackupIndexFileName() string {
	return fmt.Sprintf("%s/%s.json", pb.RemoteDumpPath, pb.BackupId)
}

func (pb *PgBackup) getDbDumpFileName() string {
	return fmt.Sprintf("%s/%s.tar", pb.RemoteDumpPath, pb.BackupId)
}

func (pb *PgBackup) active() bool {
	mutex.Lock()
	_, active := activeBackups[pb.BackupId]
	mutex.Unlock()
	log.Infof("backup: %s is active: %v", pb.BackupId, active)
	return active
}

func (pb *PgBackup) activate() {
	mutex.Lock()
	activeBackups[pb.BackupId] = true
	mutex.Unlock()
	log.Infof("backup: %s has been activated", pb.BackupId)
}

func (pb *PgBackup) deactivate() {
	mutex.Lock()
	if _, active := activeBackups[pb.BackupId]; active {
		delete(activeBackups, pb.BackupId)
	}
	mutex.Unlock()
	log.Infof("backup: %s has been deactivated", pb.BackupId)
}

func (pb *PgBackup) remove() {
	mc := pb.Bucket.getMinioClient()
	opts := minio.RemoveObjectOptions{GovernanceBypass: true}
	err := mc.RemoveObject(context.Background(), pb.Bucket.Bucket, pb.getBackupIndexFileName(), opts)
	if err != nil {
		log.Error(err)
	}
	err = mc.RemoveObject(context.Background(), pb.Bucket.Bucket, pb.getDbDumpFileName(), opts)
	if err != nil {
		log.Error(err)
	}
	log.Infof("backup dir: %s has been removed", pb.RemoteDumpPath)
}

func (b *Bucket) getMinioClient() *minio.Client {

	connOptions := &minio.Options{Creds: credentials.NewStaticV4(b.AccessKey, b.SecretKey, ""), Secure: b.UseSSL}
	mc, err := minio.New(b.Endpoint, connOptions)
	if err != nil {
		log.Fatal(err)
	}
	return mc
}

func (b *Bucket) ScanBucket() []*PgBackup {
	var pgBackups []*PgBackup
	lo := minio.ListObjectsOptions{Prefix: b.DstDir, Recursive: true, WithMetadata: true}
	objectCh := b.getMinioClient().ListObjects(context.Background(), b.Bucket, lo)
	for object := range objectCh {
		if object.Err != nil {
			log.Errorf("error listing backups in: %s , err: %s ", b.Id, object.Err)
			return nil
		}
		indexfileMetadataKey := fmt.Sprintf("X-Amz-Meta-%s", IndexfileTag)
		if _, ok := object.UserMetadata[indexfileMetadataKey]; ok {
			mc := b.getMinioClient()
			stream, err := mc.GetObject(context.Background(), b.Bucket, object.Key, minio.GetObjectOptions{})
			if err != nil {
				log.Errorf("can't get object: %s, err: %s", object.Key, err)
				continue
			}
			buf := new(bytes.Buffer)
			if _, err := buf.ReadFrom(stream); err != nil {
				log.Errorf("error reading stream, object: %s, err: %s", object.Key, err)
				continue
			}
			pgBackup := PgBackup{}
			if err := json.Unmarshal(buf.Bytes(), &pgBackup); err != nil {
				log.Errorf("error unmarshal PgBackup request, object: %s, err: %s", object.Key, err)
				continue
			}
			pgBackups = append(pgBackups, &pgBackup)
		}
	}
	sort.Slice(pgBackups, func(i, j int) bool { return pgBackups[i].BackupDate.After(pgBackups[j].BackupDate) })
	return pgBackups
}

func (b *Bucket) rotateBackups() {
	backups := b.ScanBucket()
	backupCount := len(backups)

	// nothing to rotate when no backups exists
	if backupCount == 0 {
		log.Infof("in bucket: %s, pg backups list is 0, skipping rotation", b.Id)
		return
	}

	// rotation not needed yet
	if backupCount < backups[0].Rotation {
		log.Infof("in bucket: %s, max rotation not reached yet (current: %d), skipping rotation", b.Id, backupCount)
		return
	}
	log.Infof("in bucket: %s, max rotation has been reached, rotating...", b.Id)

	// remove the oldest backup
	backups[backupCount-1].remove()
}

func GetBackupBuckets() (bucket []*Bucket) {
	apps := k8s.GetCnvrgApps()
	for _, app := range apps.Items {
		// make sure backups enabled
		if !shouldBackup(app) {
			continue // backup not required, either backup disabled or the ns is blocked for backups
		}
		// discover destination bucket
		b, err := NewBackupBucketWithAutoDiscovery(app.Spec.Dbs.Pg.Backup.BucketRef, app.Namespace)
		if err != nil {
			log.Errorf("error discovering backup bucket, err: %s", err)
			continue
		}
		bucket = append(bucket, b)
	}
	return bucket
}

func discoverCnvrgAppBackupBucketConfiguration(bc chan<- *Bucket) {

	if viper.GetBool("auto-discovery") { // auto-discovery is true
		for {
			for _, b := range GetBackupBuckets() {
				bc <- b
			}
			time.Sleep(10 * time.Second)
		}
	}
}

func scanBucketForBackupRequests(bb <-chan *Bucket) {
	for bucket := range bb {
		bucket.rotateBackups()
		for _, pgBackup := range bucket.ScanBucket() {
			go pgBackup.backup()
		}
	}
}
