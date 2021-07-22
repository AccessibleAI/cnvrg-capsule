package backup

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	"github.com/lithammer/shortuuid/v3"
	"github.com/spf13/viper"
	"k8s.io/apimachinery/pkg/types"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

type PgCreds struct {
	Host   string `json:"host,omitempty"`
	DbName string `json:"db,omitempty"`
	User   string `json:"user,omitempty"`
	Pass   string `json:"pass,omitempty"`
}

type PgBackup struct {
	BackupId       string    `json:"backupId"`
	Bucket         Bucket    `json:"bucket"`
	Status         Status    `json:"status"`
	BackupDate     time.Time `json:"backupDate"`
	PgCreds        PgCreds   `json:"pgCreds,omitempty"`
	LocalDumpPath  string    `json:"localDumpPath"`
	RemoteDumpPath string    `json:"remoteDumpPath"`
	BackupCmd      []string  `json:"backupCmd"`
	Period         float64   `json:"period"`
	Rotation       int       `json:"rotation"`
}

func (pb *PgBackup) Jsonify() (string, error) {
	jsonStr, err := json.Marshal(pb)
	if err != nil {
		log.Errorf("can't marshal struct, err: %v", err)
		return "", nil
	}
	return string(jsonStr), nil
}

func (pb *PgBackup) DumpDb() error {
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
	// activate backup in runtime - so other go routine won't initiate backup process again
	pb.activate()
	// deactivate backup
	defer pb.deactivate()

	// dump db
	if err := pb.setStatusAndSyncState(DumpingDB); err != nil {
		return err
	}
	if err := pb.DumpDb(); err != nil {
		_ = pb.setStatusAndSyncState(Failed)
		return err
	}

	// upload db dump to s3
	if err := pb.setStatusAndSyncState(UploadingDB); err != nil {
		return err
	}
	if err := pb.Bucket.UploadFile(pb.LocalDumpPath, pb.getDbDumpFileName()); err != nil {
		_ = pb.setStatusAndSyncState(Failed)
		return err
	}

	// finish backup
	if err := pb.setStatusAndSyncState(Finished); err != nil {
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

	jsonStr, err := pb.Jsonify()
	if err != nil {
		return err
	}
	_ = pb.Bucket.SyncMetadataState(jsonStr, pb.getBackupIndexFileName())

	return nil
}

func (pb *PgBackup) ensureBackupBucketExists() (exists bool, err error) {
	//if pb.Bucket.BucketType == AzureBucket || pb.Bucket.BucketType == AwsBucket {
	//	log.Debugf("bucket type of: %s doesn not required bucket existing check, skipping", pb.Bucket.BucketType)
	//	return true, nil
	//}
	//backupBucket := viper.GetString("backup-bucket")
	//exists, err = pb.Bucket.getMinioClient().BucketExists(context.Background(), pb.Bucket.Bucket)
	//if err != nil {
	//	log.Errorf("can't check if %s exists, err: %s", backupBucket, err)
	//}
	//if exists {
	//	log.Debugf("backup bucket %s exists", backupBucket)
	//} else {
	//	log.Errorf("backup bucket %s does not exists", backupBucket)
	//	return false, &BucketDoesNotExists{BucketName: backupBucket, Message: "bucket does not exists"}
	//}
	return true, nil
}

func (pb *PgBackup) ensureBackupRequestIsNeeded() bool {
	//pgBackups := pb.Bucket.ScanBucket()
	//// backup is needed if backups list is empty
	//if len(pgBackups) == 0 {
	//	log.Info("no backups has been done so far, backup is required")
	//	return true
	//}
	//
	//// make sure if period for the next backup has been reached
	//diff := time.Now().Sub(pgBackups[0].BackupDate).Seconds()
	//if diff < pgBackups[0].Period {
	//	log.Infof("latest backup not reached expiration period (left: %fs), backup is not required", pgBackups[0].Period-diff)
	//	return false
	//}
	//
	//// period has been expired, make sure max rotation didn't reached
	//if len(pgBackups) <= pb.Rotation {
	//	log.Infof("latest backup is old enough (%fs), backup is required", diff-pgBackups[0].Period)
	//	return true
	//}
	//
	//log.Warnf("max rotation has been reached (how come? this shouldn't happen?! 🙀) bucketId: %s, cleanup backups manually, and ask Dima wtf?", pb.Bucket.Id)
	return true
}

func (pb *PgBackup) syncBackupStateAzure() error {
	//credential, err := azblob.NewSharedKeyCredential(pb.Bucket.AccessKey, pb.Bucket.SecretKey)
	//if err != nil {
	//	log.Errorf("error saving object: %s to S3, err: %s", pb.getBackupIndexFileName(), err)
	//}
	//p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	//URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", pb.Bucket.AccessKey, pb.Bucket.Bucket))
	//containerURL := azblob.NewContainerURL(*URL, p)
	//jsonStr, err := pb.Jsonify()
	//if err != nil {
	//	return err
	//}
	//f := strings.NewReader(jsonStr)
	//blobURL := containerURL.NewBlockBlobURL(pb.getBackupIndexFileName())
	//options := azblob.UploadStreamToBlockBlobOptions{BufferSize: 2 * 1024 * 1024, MaxBuffers: 3}
	//_, err = azblob.UploadStreamToBlockBlob(context.Background(), f, blobURL, options)
	//if err != nil {
	//	log.Errorf("error uploaind bolb to azure storage, err: %s", err)
	//	return err
	//}
	return nil

}

func (pb *PgBackup) getBackupIndexFileName() string {
	return fmt.Sprintf("%s/%s-%s.json", pb.RemoteDumpPath, IndexfileTag, pb.BackupId)
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

func (pb *PgBackup) remove() bool {

	if err := pb.Bucket.Remove(pb.getBackupIndexFileName()); err != nil {
		log.Error(err)
		return false
	}

	if err := pb.Bucket.Remove(pb.getDbDumpFileName()); err != nil {
		log.Error(err)
		return false
	}

	log.Infof("backup dir: %s has been removed", pb.RemoteDumpPath)
	return true
}

func (pb *PgBackup) UnmarshalJSON(b []byte) error {
	//help: http://gregtrowbridge.com/golang-json-serialization-with-interfaces/

	// get the bucket struct
	var objMap map[string]*json.RawMessage
	if err := json.Unmarshal(b, &objMap); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal backupId
	if err := json.Unmarshal(*objMap["backupId"], &pb.BackupId); err != nil {
		log.Error(err)
		return err
	}
	// unmarshal backupId
	if err := json.Unmarshal(*objMap["backupId"], &pb.BackupId); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal status
	if err := json.Unmarshal(*objMap["status"], &pb.Status); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal backupDate
	if err := json.Unmarshal(*objMap["backupDate"], &pb.BackupDate); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal PG Creds
	if err := json.Unmarshal(*objMap["pgCreds"], &pb.PgCreds); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal local dump path
	if err := json.Unmarshal(*objMap["localDumpPath"], &pb.LocalDumpPath); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal remote dump path
	if err := json.Unmarshal(*objMap["remoteDumpPath"], &pb.RemoteDumpPath); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal backup cmd
	if err := json.Unmarshal(*objMap["backupCmd"], &pb.BackupCmd); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal period
	if err := json.Unmarshal(*objMap["period"], &pb.Period); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal rotation
	if err := json.Unmarshal(*objMap["rotation"], &pb.Rotation); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal status
	if err := json.Unmarshal(*objMap["status"], &pb.Status); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal bucket
	var bucketData map[string]interface{}
	if err := json.Unmarshal(*objMap["bucket"], &bucketData); err != nil {
		log.Error(err)
		return err
	}
	if strings.Contains(bucketData["id"].(string), "minio-") {
		mb := MinioBucket{}
		if err := json.Unmarshal(*objMap["bucket"], &mb); err != nil {
			log.Error(err)
			return err
		}
		pb.Bucket = &mb
	}
	return nil
}

func (pb *PgBackup) setStatusAndSyncState(s Status) error {
	pb.Status = s
	jsonStr, err := pb.Jsonify()
	if err != nil {
		log.Errorf("error jsonify, err: %s", err)
		return err
	}
	if err := pb.Bucket.SyncMetadataState(jsonStr, pb.getBackupIndexFileName()); err != nil {
		log.Errorf("error syncing metadata state, err: %s", err)
		return err
	}
	return nil
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

func NewPgBackup(idPrefix string, period string, rotation int, bucket Bucket, creds PgCreds) *PgBackup {
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
		RemoteDumpPath: fmt.Sprintf("%s/%s-pg", bucket.GetDstDir(), backupId),
		Period:         getPeriodInSeconds(period),
		Rotation:       rotation,
	}
	log.Debugf("new PG Backup initiated: %#v", b)
	return b
}

func getPeriodInSeconds(period string) float64 {
	unit := period[len(period)-1:]
	n, err := strconv.ParseFloat(period[:len(period)-1], 64)
	if err != nil {
		log.Fatalf("can't get cust period to int64, err: %s", err)
	}
	switch unit {
	case "s":
		return n // return as is
	case "m":
		return n * 60 // return minutes as seconds
	case "h":
		return n * 60 * 60 // return hours as seconds
	}
	log.Fatalf("period worng format, must be on of the [Xs, Xm, Xh]: %s", err)
	return n
}
