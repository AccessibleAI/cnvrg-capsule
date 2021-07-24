package backup

import (
	"encoding/json"
	"fmt"
	"github.com/lithammer/shortuuid/v3"
	"strconv"
	"strings"
	"time"
)

//type Backup struct {
//	BackupId string    `json:"backupId"`
//	Rotation int       `json:"rotation"`
//	Date     time.Time `json:"date"`
//	Period   float64   `json:"period"`
//	Bucket   Bucket    `json:"bucket"`
//	Service  Service   `json:"service"`
//	Status   Status    `json:"status"`
//}

func (pb *Backup) Jsonify() (string, error) {
	jsonStr, err := json.Marshal(pb)
	if err != nil {
		log.Errorf("can't marshal struct, err: %v", err)
		return "", nil
	}
	return string(jsonStr), nil
}

func (pb *Backup) DumpDb() error {
	//log.Infof("starting backup: %s", pb.BackupId)
	//cmdParams := append([]string{"-lc"}, strings.Join(pb.BackupCmd, " "))
	//log.Debugf("pg backup cmd: %s ", cmdParams)
	//cmd := exec.Command("/bin/bash", cmdParams...)
	//
	//stdout, err := cmd.StdoutPipe()
	//if err != nil {
	//	log.Error(err)
	//	return err
	//}
	//
	//stderr, err := cmd.StderrPipe()
	//if err != nil {
	//	log.Error(err)
	//	return err
	//}
	//
	//err = cmd.Start()
	//if err != nil {
	//	log.Error(err)
	//	return err
	//}
	//
	//stdoutScanner := bufio.NewScanner(stdout)
	//for stdoutScanner.Scan() {
	//	m := stdoutScanner.Text()
	//	log.Infof("|%s| %s", pb.BackupId, m)
	//}
	//
	//stderrScanner := bufio.NewScanner(stderr)
	//for stderrScanner.Scan() {
	//	m := stderrScanner.Text()
	//	log.Errorf("|%s| %s", pb.BackupId, m)
	//	return err
	//}
	//
	//if err := cmd.Wait(); err != nil {
	//	log.Error(err)
	//	return err
	//}
	//
	//log.Infof("backup %s is finished", pb.BackupId)
	return nil

}

func (pb *Backup) backup() error {

	//// if backups status is Finished - all good, backup is ready
	//if pb.Status == Finished {
	//	log.Infof("backup: %s status is finished, skipping backup", pb.BackupId)
	//	return nil
	//}
	//
	//// check if current backups is not active in another backup go routine
	//if pb.active() {
	//	log.Infof("backup %s is active, skipping", pb.BackupId)
	//	return nil
	//}
	//// activate backup in runtime - so other go routine won't initiate backup process again
	//pb.activate()
	//// deactivate backup
	//defer pb.deactivate()
	//
	//// dump db
	//if err := pb.setStatusAndSyncState(DumpingDB); err != nil {
	//	return err
	//}
	//if err := pb.DumpDb(); err != nil {
	//	_ = pb.setStatusAndSyncState(Failed)
	//	return err
	//}
	//
	//// upload db dump to s3
	//if err := pb.setStatusAndSyncState(UploadingDB); err != nil {
	//	return err
	//}
	//if err := pb.Bucket.UploadFile(pb.LocalDumpPath, pb.getDbDumpFileName()); err != nil {
	//	_ = pb.setStatusAndSyncState(Failed)
	//	return err
	//}
	//
	//// finish backup
	//if err := pb.setStatusAndSyncState(Finished); err != nil {
	//	return err
	//}

	return nil
}

func (pb *Backup) createBackupRequest() error {

	if err := pb.Bucket.Ping(); err != nil {
		log.Errorf("can't upload DB dump: %s, error during pinging bucket", pb.BackupId)
		return err
	}

	if !pb.ensureBackupRequestIsNeeded(pb.ServiceType) {
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

func (pb *Backup) ensureBackupRequestIsNeeded(serviceType ServiceType) bool {
	backups := pb.Bucket.ScanBucket(serviceType)
	// backup is needed if backups list is empty
	if len(backups) == 0 {
		log.Info("no backups has been done so far, backup is required")
		return true
	}

	// make sure if period for the next backup has been reached
	diff := time.Now().Sub(backups[0].Date).Seconds()
	if diff < backups[0].Period {
		log.Infof("latest backup not reached expiration period (left: %fs), backup is not required", backups[0].Period-diff)
		return false
	}

	// period has been expired, make sure max rotation didn't reached
	if len(backups) <= pb.Rotation {
		log.Infof("latest backup is old enough (%fs), backup is required", diff-backups[0].Period)
		return true
	}

	log.Warnf("max rotation has been reached (how come? this shouldn't happen?! 🙀) bucketId: %s, cleanup backups manually, and ask Dima wtf?", pb.Bucket.BucketId())
	return false
}

func (pb *Backup) syncBackupStateAzure() error {
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

func (pb *Backup) getBackupIndexFileName() string {
	return fmt.Sprintf("%s/%s", pb.BackupId, Indexfile)
}

func (pb *Backup) getDbDumpFileName() string {
	//return fmt.Sprintf("%s/%s.tar", pb.RemoteDumpPath, pb.BackupId)
	return fmt.Sprintf("%s/%s.tar", "pb.RemoteDumpPath", pb.BackupId)
}

func (pb *Backup) active() bool {
	mutex.Lock()
	_, active := activeBackups[pb.BackupId]
	mutex.Unlock()
	log.Infof("backup: %s is active: %v", pb.BackupId, active)
	return active
}

func (pb *Backup) activate() {
	mutex.Lock()
	activeBackups[pb.BackupId] = true
	mutex.Unlock()
	log.Infof("backup: %s has been activated", pb.BackupId)
}

func (pb *Backup) deactivate() {
	mutex.Lock()
	if _, active := activeBackups[pb.BackupId]; active {
		delete(activeBackups, pb.BackupId)
	}
	mutex.Unlock()
	log.Infof("backup: %s has been deactivated", pb.BackupId)
}

func (pb *Backup) remove() bool {

	//if err := pb.Bucket.Remove(pb.getBackupIndexFileName()); err != nil {
	//	log.Error(err)
	//	return false
	//}
	//
	//if err := pb.Bucket.Remove(pb.getDbDumpFileName()); err != nil {
	//	log.Error(err)
	//	return false
	//}
	//
	//log.Infof("backup dir: %s has been removed", pb.RemoteDumpPath)
	return true
}

func (pb *Backup) UnmarshalJSON(b []byte) error {
	//help: http://gregtrowbridge.com/golang-json-serialization-with-interfaces/

	//get the bucket struct
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

	// unmarshal status
	if err := json.Unmarshal(*objMap["status"], &pb.Status); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal backupDate
	if err := json.Unmarshal(*objMap["date"], &pb.Date); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal service type
	if err := json.Unmarshal(*objMap["serviceType"], &pb.ServiceType); err != nil {
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

	// unmarshal service
	if pb.ServiceType == PgService {
		pgBackupService := PgBackupService{}
		if err := json.Unmarshal(*objMap["service"], &pgBackupService); err != nil {
			log.Error(err)
			return err
		}
		pb.Service = &pgBackupService
	}
	return nil
}

func (pb *Backup) setStatusAndSyncState(s Status) error {
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

//func NewPgCredsWithAutoDiscovery(credsRef, ns string) (*PgCreds, error) {
//	n := types.NamespacedName{Namespace: ns, Name: credsRef}
//	pgSecret := k8s.GetSecret(n)
//	if err := validatePgCreds(n.Name, pgSecret.Data); err != nil {
//		log.Errorf("pg creds secret invalid, err: %s", err)
//		return nil, err
//	}
//	return &PgCreds{
//		Host:   fmt.Sprintf("%s.%s", pgSecret.Data["POSTGRES_HOST"], ns),
//		DbName: string(pgSecret.Data["POSTGRES_DB"]),
//		User:   string(pgSecret.Data["POSTGRES_USER"]),
//		Pass:   string(pgSecret.Data["POSTGRES_PASSWORD"]),
//	}, nil
//}

//func NewPgBackup(idPrefix string, period string, rotation int, bucket Bucket, creds PgCreds) *Backup {
//backupId := fmt.Sprintf("%s-%s", idPrefix, shortuuid.New())
//backupTime := time.Now()
////localDumpPath := fmt.Sprintf("%s/%s.tar", viper.GetString("dumpdir"), backupId)
////backupCmd := []string{
////	"2>&1", // for some reason pg_dump with verbose mode outputs to stderr (wtf?)
////	"pg_dump",
////	fmt.Sprintf("--dbname=postgresql://%s:%s@%s:5432/%s", creds.User, creds.Pass, creds.Host, creds.DbName),
////	fmt.Sprintf("--file=%s", localDumpPath),
////	"--format=t",
////	"--verbose",
////}
//b := &Backup{
//	BackupId: backupId,
//	Bucket:   bucket,
//	//PgCreds:        creds,
//	Status:     Initialized,
//	BackupDate: backupTime,
//	//BackupCmd:      backupCmd,
//	//LocalDumpPath:  localDumpPath,
//	//RemoteDumpPath: fmt.Sprintf("%s/%s-pg", bucket.GetDstDir(), backupId),
//	//Period:         getPeriodInSeconds(period),
//	//Rotation:       rotation,
//}
//log.Debugf("new PG Backup initiated: %#v", b)
//return b
//return nil
//}

func NewBackup(bucket Bucket, backupService Service, period string, rotation int) *Backup {
	b := &Backup{
		BackupId:    fmt.Sprintf("%s-%s", backupService.ServiceType(), shortuuid.New()),
		Bucket:      bucket,
		Status:      Initialized,
		Date:        time.Now(),
		ServiceType: backupService.ServiceType(),
		Service:     backupService,
		Period:      getPeriodInSeconds(period),
		Rotation:    rotation,
	}
	log.Debugf("new backup initiated: %#v", b)
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
