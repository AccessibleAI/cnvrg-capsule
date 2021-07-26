package backup

import (
	"encoding/json"
	"fmt"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	v1 "github.com/AccessibleAI/cnvrg-operator/api/v1"
	"github.com/lithammer/shortuuid/v3"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strconv"
	"sync"
	"time"
)

type Backup struct {
	BackupId    string      `json:"backupId"`
	Rotation    int         `json:"rotation"`
	Date        time.Time   `json:"date"`
	Period      float64     `json:"period"`
	BucketType  BucketType  `json:"bucketType"`
	Bucket      Bucket      `json:"bucket"`
	ServiceType ServiceType `json:"serviceType"`
	Service     Service     `json:"service"`
	Status      Status      `json:"status"`
	CnvrgAppRef string      `json:"cnvrgAppRef"`
}

var (
	log                = logrus.WithField("module", "backup-engine")
	mutex              = sync.Mutex{}
	BucketsToWatchChan = make(chan Bucket, 100)
	activeBackups      = map[string]bool{}
)

func (pb *Backup) Jsonify() (string, error) {
	jsonStr, err := json.Marshal(pb)
	if err != nil {
		log.Errorf("can't marshal struct, err: %v", err)
		return "", nil
	}
	return string(jsonStr), nil
}

func (pb *Backup) backup() error {

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

	if err := pb.Service.Dump(); err != nil {
		_ = pb.setStatusAndSyncState(Failed)
		return err
	}

	// upload db dump to s3
	if err := pb.setStatusAndSyncState(UploadingDB); err != nil {
		return err
	}
	if err := pb.Service.UploadBackupAssets(pb.Bucket, pb.BackupId); err != nil {
		_ = pb.setStatusAndSyncState(Failed)
		return err
	}

	// finish backup
	if err := pb.setStatusAndSyncState(Finished); err != nil {
		return err
	}

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

	// unmarshal backup type
	if err := json.Unmarshal(*objMap["bucketType"], &pb.BucketType); err != nil {
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
	if pb.BucketType == MinioBucketType || pb.BucketType == AwsBucketType {
		mb := MinioBucket{}
		if err := json.Unmarshal(*objMap["bucket"], &mb); err != nil {
			log.Error(err)
			return err
		}
		pb.Bucket = &mb
	} else {
		err := &UnsupportedBucketError{}
		log.Error(err.Error())
		return err
	}

	// unmarshal service
	if pb.ServiceType == PgService {
		pgBackupService := PgBackupService{}
		if err := json.Unmarshal(*objMap["service"], &pgBackupService); err != nil {
			log.Error(err)
			return err
		}
		pb.Service = &pgBackupService
	} else {
		err := &UnsupportedBackupService{}
		log.Error(err.Error())
		return err
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

func Run() {
	log.Info("starting backup service...")
	go discoverBackups()
	go discoverCnvrgAppBackupBucketConfiguration(BucketsToWatchChan)
	go scanBucketForBackupRequests(BucketsToWatchChan)
}

func NewBackup(bucket Bucket, backupService Service, period string, rotation int, cnvrgAppRef string) *Backup {
	b := &Backup{
		BackupId:    fmt.Sprintf("%s-%s", backupService.ServiceType(), shortuuid.New()),
		BucketType:  bucket.BucketType(),
		Bucket:      bucket,
		Status:      Initialized,
		Date:        time.Now(),
		ServiceType: backupService.ServiceType(),
		Service:     backupService,
		Period:      getPeriodInSeconds(period),
		Rotation:    rotation,
		CnvrgAppRef: cnvrgAppRef,
	}
	log.Debugf("new backup initiated: %#v", b)
	return b
}

func GetBackupBuckets() (bucket []Bucket) {
	apps := k8s.GetCnvrgApps()
	for _, app := range apps.Items {
		// make sure backups enabled
		if !ShouldBackup(app) {
			continue // backup not required, either backup disabled or the ns is blocked for backups
		}
		// discover destination bucket
		b, err := NewBucketWithAutoDiscovery(app.Spec.Dbs.Pg.Backup.BucketRef, app.Namespace)
		if err != nil {
			log.Errorf("error discovering backup bucket, err: %s", err)
			continue
		}
		bucket = append(bucket, b)
	}
	return bucket
}

func discoverBackups() {
	//if auto-discovery is true
	if viper.GetBool("auto-discovery") {
		for {
			// get all cnvrg apps
			apps := k8s.GetCnvrgApps()
			for _, app := range apps.Items {

				// make sure backups enabled
				if !ShouldBackup(app) {
					continue // backup not required, either backup disabled or the ns is blocked for backups
				}

				// discover PG backups
				_ = discoverPgBackups(app)

			}
			time.Sleep(60 * time.Second)
		}
	}
}

func discoverPgBackups(app v1.CnvrgApp) error {

	//discover pg creds
	pgCreds, err := NewPgCredsWithAutoDiscovery(app.Spec.Dbs.Pg.Backup.CredsRef, app.Namespace)
	if err != nil {
		return err
	}
	// discover destination bucket
	bucket, err := NewBucketWithAutoDiscovery(app.Spec.Dbs.Pg.Backup.BucketRef, app.Namespace)
	if err != nil {
		return err
	}
	// create backup request
	period := app.Spec.Dbs.Pg.Backup.Period
	rotation := app.Spec.Dbs.Pg.Backup.Rotation
	pgBackupService := NewPgBackupService(*pgCreds)
	backup := NewBackup(bucket, pgBackupService, period, rotation, cnvrgAppRef(app))
	if err := backup.createBackupRequest(); err != nil {
		log.Errorf("error creating backup request, err: %s", err)
		return err
	}
	return nil
}

func discoverCnvrgAppBackupBucketConfiguration(bc chan<- Bucket) {

	if viper.GetBool("auto-discovery") { // auto-discovery is true
		for {
			for _, b := range GetBackupBuckets() {
				bc <- b
			}
			time.Sleep(10 * time.Second)
		}
	}
}

func scanBucketForBackupRequests(bb <-chan Bucket) {
	for bucket := range bb {
		pgBackups := bucket.ScanBucket(PgService)
		bucket.RotateBackups(pgBackups)
		for _, pgBackup := range bucket.ScanBucket(PgService) {
			go pgBackup.backup()
		}
	}
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

func cnvrgAppRef(app v1.CnvrgApp) string {
	return fmt.Sprintf("%s/%s", app.Namespace, app.Name)
}
