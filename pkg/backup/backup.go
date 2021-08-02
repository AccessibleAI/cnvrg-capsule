package backup

import (
	"encoding/json"
	"fmt"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	"github.com/lithammer/shortuuid/v3"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strconv"
	"sync"
	"time"
)

type Restore struct {
	Date   time.Time `json:"date"`
	Status Status    `json:"status"`
}

type Backup struct {
	BackupId    string      `json:"backupId"`
	Rotation    int         `json:"rotation"`
	Date        time.Time   `json:"date"`
	Period      float64     `json:"period"`
	BucketType  BucketType  `json:"bucketType"`
	Bucket      Bucket      `json:"bucket"`
	ServiceType ServiceType `json:"serviceType"`
	Service     Service     `json:"service"`
	Restores    []*Restore  `json:"restores"`
	Status      Status      `json:"status"`
}

type PvcAnnotation string

const (
	BackupEnabledAnnotation PvcAnnotation = "capsule.mlops.cnvrg.io/backup"
	ServiceTypeAnnotation   PvcAnnotation = "capsule.mlops.cnvrg.io/serviceType"
	BucketRefAnnotation     PvcAnnotation = "capsule.mlops.cnvrg.io/bucketRef"
	CredsRefAnnotation      PvcAnnotation = "capsule.mlops.cnvrg.io/credsRef"
	RotationRefAnnotation   PvcAnnotation = "capsule.mlops.cnvrg.io/rotation"
	PeriodAnnotation        PvcAnnotation = "capsule.mlops.cnvrg.io/period"
)

type DiscoveryInputs struct {
	BackupEnabled   bool
	ServiceType     ServiceType
	BucketRefSecret string
	CredsRefSecret  string
	Rotation        int
	Period          string
	PvcName         string
	PvcNamespace    string
}

var (
	log                = logrus.WithField("module", "backup-engine")
	mutex              = sync.Mutex{}
	BucketsToWatchChan = make(chan Bucket, 100)
	activeBackups      = map[string]bool{}
)

func (b *Backup) Jsonify() (string, error) {
	jsonStr, err := json.Marshal(b)
	if err != nil {
		log.Errorf("can't marshal struct, err: %v", err)
		return "", nil
	}
	return string(jsonStr), nil
}

func (b *Backup) backup() error {

	// if backup status is Finished - all good, backup is ready
	if b.Status == Finished {
		log.Infof("backupId: %s status: %s, skipping backup", b.BackupId, b.Status)
		return nil
	}

	// check if current backups is not active in another backup go routine
	if b.active() {
		log.Infof("backup %s is active, skipping", b.BackupId)
		return nil
	}
	// activate backup in runtime - so other go routine won't initiate backup process again
	b.activate()
	// deactivate backup
	defer b.deactivate()

	// dump db
	if err := b.SetStatusAndSyncState(DumpingDB); err != nil {
		return err
	}

	if err := b.Service.Dump(); err != nil {
		_ = b.SetStatusAndSyncState(Failed)
		return err
	}

	// upload db dump to s3
	if err := b.SetStatusAndSyncState(UploadingDB); err != nil {
		return err
	}
	if err := b.Service.UploadBackupAssets(b.Bucket, b.BackupId); err != nil {
		_ = b.SetStatusAndSyncState(Failed)
		return err
	}

	// finish backup
	if err := b.SetStatusAndSyncState(Finished); err != nil {
		return err
	}

	return nil
}

func (b *Backup) Restore() error {

	// Restore when status is RestoreRequest
	for _, requestRestore := range b.Restores {
		if requestRestore.Status == RestoreRequest {
			log.Infof("restoring backup: %s ", b.BackupId)
			// check if current backups is not active in another backup go routine
			if b.active() {
				log.Infof("Restore %s is active, skipping", b.BackupId)
				return nil
			}
			// activate Restore - so other go routine won't initiate Restore process again
			b.activate()
			// deactivate backup
			defer b.deactivate()
			// run Restore
			if err := b.Service.Restore(); err != nil {
				requestRestore.Status = Failed
				_ = b.SyncState()
				return err
			}
			// finish restore
			requestRestore.Status = Finished
			if err := b.SyncState(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Backup) createBackupRequest() error {

	if err := b.Bucket.Ping(); err != nil {
		log.Errorf("can't upload DB dump: %s, error during pinging bucket", b.BackupId)
		return err
	}

	if !b.ensureBackupRequestIsNeeded(b.ServiceType) {
		log.Infof("backup %s is not needed, skipping", b.BackupId)
		return nil
	}

	jsonStr, err := b.Jsonify()
	if err != nil {
		return err
	}
	_ = b.Bucket.SyncMetadataState(jsonStr, b.getBackupIndexFileName())

	return nil
}

func (b *Backup) ensureBackupRequestIsNeeded(serviceType ServiceType) bool {
	backups := b.Bucket.ScanBucket(serviceType)
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
	if len(backups) <= b.Rotation {
		log.Infof("latest backup is old enough (%fs), backup is required", diff-backups[0].Period)
		return true
	}

	log.Warnf("max rotation has been reached (how come? this shouldn't happen?! 🙀) bucketId: %s, cleanup backups manually, and ask Dima wtf?", b.Bucket.BucketId())
	return false
}

func (b *Backup) getBackupIndexFileName() string {
	return fmt.Sprintf("%s/%s", b.BackupId, Indexfile)
}

func (b *Backup) active() bool {
	mutex.Lock()
	_, active := activeBackups[b.BackupId]
	mutex.Unlock()
	log.Infof("backup: %s is active: %v", b.BackupId, active)
	return active
}

func (b *Backup) activate() {
	mutex.Lock()
	activeBackups[b.BackupId] = true
	mutex.Unlock()
	log.Infof("backup: %s has been activated", b.BackupId)
}

func (b *Backup) deactivate() {
	mutex.Lock()
	if _, active := activeBackups[b.BackupId]; active {
		delete(activeBackups, b.BackupId)
	}
	mutex.Unlock()
	log.Infof("backup: %s has been deactivated", b.BackupId)
}

func (b *Backup) UnmarshalJSON(bytes []byte) error {
	//help: http://gregtrowbridge.com/golang-json-serialization-with-interfaces/

	//get the bucket struct
	var objMap map[string]*json.RawMessage
	if err := json.Unmarshal(bytes, &objMap); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal backupId
	if err := json.Unmarshal(*objMap["backupId"], &b.BackupId); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal status
	if err := json.Unmarshal(*objMap["status"], &b.Status); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal backupDate
	if err := json.Unmarshal(*objMap["date"], &b.Date); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal backup type
	if err := json.Unmarshal(*objMap["bucketType"], &b.BucketType); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal service type
	if err := json.Unmarshal(*objMap["serviceType"], &b.ServiceType); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal period
	if err := json.Unmarshal(*objMap["period"], &b.Period); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal rotation
	if err := json.Unmarshal(*objMap["rotation"], &b.Rotation); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal restores
	if err := json.Unmarshal(*objMap["restores"], &b.Restores); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal status
	if err := json.Unmarshal(*objMap["status"], &b.Status); err != nil {
		log.Error(err)
		return err
	}

	// unmarshal bucket
	if b.BucketType == MinioBucketType || b.BucketType == AwsBucketType { // minio or aws bucket
		mb := MinioBucket{}
		if err := json.Unmarshal(*objMap["bucket"], &mb); err != nil {
			log.Error(err)
			return err
		}
		b.Bucket = &mb
	} else if b.BucketType == AzureBucketType { // azure bucket
		ab := AzureBucket{}
		if err := json.Unmarshal(*objMap["bucket"], &ab); err != nil {
			log.Error(err)
			return err
		}
		b.Bucket = &ab
	} else if b.BucketType == GcpBucketType {
		gb := GcpBucket{}
		if err := json.Unmarshal(*objMap["bucket"], &gb); err != nil {
			log.Error(err)
			return err
		}
		b.Bucket = &gb
	} else {
		err := &UnsupportedBucketError{}
		log.Error(err.Error())
		return err
	}

	// unmarshal service
	if b.ServiceType == PgService {
		pgBackupService := PgBackupService{}
		if err := json.Unmarshal(*objMap["service"], &pgBackupService); err != nil {
			log.Error(err)
			return err
		}
		b.Service = &pgBackupService
	} else {
		err := &UnsupportedBackupService{}
		log.Error(err.Error())
		return err
	}
	return nil
}

func (b *Backup) SetStatusAndSyncState(s Status) error {

	b.Status = s
	return b.SyncState()
}

func (b *Backup) SyncState() error {
	jsonStr, err := b.Jsonify()
	if err != nil {
		log.Errorf("error jsonify, err: %s", err)
		return err
	}
	if err := b.Bucket.SyncMetadataState(jsonStr, b.getBackupIndexFileName()); err != nil {
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

func NewDiscoveryInputs(inputs map[string]string, pvcName, ns string) *DiscoveryInputs {
	if err := validatePvcAnnotations(pvcName, inputs); err != nil {
		log.Errorf("bad pvc annotations, err: %s", err)
		return nil
	}
	ds := DiscoveryInputs{}
	enabled := inputs[string(BackupEnabledAnnotation)]
	if enabled == "true" {
		ds.BackupEnabled = true
	}
	ds.ServiceType = ServiceType(inputs[string(ServiceTypeAnnotation)])
	ds.BucketRefSecret = inputs[string(BucketRefAnnotation)]
	ds.CredsRefSecret = inputs[string(CredsRefAnnotation)]
	r, err := strconv.Atoi(inputs[string(RotationRefAnnotation)])
	if err != nil {
		log.Error(err)
		return nil
	}
	ds.Rotation = r
	ds.Period = inputs[string(PeriodAnnotation)]
	ds.PvcName = pvcName
	ds.PvcNamespace = ns
	return &ds

}

func NewBackup(bucket Bucket, backupService Service, period string, rotation int) *Backup {
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
		Restores:    []*Restore{},
	}
	log.Debugf("new backup initiated: %#v", b)
	return b
}

func GetBackupBuckets() (bucket []Bucket) {

	for _, pvc := range k8s.GetPvcs().Items {

		if !CapsuleEnabledPvc(pvc.Annotations) {
			continue
		}

		ds := NewDiscoveryInputs(pvc.Annotations, pvc.Name, pvc.Namespace)
		if ds == nil {
			log.Error("empty discover inputs, validate pvc annotations, skipping backup ")
			continue
		}

		if !ShouldBackup(ds) {
			continue // backup not required, either backup disabled or the ns is blocked for backups
		}

		// discover destination bucket
		b, err := NewBucketWithAutoDiscovery(ds.PvcNamespace, ds.BucketRefSecret)
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

			for _, pvc := range k8s.GetPvcs().Items {

				if !CapsuleEnabledPvc(pvc.Annotations) {
					continue
				}

				ds := NewDiscoveryInputs(pvc.Annotations, pvc.Name, pvc.Namespace)
				if ds == nil {
					log.Error("empty discover inputs, validate pvc annotations, skipping backup ")
					continue
				}

				if !ShouldBackup(ds) {
					continue // backup not required, either backup disabled or the ns is blocked for backups
				}
				_ = discoverPgBackups(ds)

			}

			time.Sleep(60 * time.Second)
		}
	}
}

func discoverPgBackups(ds *DiscoveryInputs) error {

	//discover pg creds
	pgCreds, err := NewPgCredsWithAutoDiscovery(ds.PvcNamespace, ds.CredsRefSecret)
	if err != nil {
		return err
	}
	// discover destination bucket
	bucket, err := NewBucketWithAutoDiscovery(ds.PvcNamespace, ds.BucketRefSecret)
	if err != nil {
		return err
	}
	// create backup request
	period := ds.Period
	rotation := ds.Rotation
	backupServiceName := fmt.Sprintf("%s/%s", ds.PvcNamespace, ds.PvcName)
	pgBackupService := NewPgBackupService(backupServiceName, *pgCreds)
	backup := NewBackup(bucket, pgBackupService, period, rotation)
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
		rotateBackups(pgBackups)
		for _, pgBackup := range bucket.ScanBucket(PgService) {
			go pgBackup.backup()
		}
	}
}

func rotateBackups(backups []*Backup) bool {
	backupCount := len(backups)

	// nothing to rotate when no backups exists
	if backupCount == 0 {
		log.Info("pg backups list is 0, skipping rotation")
		return false
	}

	// calculate success backups
	successBackups := 0
	for _, backup := range backups {
		if backup.Status == Finished {
			successBackups++
		}
	}

	// rotation not needed yet
	if successBackups <= backups[0].Rotation {
		log.Infof("in bucket: %s, max rotation not reached yet (current: %d), skipping rotation", backups[0].Bucket.BucketId(), backupCount)
		return false
	}
	log.Infof("in bucket: %s, max rotation has been reached, rotating...", backups[0].Bucket.BucketId())

	// remove the oldest backup
	oldestBackup := backups[backupCount-1]
	if err := oldestBackup.Bucket.Remove(oldestBackup.BackupId); err != nil {
		return false
	}
	return true
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

func CapsuleEnabledPvc(pvcAnnotations map[string]string) bool {
	_, capsuleEnabledForPvc := pvcAnnotations[string(BackupEnabledAnnotation)]
	return capsuleEnabledForPvc
}
