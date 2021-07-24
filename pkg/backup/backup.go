package backup

import (
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"sync"
	"time"
)

type Backup struct {
	BackupId    string      `json:"backupId"`
	Rotation    int         `json:"rotation"`
	Date        time.Time   `json:"date"`
	Period      float64     `json:"period"`
	Bucket      Bucket      `json:"bucket"`
	ServiceType ServiceType `json:"serviceType"`
	Service     Service     `json:"service"`
	Status      Status      `json:"status"`
}

var (
	log                = logrus.WithField("module", "backup-engine")
	mutex              = sync.Mutex{}
	BucketsToWatchChan = make(chan Bucket, 100)
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
	//if viper.GetBool("auto-discovery") {
	//	for {
	//		// get all cnvrg apps
	//		apps := k8s.GetCnvrgApps()
	//		for _, app := range apps.Items {
	//			// make sure backups enabled
	//			if !ShouldBackup(app) {
	//				continue // backup not required, either backup disabled or the ns is blocked for backups
	//			}
	//			// discover pg creds
	//			//pgCreds, err := NewPgCredsWithAutoDiscovery(app.Spec.Dbs.Pg.Backup.CredsRef, app.Namespace)
	//			//if err != nil {
	//			//	return
	//			//}
	//			// discover destination bucket
	//			bucket, err := NewBackupBucketWithAutoDiscovery(app.Spec.Dbs.Pg.Backup.BucketRef, app.Namespace)
	//			if err != nil {
	//				return
	//			}
	//
	//			// create backup request
	//			idPrefix := fmt.Sprintf("%s-%s", app.Name, app.Namespace)
	//			period := app.Spec.Dbs.Pg.Backup.Period
	//			rotation := app.Spec.Dbs.Pg.Backup.Rotation
	//			pgBackupService := PgBackupService{}
	//			backup := NewBackup(idPrefix, period, rotation, bucket, &pgBackupService)
	//
	//			//backup := NewPgBackup(idPrefix, period, rotation, bucket, *pgCreds)
	//			if err := backup.createBackupRequest(); err != nil {
	//				log.Errorf("error creating backup request, err: %s", err)
	//			}
	//		}
	//		time.Sleep(60 * time.Second)
	//	}
	//}
}

func GetBackupBuckets() (bucket []Bucket) {
	apps := k8s.GetCnvrgApps()
	for _, app := range apps.Items {
		// make sure backups enabled
		if !ShouldBackup(app) {
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
		bucket.RotateBackups(PgServiceType)
		for _, pgBackup := range bucket.ScanBucket(PgServiceType) {
			go pgBackup.backup()
		}
	}
}

//func (b *Bucket) getMinioClient() *minio.Client {
//
//	// skip TLS verify
//	tlsConfig := &tls.Config{InsecureSkipVerify: true}
//	var transport http.RoundTripper = &http.Transport{
//		Proxy: http.ProxyFromEnvironment,
//		DialContext: (&net.Dialer{
//			Timeout:   30 * time.Second,
//			KeepAlive: 30 * time.Second,
//		}).DialContext,
//		MaxIdleConns:          100,
//		IdleConnTimeout:       90 * time.Second,
//		TLSHandshakeTimeout:   10 * time.Second,
//		ExpectContinueTimeout: 1 * time.Second,
//		TLSClientConfig:       tlsConfig,
//		DisableCompression:    true,
//	}
//	if b.BucketType == MinioBucket {
//		connOptions := &minio.Options{Creds: credentials.NewStaticV4(b.AccessKey, b.SecretKey, ""), Secure: b.UseSSL, Transport: transport}
//		mc, err := minio.New(b.Endpoint, connOptions)
//		if err != nil {
//			log.Fatal(err)
//		}
//		return mc
//	}
//	if b.BucketType == AwsBucket {
//		if b.AccessKey == "" && b.SecretKey == "" {
//			iam := credentials.NewIAM("")
//			connOptions := &minio.Options{Creds: iam, Secure: true, Region: b.Region, Transport: transport}
//			mc, err := minio.New("s3.amazonaws.com", connOptions)
//			if err != nil {
//				log.Fatal(err)
//			}
//			return mc
//		} else {
//			creds := credentials.NewStaticV4(b.AccessKey, b.SecretKey, "")
//			connOptions := &minio.Options{Creds: creds, Secure: true, Region: b.Region, Transport: transport}
//			mc, err := minio.New("s3.amazonaws.com", connOptions)
//			if err != nil {
//				log.Fatal(err)
//			}
//			return mc
//		}
//	}
//	log.Fatalf("not supported bucket type: %s", b.BucketType)
//	return nil
//}
