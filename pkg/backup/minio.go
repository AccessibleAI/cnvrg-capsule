package backup

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

type MinioBucket struct {
	Id        string `json:"id"`
	Endpoint  string `json:"endpoint"`
	Region    string `json:"region"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
	UseSSL    bool   `json:"useSSL"`
	Bucket    string `json:"bucket"`
	DstDir    string `json:"dstDir"`
}

func (mb *MinioBucket) SyncMetadataState(state, objectName string) error {
	mc := GetMinioClient(mb.Endpoint, mb.AccessKey, mb.SecretKey, mb.UseSSL)
	po := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	f := strings.NewReader(state)
	_, err := mc.PutObject(context.Background(), mb.Bucket, objectName, f, f.Size(), po)
	if err != nil {
		log.Errorf("error saving object: %s to S3, err: %s", objectName, err)
		return err
	}
	log.Infof("backup state synchronized: %v", objectName)
	return nil
}

func (mb *MinioBucket) GetDstDir() string {
	if mb.DstDir == "" {
		return "cnvrg-smart-backups"
	}
	return mb.DstDir
}

func (mb *MinioBucket) GetBucketName() string {
	return mb.Bucket
}

func (mb *MinioBucket) Remove(objectName string) error {
	opts := minio.RemoveObjectOptions{GovernanceBypass: false}
	mc := GetMinioClient(mb.Endpoint, mb.AccessKey, mb.SecretKey, mb.UseSSL)
	err := mc.RemoveObject(context.Background(), mb.Bucket, objectName, opts)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (mb *MinioBucket) RotateBackups() bool {
	backups := mb.ScanBucket()
	backupCount := len(backups)

	// nothing to rotate when no backups exists
	if backupCount == 0 {
		log.Infof("in bucket: %s, pg backups list is 0, skipping rotation", mb.Id)
		return false
	}

	// rotation not needed yet
	if backupCount <= backups[0].Rotation {
		log.Infof("in bucket: %s, max rotation not reached yet (current: %d), skipping rotation", mb.Id, backupCount)
		return false
	}
	log.Infof("in bucket: %s, max rotation has been reached, rotating...", mb.Id)

	// remove the oldest backup
	return backups[backupCount-1].remove()
}

func (mb *MinioBucket) ScanBucket() []*PgBackup {
	var pgBackups []*PgBackup
	lo := minio.ListObjectsOptions{Prefix: mb.GetDstDir(), Recursive: true}
	mc := GetMinioClient(mb.Endpoint, mb.AccessKey, mb.SecretKey, mb.UseSSL)
	objectCh := mc.ListObjects(context.Background(), mb.Bucket, lo)
	for object := range objectCh {
		if object.Err != nil {
			log.Errorf("error listing backups in: %s , err: %s ", mb.Id, object.Err)
			return nil
		}
		// only index files required for bucket scan
		if strings.Contains(object.Key, IndexfileTag) {
			stream, err := mc.GetObject(context.Background(), mb.Bucket, object.Key, minio.GetObjectOptions{})
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

func (mb *MinioBucket) UploadFile(path, objectName string) error {

	file, err := os.Open(path)
	if err != nil {
		log.Errorf("can't open dump file: %s, err: %s", path, err)
		return err
	}
	defer file.Close()
	fileStat, err := file.Stat()
	if err != nil {
		log.Errorf("can't open dump file: %s, err: %s", path, err)
		return err
	}
	mc := GetMinioClient(mb.Endpoint, mb.AccessKey, mb.SecretKey, mb.UseSSL)
	uploadInfo, err := mc.PutObject(context.Background(), mb.Bucket, objectName, file, fileStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		log.Error(err)
		return err
	}
	log.Infof("successfully uploaded DB dump: %s, size: %d", path, uploadInfo.Size)
	return nil
}

func GetMinioClient(endpoint, accessKey, secretKey string, secure bool) *minio.Client {
	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	var transport http.RoundTripper = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig:       tlsConfig,
		DisableCompression:    true,
	}
	connOptions := &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: secure, Transport: transport}
	mc, err := minio.New(endpoint, connOptions)
	if err != nil {
		log.Fatal(err)
	}
	return mc
}

func NewMinioBackupBucket(endpoint, region, accessKey, secretKey, bucket, dstDir string) *MinioBucket {

	if strings.Contains(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
	}

	if strings.Contains(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
	}

	if dstDir == "" {
		dstDir = "cnvrg-smart-backups"
	}

	return &MinioBucket{
		Id:        fmt.Sprintf("minio-%s-%s", endpoint, bucket),
		Endpoint:  endpoint,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    strings.Contains(endpoint, "https://"),
		Bucket:    bucket,
	}
}
