package backup

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/teris-io/shortid"
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
	mc := GetMinioClient(mb)
	po := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	f := strings.NewReader(state)
	fullObjectName := fmt.Sprintf("%s/%s", mb.GetDstDir(), objectName)
	_, err := mc.PutObject(context.Background(), mb.Bucket, fullObjectName, f, f.Size(), po)
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

func (mb *MinioBucket) BucketId() string {
	return mb.Id
}

func (mb *MinioBucket) Remove(backupDirName string) error {

	mc := GetMinioClient(mb)
	prefix := fmt.Sprintf("%s/%s", mb.GetDstDir(), backupDirName)
	lo := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
	objectCh := mc.ListObjects(context.Background(), mb.Bucket, lo)
	for object := range objectCh {
		if object.Err != nil {
			log.Errorf("error listing backups in: %s , err: %s ", mb.Id, object.Err)
			return nil
		}
		log.Infof("removing: %s", object.Key)
		opts := minio.RemoveObjectOptions{GovernanceBypass: false}
		err := mc.RemoveObject(context.Background(), mb.Bucket, object.Key, opts)
		if err != nil {
			log.Error(err)
			return err
		}
	}
	return nil
}

func (mb *MinioBucket) RotateBackups(backups []*Backup) bool {
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
	oldestBackup := backups[backupCount-1]
	if err := oldestBackup.Bucket.Remove(oldestBackup.BackupId); err != nil {
		return false
	}
	return true
}

func (mb *MinioBucket) ScanBucket(serviceType ServiceType) []*Backup {
	log.Infof("scanning bucket for serviceType: %s", serviceType)
	var backups []*Backup
	mc := GetMinioClient(mb)
	lo := minio.ListObjectsOptions{Prefix: mb.GetDstDir(), Recursive: true}
	objectCh := mc.ListObjects(context.Background(), mb.Bucket, lo)
	for object := range objectCh {
		if object.Err != nil {
			log.Errorf("error listing backups in: %s , err: %s ", mb.Id, object.Err)
			return nil
		}
		// only index files required for bucket scan
		if strings.Contains(object.Key, Indexfile) {
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
			backup := Backup{}
			if err := json.Unmarshal(buf.Bytes(), &backup); err != nil {
				log.Errorf("error unmarshal Backup request, object: %s, err: %s", object.Key, err)
				continue
			}
			if backup.ServiceType == serviceType {
				backups = append(backups, &backup)
			}
		}
	}
	sort.Slice(backups, func(i, j int) bool { return backups[i].Date.After(backups[j].Date) })
	return backups
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
	mc := GetMinioClient(mb)
	fullObjectName := fmt.Sprintf("%s/%s", mb.GetDstDir(), objectName)
	uploadInfo, err := mc.PutObject(context.Background(), mb.Bucket, fullObjectName, file, fileStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		log.Error(err)
		return err
	}
	log.Infof("successfully uploaded DB dump: %s, size: %d", path, uploadInfo.Size)
	return nil
}

func (mb *MinioBucket) Ping() error {
	expectedHash, _ := shortid.Generate()
	if err := mb.SyncMetadataState(expectedHash, "ping"); err != nil {
		return err
	}
	mc := GetMinioClient(mb)
	objectName := fmt.Sprintf("%s/ping", mb.GetDstDir())
	stream, err := mc.GetObject(context.Background(), mb.Bucket, objectName, minio.GetObjectOptions{})
	if err != nil {
		log.Errorf("ping failed, err: %s", err)
		return err
	}
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(stream); err != nil {
		log.Errorf("ping failed, err: %s", err)
		return err
	}
	actualHash := string(buf.Bytes())
	if actualHash != expectedHash {
		return &BucketPingFailure{BucketId: mb.Id, ActualPingHash: actualHash, ExpectedPingHash: expectedHash}
	}
	return nil
}

func GetMinioClient(mb *MinioBucket) *minio.Client {
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
	connOptions := &minio.Options{Creds: credentials.NewStaticV4(mb.AccessKey, mb.SecretKey, ""), Secure: mb.UseSSL, Transport: transport}
	mc, err := minio.New(mb.Endpoint, connOptions)
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
