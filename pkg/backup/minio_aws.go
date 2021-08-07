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
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	MinioBucketType BucketType = "minio"
	AwsBucketType   BucketType = "aws"
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

func (m *MinioBucket) SyncMetadataState(state, objectName string) error {
	mc := GetMinioClient(m)
	po := minio.PutObjectOptions{ContentType: "application/octet-stream"}
	f := strings.NewReader(state)
	fullObjectName := fmt.Sprintf("%s/%s", m.GetDstDir(), objectName)
	_, err := mc.PutObject(context.Background(), m.Bucket, fullObjectName, f, f.Size(), po)
	if err != nil {
		log.Errorf("error saving object: %s to S3, err: %s", objectName, err)
		return err
	}
	log.Infof("backup state synchronized: %v", objectName)
	return nil
}

func (m *MinioBucket) GetDstDir() string {
	return getDestinationDir(m.DstDir)
}

func (m *MinioBucket) BucketId() string {
	return m.Id
}

func (m *MinioBucket) Remove(backupId string) error {

	mc := GetMinioClient(m)
	prefix := fmt.Sprintf("%s/%s", m.GetDstDir(), backupId)
	lo := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
	objectCh := mc.ListObjects(context.Background(), m.Bucket, lo)
	for object := range objectCh {
		if object.Err != nil {
			log.Errorf("error listing backups in: %s , err: %s ", m.Id, object.Err)
			return nil
		}
		log.Infof("removing: %s", object.Key)
		opts := minio.RemoveObjectOptions{GovernanceBypass: false}
		err := mc.RemoveObject(context.Background(), m.Bucket, object.Key, opts)
		if err != nil {
			log.Error(err)
			return err
		}
	}
	return nil
}

func (m *MinioBucket) ScanBucket(o *ScanBucketOptions) []*Backup {
	log.Infof("scanning bucket for serviceType: %s", o.ServiceType)
	var backups []*Backup
	mc := GetMinioClient(m)
	lo := minio.ListObjectsOptions{Prefix: m.GetDstDir(), Recursive: true}
	objectCh := mc.ListObjects(context.Background(), m.Bucket, lo)
	for object := range objectCh {
		if object.Err != nil {
			log.Errorf("error listing backups in: %s , err: %s ", m.Id, object.Err)
			return nil
		}
		// only index files required for bucket scan
		if strings.Contains(object.Key, Statefile) {
			stream, err := mc.GetObject(context.Background(), m.Bucket, object.Key, minio.GetObjectOptions{})
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
			if o.matchBackup(backup) {
				backups = append(backups, &backup)
			}
		}
	}
	sort.Slice(backups, func(i, j int) bool { return backups[i].Date.After(backups[j].Date) })
	return backups
}

func (m *MinioBucket) UploadFile(path, objectName string) error {

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
	mc := GetMinioClient(m)
	fullObjectName := fmt.Sprintf("%s/%s", m.GetDstDir(), objectName)
	uploadInfo, err := mc.PutObject(context.Background(), m.Bucket, fullObjectName, file, fileStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		log.Error(err)
		return err
	}
	log.Infof("successfully uploaded DB dump: %s, size: %d", path, uploadInfo.Size)
	return nil
}

func (m *MinioBucket) DownloadFile(objectName, localFile string) error {
	log.Infof("downloading %s into %s", objectName, localFile)
	mc := GetMinioClient(m)
	fullObjectName := fmt.Sprintf("%s/%s", m.GetDstDir(), objectName)
	stream, err := mc.GetObject(context.Background(), m.Bucket, fullObjectName, minio.GetObjectOptions{})
	if err != nil {
		log.Error(err)
		return err
	}
	file, err := os.Create(localFile)
	if err != nil {
		log.Errorf("can't open dump file: %s, err: %s", localFile, err)
		return err
	}
	_, err = io.Copy(file, stream)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (m *MinioBucket) Ping() error {
	expectedHash, _ := shortid.Generate()
	if err := m.SyncMetadataState(expectedHash, "ping"); err != nil {
		return err
	}
	mc := GetMinioClient(m)
	objectName := fmt.Sprintf("%s/ping", m.GetDstDir())
	stream, err := mc.GetObject(context.Background(), m.Bucket, objectName, minio.GetObjectOptions{})
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
		return &BucketPingFailure{BucketId: m.Id, ActualPingHash: actualHash, ExpectedPingHash: expectedHash}
	}
	return nil
}

func (m *MinioBucket) BucketType() BucketType {
	if m.Endpoint == "s3.amazonaws.com" {
		return AwsBucketType
	}
	return MinioBucketType
}

func GetMinioClient(mb *MinioBucket) *minio.Client {
	if mb.Endpoint == "s3.amazonaws.com" && mb.AccessKey == "" && mb.SecretKey == "" {
		iam := credentials.NewIAM("")
		connOptions := &minio.Options{Creds: iam, Secure: mb.UseSSL, Region: mb.Region}
		mc, err := minio.New(mb.Endpoint, connOptions)
		if err != nil {
			log.Fatal(err)
		}
		return mc
	} else {
		tlsConfig := &tls.Config{InsecureSkipVerify: true}
		var transport http.RoundTripper = &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           (&net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
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
}

func NewMinioBucket(endpoint, region, accessKey, secretKey, bucket, dstDir string) *MinioBucket {

	if strings.Contains(endpoint, "https://") {
		endpoint = strings.TrimPrefix(endpoint, "https://")
	}

	if strings.Contains(endpoint, "http://") {
		endpoint = strings.TrimPrefix(endpoint, "http://")
	}

	return &MinioBucket{
		Id:        getBucketId(MinioBucketType, endpoint, bucket),
		Endpoint:  endpoint,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    strings.Contains(endpoint, "https://"),
		Bucket:    bucket,
		DstDir:    getDestinationDir(dstDir),
	}
}

func NewAwsBucket(region, accessKey, secretKey, bucket, dstDir string) *MinioBucket {
	endpoint := "s3.amazonaws.com"
	return &MinioBucket{
		Id:        getBucketId(AwsBucketType, endpoint, bucket),
		Endpoint:  endpoint,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    true,
		Bucket:    bucket,
		DstDir:    getDestinationDir(dstDir),
	}
}

func NewAwsIamBucket(region, bucket, dstDir string) *MinioBucket {
	endpoint := "s3.amazonaws.com"
	return &MinioBucket{
		Id:        getBucketId(AwsBucketType, endpoint, bucket),
		Endpoint:  endpoint,
		Region:    region,
		AccessKey: "",
		SecretKey: "",
		UseSSL:    true,
		Bucket:    bucket,
		DstDir:    getDestinationDir(dstDir),
	}
}
