package backup

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/teris-io/shortid"
	"io"
	"io/ioutil"
	"strings"
	"time"
)

type GcpBucket struct {
	Id        string `json:"id"`
	ProjectId string `json:"projectId"`
	Bucket    string `json:"bucket"`
	DstDir    string `json:"dstDir"`
}

const (
	GcpBucketType BucketType = "gcp"
)

func (g *GcpBucket) Ping() error {

	ctx := context.Background()
	expectedHash, _ := shortid.Generate()
	fullObjectName := fmt.Sprintf("%s/%s", g.GetDstDir(), "ping")
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()
	f := strings.NewReader(expectedHash)
	wc := client.Bucket(g.Bucket).Object(fullObjectName).NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		log.Error(err)
		return err
	}
	if err := wc.Close(); err != nil {
		log.Error(err)
		return err
	}
	rc, err := client.Bucket(g.Bucket).Object(fullObjectName).NewReader(ctx)
	if err != nil {
		log.Error(err)
		return err
	}
	defer rc.Close()

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		log.Error(err)
		return err
	}
	actualHash := string(data)
	if actualHash != expectedHash {
		return &BucketPingFailure{BucketId: g.Id, ActualPingHash: actualHash, ExpectedPingHash: expectedHash}
	}
	return nil

}

func (g *GcpBucket) BucketId() string {
	return g.Id
}

func (g *GcpBucket) GetDstDir() string {
	return getDestinationDir(g.DstDir)
}

func (g *GcpBucket) BucketType() BucketType {
	return GcpBucketType
}

func (g *GcpBucket) Remove(backupId string) error {

	return nil
}

func (g *GcpBucket) UploadFile(path, objectName string) error {

	return nil
}

func (g *GcpBucket) DownloadFile(objectName, localFile string) error {
	return nil
}

func (g *GcpBucket) ScanBucket(serviceType ServiceType) []*Backup {

	return nil
}

func (g *GcpBucket) SyncMetadataState(state, objectName string) error {

	return nil
}

func NewGcpBucket(projectId, bucket, dstDir string) *GcpBucket {
	return &GcpBucket{
		Id:     getBucketId(GcpBucketType, "gcp", bucket),
		Bucket: bucket,
		DstDir: getDestinationDir(dstDir),
	}
}
