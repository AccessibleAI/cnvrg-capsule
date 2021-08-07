package backup

import (
	"cloud.google.com/go/storage"
	"context"
	"encoding/json"
	"fmt"
	"github.com/teris-io/shortid"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)

type GcpBucket struct {
	Id        string `json:"id"`
	KeyJson   string `json:"keyJson"`
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
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(g.KeyJson)))
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
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(g.KeyJson)))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()
	prefix := fmt.Sprintf("%s/%s", g.GetDstDir(), backupId)
	q := &storage.Query{Prefix: prefix}
	it := client.Bucket(g.Bucket).Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error(err)
			return err
		}
		o := client.Bucket(g.Bucket).Object(attrs.Name)
		if err := o.Delete(ctx); err != nil {
			log.Error(err)
			return err
		}
	}
	return nil
}

func (g *GcpBucket) UploadFile(path, objectName string) error {

	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(g.KeyJson)))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()
	file, err := os.Open(path)
	if err != nil {
		log.Errorf("can't open dump file: %s, err: %s", path, err)
		return err
	}
	defer file.Close()

	fullObjectName := fmt.Sprintf("%s/%s", g.GetDstDir(), objectName)
	wc := client.Bucket(g.Bucket).Object(fullObjectName).NewWriter(ctx)
	if _, err = io.Copy(wc, file); err != nil {
		log.Error(err)
		return err
	}
	if err := wc.Close(); err != nil {
		log.Error(err)
		return err
	}
	log.Infof("successfully uploaded: %s", path)
	return nil
}

func (g *GcpBucket) DownloadFile(objectName, localFile string) error {

	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(g.KeyJson)))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()
	fullObjectName := fmt.Sprintf("%s/%s", g.GetDstDir(), objectName)
	rc, err := client.Bucket(g.Bucket).Object(fullObjectName).NewReader(ctx)
	if err != nil {
		log.Error(err)
		return err
	}
	defer rc.Close()
	file, err := os.Create(localFile)
	if err != nil {
		log.Errorf("can't open dump file: %s, err: %s", localFile, err)
		return err
	}
	_, err = io.Copy(file, rc)
	if err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (g *GcpBucket) ScanBucket(o *ScanBucketOptions) []*Backup {

	log.Infof("scanning bucket for serviceType: %s", o.ServiceType)
	var backups []*Backup
	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(g.KeyJson)))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()
	q := &storage.Query{Prefix: g.GetDstDir()}
	it := client.Bucket(g.Bucket).Objects(ctx, q)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Error(err)
			return nil
		}
		if strings.Contains(attrs.Name, Statefile) {
			rc, err := client.Bucket(g.Bucket).Object(attrs.Name).NewReader(ctx)
			if err != nil {
				log.Error(err)
				continue
			}
			data, err := ioutil.ReadAll(rc)
			if err != nil {
				log.Error(err)
				continue
			}
			rc.Close()
			backup := Backup{}
			if err := json.Unmarshal(data, &backup); err != nil {
				log.Errorf("error unmarshal Backup request, object: %s, err: %s", attrs.Name, err)
				continue
			}
			if o.haveServiceType(backup.ServiceType) &&
				o.haveRequestType(backup.RequestType) &&
				o.matchStatefileVersion(backup.StatefileVersion) {
				backups = append(backups, &backup)
			}
		}
	}
	sort.Slice(backups, func(i, j int) bool { return backups[i].Date.After(backups[j].Date) })
	return backups
}

func (g *GcpBucket) SyncMetadataState(state, objectName string) error {

	ctx := context.Background()
	client, err := storage.NewClient(ctx, option.WithCredentialsJSON([]byte(g.KeyJson)))
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()
	f := strings.NewReader(state)
	fullObjectName := fmt.Sprintf("%s/%s", g.GetDstDir(), objectName)
	wc := client.Bucket(g.Bucket).Object(fullObjectName).NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		log.Error(err)
		return err
	}
	if err := wc.Close(); err != nil {
		log.Error(err)
		return err
	}
	log.Infof("backup state synchronized: %v", objectName)
	return nil
}

func NewGcpBucket(keyJons, projectId, bucket, dstDir string) *GcpBucket {
	return &GcpBucket{
		Id:        getBucketId(GcpBucketType, projectId, bucket),
		KeyJson:   keyJons,
		ProjectId: projectId,
		Bucket:    bucket,
		DstDir:    getDestinationDir(dstDir),
	}
}
