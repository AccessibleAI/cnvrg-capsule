package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/teris-io/shortid"
	"net/url"
	"os"
	"sort"
	"strings"
)

type AzureBucket struct {
	Id          string `json:"id"`
	AccountName string `json:"accountName"`
	AccountKey  string `json:"accountKey"`
	Bucket      string `json:"bucket"`
	DstDir      string `json:"dstDir"`
}

const (
	AzureBucketType BucketType = "azure"
)

func (a *AzureBucket) Ping() error {
	containerURL, err := getContainerUrl(a)
	if err != nil {
		return err
	}
	objectName := "ping"
	expectedHash, _ := shortid.Generate()
	f := strings.NewReader(expectedHash)
	fullObjectName := fmt.Sprintf("%s/%s", a.GetDstDir(), objectName)
	blobURL := containerURL.NewBlockBlobURL(fullObjectName)

	options := azblob.UploadStreamToBlockBlobOptions{BufferSize: 2 * 1024 * 1024, MaxBuffers: 3}
	_, err = azblob.UploadStreamToBlockBlob(context.Background(), f, blobURL, options)
	if err != nil {
		log.Errorf("error uploaind bolb to azure storage, err: %s", err)
		return err
	}
	cpk := azblob.ClientProvidedKeyOptions{}
	dr, err := blobURL.Download(context.Background(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, cpk)
	if err != nil {
		log.Error(err)
		return err
	}
	stream := dr.Body(azblob.RetryReaderOptions{MaxRetryRequests: 3})
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(stream); err != nil {
		log.Errorf("ping failed, err: %s", err)
		return err
	}
	actualHash := string(buf.Bytes())
	if actualHash != expectedHash {
		return &BucketPingFailure{BucketId: a.Id, ActualPingHash: actualHash, ExpectedPingHash: expectedHash}
	}
	return nil
}

func (a *AzureBucket) BucketId() string {
	return a.Id
}

func (a *AzureBucket) GetDstDir() string {
	return getDestinationDir(a.DstDir)
}

func (a *AzureBucket) BucketType() BucketType {
	return AzureBucketType
}

func (a *AzureBucket) Remove(backupId string) error {

	containerURL, err := getContainerUrl(a)
	if err != nil {
		return nil
	}
	prefix := fmt.Sprintf("%s/%s", a.GetDstDir(), backupId)
	lo := azblob.ListBlobsSegmentOptions{Prefix: prefix}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := containerURL.ListBlobsFlatSegment(context.Background(), marker, lo)
		if err != nil {
			log.Error(err)
			return nil
		}
		marker = listBlob.NextMarker
		for _, object := range listBlob.Segment.BlobItems {
			blobURL := containerURL.NewBlockBlobURL(object.Name)
			_, err := blobURL.Delete(context.Background(), azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
			if err != nil {
				log.Error(err)
				return err
			}
		}
	}
	return nil
}

func (a *AzureBucket) UploadFile(path, objectName string) error {
	containerURL, err := getContainerUrl(a)
	if err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		log.Errorf("can't open dump file: %s, err: %s", path, err)
		return err
	}
	defer file.Close()
	fullObjectName := fmt.Sprintf("%s/%s", a.GetDstDir(), objectName)
	blobURL := containerURL.NewBlockBlobURL(fullObjectName)
	uo := azblob.UploadToBlockBlobOptions{BlockSize: 4 * 1024 * 1024, Parallelism: 16}
	_, err = azblob.UploadFileToBlockBlob(context.Background(), file, blobURL, uo)
	return err
}

func (a *AzureBucket) DownloadFile(objectName, localFile string) error {
	log.Infof("downloading %s into %s", objectName, localFile)
	containerURL, err := getContainerUrl(a)
	if err != nil {
		return err
	}
	do := azblob.DownloadFromBlobOptions{}
	fullObjectName := fmt.Sprintf("%s/%s", a.GetDstDir(), objectName)
	blobURL := containerURL.NewBlobURL(fullObjectName)
	file, err := os.Create(localFile)
	if err != nil {
		log.Errorf("can't open dump file: %s, err: %s", localFile, err)
		return err
	}
	defer file.Close()
	return azblob.DownloadBlobToFile(context.Background(), blobURL, 0, 0, file, do)
}

func (a *AzureBucket) ScanBucket(o *ScanBucketOptions) []*Backup {
	log.Infof("scanning bucket for serviceType: %s", o.ServiceType)
	var backups []*Backup
	containerURL, err := getContainerUrl(a)
	if err != nil {
		return nil
	}
	lo := azblob.ListBlobsSegmentOptions{Prefix: a.GetDstDir()}
	for marker := (azblob.Marker{}); marker.NotDone(); {
		listBlob, err := containerURL.ListBlobsFlatSegment(context.Background(), marker, lo)
		if err != nil {
			log.Error(err)
			return nil
		}
		marker = listBlob.NextMarker
		for _, object := range listBlob.Segment.BlobItems {
			if strings.Contains(object.Name, Statefile) {
				blobURL := containerURL.NewBlockBlobURL(object.Name)
				dr, err := blobURL.Download(context.Background(), 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false, azblob.ClientProvidedKeyOptions{})
				if err != nil {
					log.Error(err)
					continue
				}
				stream := dr.Body(azblob.RetryReaderOptions{MaxRetryRequests: 3})
				buf := new(bytes.Buffer)
				if _, err := buf.ReadFrom(stream); err != nil {
					log.Errorf("error reading stream, err: %s", err)
					continue
				}
				backup := Backup{}
				if err := json.Unmarshal(buf.Bytes(), &backup); err != nil {
					log.Errorf("error unmarshal Backup request, object: %s, err: %s", object.Name, err)
					continue
				}
				if o.haveServiceType(backup.ServiceType) && o.haveRequestType(backup.RequestType) {
					backups = append(backups, &backup)
				}
			}
		}
	}
	sort.Slice(backups, func(i, j int) bool { return backups[i].Date.After(backups[j].Date) })
	return backups
}

func (a *AzureBucket) SyncMetadataState(state, objectName string) error {
	credential, err := azblob.NewSharedKeyCredential(a.AccountName, a.AccountKey)
	if err != nil {
		log.Errorf("error saving object: %s to S3, err: %s", objectName, err)
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", a.AccountName, a.Bucket))
	containerURL := azblob.NewContainerURL(*URL, p)
	f := strings.NewReader(state)
	fullObjectName := fmt.Sprintf("%s/%s", a.GetDstDir(), objectName)
	blobURL := containerURL.NewBlockBlobURL(fullObjectName)

	options := azblob.UploadStreamToBlockBlobOptions{BufferSize: 2 * 1024 * 1024, MaxBuffers: 3}
	_, err = azblob.UploadStreamToBlockBlob(context.Background(), f, blobURL, options)
	if err != nil {
		log.Errorf("error uploaind bolb to azure storage, err: %s", err)
		return err
	}
	return nil
}

func getContainerUrl(a *AzureBucket) (*azblob.ContainerURL, error) {

	credential, err := azblob.NewSharedKeyCredential(a.AccountName, a.AccountKey)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", a.AccountName, a.Bucket))
	containerURL := azblob.NewContainerURL(*URL, p)
	return &containerURL, nil
}

func NewAzureBucket(accountName, accountKey, bucket, dstDir string) *AzureBucket {
	return &AzureBucket{
		Id:          getBucketId(AzureBucketType, "blob.core.windows.net", bucket),
		AccountName: accountName,
		AccountKey:  accountKey,
		Bucket:      bucket,
		DstDir:      getDestinationDir(dstDir),
	}
}
