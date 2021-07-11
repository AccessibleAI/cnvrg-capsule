package types

import "fmt"

type BucketDoeNotExists struct {
	BucketName string `json:"bucketName"`
	Message    string `json:"message"`
}

func (e *BucketDoeNotExists) Error() string {
	return fmt.Sprintf("bucket name %s does not exists", e.BucketName)
}
