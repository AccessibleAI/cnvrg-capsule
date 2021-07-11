package types

import "fmt"

type BucketDoesNotExists struct {
	BucketName string `json:"bucketName"`
	Message    string `json:"message"`
}

func (e *BucketDoesNotExists) Error() string {
	return fmt.Sprintf("bucket name %s does not exists", e.BucketName)
}

type RequiredKeyIsMissing struct {
	ObjectName string `json:"objectName"`
	Key        string `json:"key"`
}

func (e *RequiredKeyIsMissing) Error() string {
	return fmt.Sprintf("key: %s is missing in %s", e.Key, e.ObjectName)
}
