package apiserver

import (
	"github.com/AccessibleAI/cnvrg-capsule/pkg/backup"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func RunApi() {
	log.Info("starting api service...")
	r := gin.New()

	ginLog := logrus.New()
	ginLog.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	r.Use(Logger(), gin.Recovery())
	r.GET("/v1/pg/backups", ListBackups)
	if err := r.Run(); err != nil {
		logrus.Fatalf("error starting api server, %s", err)
	}
}

func ListBackups(c *gin.Context) {
	var backups []*backup.Backup
	for _, bucket := range backup.GetBackupBuckets() {
		backups = append(backups, bucket.ScanBucket(backup.NewAllV1Alpha1ScanOptions())...)
	}
	c.JSON(200, gin.H{"backups": backups})
}
