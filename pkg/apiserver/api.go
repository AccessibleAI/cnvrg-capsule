package apiserver

import (
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

//type Payload struct {
//	BackupType backup.BackupType `uri:"backupType" binding:"required,backupabletype"`
//}

//var backupableType validator.Func = func(fl validator.FieldLevel) bool {
//	backupType, ok := fl.Field().Interface().(backup.BackupType)
//	if ok {
//		//return backup.ValidateBackupType(backupType)
//		log.Info(backupType)
//		return true
//	}
//	return false
//}

func RunApi() {
	log.Info("starting api service...")
	r := gin.New()
	//if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		//if err := v.RegisterValidation("backupabletype", backupableType); err != nil {
		//	logrus.Fatal(err)
		//}
	//}
	ginLog := logrus.New()
	ginLog.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	r.Use(Logger(), gin.Recovery())
	r.POST("/v1/backup/:backupType", CreateBackup)
	logrus.Info("starting api server ... ")
	if err := r.Run(); err != nil {
		logrus.Fatalf("error starting api server, %s", err)
	}
}

func CreateBackup(c *gin.Context) {
	//var payload Payload
	//if err := c.ShouldBindUri(&payload); err != nil {
	//	log.Error(err)
	//	c.JSON(400, gin.H{"error": err.Error()})
	//	return
	//}
	//b := backup.New(payload.BackupType)
	//if err := b.CreateBackupRequest(); err != nil {
	//	c.JSON(400, gin.H{"error": err})
	//} else {
	//	c.JSON(200, gin.H{})
	//}

}
