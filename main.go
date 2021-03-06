package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/apiserver"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/backup"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	"github.com/briandowns/spinner"
	"github.com/manifoldco/promptui"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"strings"
	"time"
)

type param struct {
	name      string
	shorthand string
	value     interface{}
	usage     string
	required  bool
}

type RunMode string

const (
	AllInOne     RunMode = "allinone"
	Api          RunMode = "api"
	BackupEngine RunMode = "backup-engine"
)

var (
	Version     string
	Build       string
	cliPgParams = []param{
		{name: "id", shorthand: "", value: "", usage: "backup id"},
		{name: "list", shorthand: "l", value: false, usage: "list backups"},
		{name: "delete", shorthand: "", value: false, usage: "delete backup"},
		{name: "create", shorthand: "c", value: false, usage: "create backup manually"},
		{name: "download", shorthand: "d", value: false, usage: "download backup"},
		{name: "restore", shorthand: "r", value: false, usage: "request backup restore"},
		{name: "describe", shorthand: "", value: false, usage: "describe backup metadata"},
		{name: "description", shorthand: "", value: "", usage: "description for backup request"},
	}
	rootParams = []param{
		{name: "verbose", shorthand: "v", value: false, usage: "--verbose=true|false"},
		{name: "dumpdir", shorthand: "", value: "/tmp/capsule-data", usage: "place to download DB dumps before uploading to s3 bucket"},
		{name: "auto-discovery", shorthand: "", value: true, usage: "automatically discover backup buckets"},
		{name: "ns-whitelist", shorthand: "", value: "*", usage: "when auto-discovery is true, specify the namespaces list, by default lookup in all namespaces"},
		{name: "kubeconfig", shorthand: "", value: k8s.KubeconfigDefaultLocation(), usage: "absolute path to the kubeconfig file"},
		{name: "endpoint", shorthand: "", value: "", usage: "S3 end point"},
		{name: "region", shorthand: "", value: "", usage: "S3 region"},
		{name: "access-key", shorthand: "", value: "", usage: "S3 access key"},
		{name: "secret-key", shorthand: "", value: "", usage: "S3 secret key"},
		{name: "use-ssl", shorthand: "", value: false, usage: "use ssl to access to S3"},
		{name: "bucket-name", shorthand: "", value: "", usage: "bucket name with backups"},
		{name: "dst-dir", shorthand: "", value: "", usage: "bucket backups base directory"},
		{name: "account-name", shorthand: "", value: "", usage: "Azure account name"},
		{name: "account-key", shorthand: "", value: "", usage: "Azure account key"},
		{name: "key-json", shorthand: "", value: "", usage: "Key.json GCP credentials file path"},
		{name: "project-id", shorthand: "", value: "", usage: "GCP project id "},
		{name: "bucket-type", shorthand: "", value: "minio", usage:
		fmt.Sprintf("bucket type, one of: %s|%s|%s|%s", backup.MinioBucketType, backup.AwsBucketType, backup.AzureBucketType, backup.GcpBucketType)},
	}
	startCapsuleParams = []param{
		{name: "api-bind-addr", shorthand: "", value: ":8080", usage: "The address to bind to the api service."},
		{name: "mode", shorthand: "", value: "allinone", usage: fmt.Sprintf("%s|%s|%s", AllInOne, Api, BackupEngine)},
	}
)

var rootCmd = &cobra.Command{
	Use:   "capsule",
	Short: "capsule - Cnvrg backup and restore service",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		setupLogging()
		dumpDirPath := viper.GetString("dumpdir")
		if _, err := os.Stat(dumpDirPath); os.IsNotExist(err) {
			if err := os.Mkdir(dumpDirPath, os.ModePerm); err != nil {
				log.Error(err)
				os.Exit(1)
			}
		}
	},
}

var capsuleVersion = &cobra.Command{
	Use:   "version",
	Short: "Print capsule version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("???? version: %s build: %s \n", Version, Build)
	},
}

var startCapsule = &cobra.Command{
	Use:   "start",
	Short: fmt.Sprintf("Start the capsule backup service (by default mode=allinone, supported modes: %s|%s|%s)", AllInOne, Api, BackupEngine),
	Run: func(cmd *cobra.Command, args []string) {
		stopChan := make(chan bool)
		runMode := RunMode(viper.GetString("mode"))
		log.Infof("starting capsule, mode: %s", runMode)
		log.Infof("capsule version: %s, build: %s", Version, Build)
		if runMode == AllInOne || runMode == Api {
			go apiserver.RunApi()
		}
		if runMode == AllInOne || runMode == BackupEngine {
			go backup.Run()
		}
		<-stopChan
	},
}

var cliBackupPg = &cobra.Command{
	Use:   "pg",
	Short: "PostgreSQL backup and restore operations",
	Run: func(cmd *cobra.Command, args []string) {
		if viper.GetBool("list") {
			cliListBackups()
			return
		}
		if viper.GetBool("delete") {
			cliDeleteBackups()
			return
		}
		if viper.GetBool("create") {
			cliCreateBackup()
			return
		}
		if viper.GetBool("download") {
			cliDownloadBackup()
			return
		}
		if viper.GetBool("restore") {
			cliRestoreBackup()
			return
		}
		if viper.GetBool("describe") {
			cliDescribeBackup()
			return
		}
	},
}

func setParams(params []param, command *cobra.Command) {
	for _, param := range params {
		switch v := param.value.(type) {
		case int:
			command.PersistentFlags().IntP(param.name, param.shorthand, v, param.usage)
		case string:
			command.PersistentFlags().StringP(param.name, param.shorthand, v, param.usage)
		case bool:
			command.PersistentFlags().BoolP(param.name, param.shorthand, v, param.usage)
		}
		if err := viper.BindPFlag(param.name, command.PersistentFlags().Lookup(param.name)); err != nil {
			panic(err)
		}
	}
}

func setupCommands() {
	viper.AutomaticEnv()
	viper.SetEnvPrefix("CNVRG_CAPSULE")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	// Setup commands
	setParams(startCapsuleParams, startCapsule)
	setParams(rootParams, rootCmd)
	setParams(cliPgParams, cliBackupPg)
	rootCmd.AddCommand(cliBackupPg)
	rootCmd.AddCommand(startCapsule)
	rootCmd.AddCommand(capsuleVersion)

}

func cliListBackups() {
	var backups []*backup.Backup
	for _, bucket := range backup.GetBackupBuckets() {
		backups = append(backups, bucket.ScanBucket(backup.NewAllV1Alpha1ScanOptions())...)
		if len(backups) == 0 {
			log.Info("backup list is empty!")
			return
		}
		for _, b := range backups {
			formatted := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", b.Date.Year(), b.Date.Month(), b.Date.Day(), b.Date.Hour(), b.Date.Minute(), b.Date.Second())
			log.Infof("backup: %s, name: %s, date:%s, status: %s", b.BackupId, b.Service.GetName(), formatted, b.Status)
		}
	}
}

func cliDeleteBackups() {
	var confirmTemplate = &promptui.SelectTemplates{
		Label:    `{{ . }}?`,
		Active:   `> {{ . | red}}`,
		Inactive: `  {{ . | faint}} `,
		Selected: `> {{ . | red }}`,
	}

	b := selectBackup("????")
	if b == nil {
		os.Exit(1)
	}

	confirmDelete := promptui.Select{
		Label:     fmt.Sprintf("Deleting %s, are you sure?", b.BackupId),
		Items:     []string{"No", "Yes"},
		Templates: confirmTemplate,
	}
	_, confirm, err := confirmDelete.Run()
	if err != nil {
		log.Error(err)
		return
	}

	if confirm == "Yes" {
		if err := b.Bucket.Remove(b.BackupId); err != nil {
			log.Error(err)
			return
		}
		log.Infof("%s removed", b.BackupId)
	}
}

func cliCreateBackup() {
	selectBackupCreateTemplate := &promptui.SelectTemplates{
		Label:    "{{ . }}?",
		Active:   `> {{ printf "%s/%s" .PvcNamespace .PvcName | cyan }}`,
		Inactive: `  {{ printf "%s/%s" .PvcNamespace .PvcName | faint }}`,
		Selected: `> {{ printf "%s/%s" .PvcNamespace .PvcName | cyan }}`,
		Details: `
--------- Create Backup ----------
{{ "Type:" | faint }}	{{ .ServiceType }}
{{ "Pvc Name:" | faint }}	{{ .PvcName }}
{{ "Pvc Namespace:" | faint }}	{{ .PvcNamespace }}`,
	}

	var dsList []*backup.DiscoveryInputs
	for _, pvc := range k8s.GetPvcs().Items {
		if !backup.CapsuleEnabledPvc(pvc.Annotations) {
			continue
		}
		dsList = append(dsList, backup.NewDiscoveryInputs(pvc.Annotations, pvc.Name, pvc.Namespace))
	}

	backupCreateSelect := promptui.Select{
		Label:     "Select a backup",
		Items:     dsList,
		Size:      10,
		Templates: selectBackupCreateTemplate,
	}
	if len(dsList) == 0 {
		log.Info("none backups enabled for the current cluster")
		return
	}
	idx, _, err := backupCreateSelect.Run()
	if err != nil {
		log.Error(err)
		return
	}

	ds := dsList[idx]
	//discover pg creds
	pgCreds, err := backup.NewPgCredsWithAutoDiscovery(ds.PvcNamespace, ds.CredsRefSecret)
	if err != nil {
		log.Error(err)
		return
	}
	// discover destination bucket
	bucket, err := backup.NewBucketWithAutoDiscovery(ds.PvcNamespace, ds.BucketRefSecret)
	if err != nil {
		log.Error(err)
		return
	}
	// create manual backup request
	period := ds.Period
	rotation := ds.Rotation
	backupServiceName := fmt.Sprintf("%s/%s", ds.PvcNamespace, ds.PvcName)
	pgBackupService := backup.NewPgBackupService(backupServiceName, *pgCreds)
	description := viper.GetString("description")
	b := backup.NewBackup(bucket, pgBackupService, period, rotation, backup.ManualBackupRequest, description)
	if err := b.CreateBackupRequest(); err != nil {
		log.Errorf("error creating backup request, err: %s", err)
		os.Exit(1)
	}
	log.Infof("created backup request, ID: %s", b.BackupId)
}

func cliDownloadBackup() {
	b := selectBackup(">")
	if b == nil {
		os.Exit(1)
	}
	s := spinner.New(spinner.CharSets[27], 50*time.Millisecond)
	s.Suffix = fmt.Sprintf("downloading DB dump to: %s", b.Service.DumpfileLocalPath())
	s.Color("green")
	s.Start()
	if err := b.Service.DownloadBackupAssets(b.Bucket, b.BackupId); err != nil {
		log.Error(err)
		return
	}
	s.Stop()
	fmt.Println("")
	log.Info("done")

}

func cliRestoreBackup() {
	b := selectBackup(">")
	if b == nil {
		os.Exit(1)
	}
	// request backup restore
	if b.Status != backup.Finished {
		log.Errorf("backup status: %s, backup restore request works only for Finished backups", b.Status)
	}
	restoreRequest := &backup.Restore{
		Date:   time.Now(),
		Status: backup.RestoreRequest,
	}
	if len(b.Restores) == 0 {
		b.Restores = []*backup.Restore{restoreRequest}
	} else {
		b.Restores = append(b.Restores, restoreRequest)
	}
	if err := b.SyncState(); err != nil {
		log.Error(err)
		os.Exit(1)
	}
	log.Info("done")

}

func cliDescribeBackup() {
	b := selectBackup(">")
	if b == nil {
		os.Exit(1)
	}
	backupStr, err := b.Jsonify()
	if err != nil {
		log.Error(err)
		return
	}
	var out bytes.Buffer
	if err := json.Indent(&out, []byte(backupStr), "", "  "); err != nil {
		log.Error(err)
		return
	}
	log.Infof("\n%s", out.Bytes())

}

func selectBackup(selector string) *backup.Backup {
	template := &promptui.SelectTemplates{
		Label:    "{{ . }}?",
		Active:   fmt.Sprintf("%s {{ .BackupId | cyan }} ({{ .Date | red }})", selector),
		Inactive: "  {{ .BackupId | faint }} ({{ .Date | faint }})",
		Selected: fmt.Sprintf("%s {{ .BackupId | red | cyan }}", selector),
		Details: `
--------- Backup ----------
{{ "Id:" | faint }}	{{ .BackupId }}
{{ "Date:" | faint }}	{{ .Date }}
{{ "ServiceType:" | faint }}	{{ .ServiceType }}
{{ "RequestType:" | faint }}	{{ .RequestType }}
{{ "Description:" | faint }}	{{ .Description }}
{{ "Status:" | faint }}	{{ if eq .Status "finished" }}{{ printf "%s" .Status | green }}{{ else }}{{ printf "%s" .Status | red }}{{ end }} `,
	}
	var backups []*backup.Backup
	for _, bucket := range backup.GetBackupBuckets() {
		backups = append(backups, bucket.ScanBucket(backup.NewAllV1Alpha1ScanOptions())...)
	}
	if len(backups) == 0 {
		log.Info("backup list is empty!")
		return nil
	}
	if viper.GetString("id") != "" {
		for _, b := range backups {
			if b.BackupId == viper.GetString("id") {
				return b
			}
		}
		log.Infof("backupId: %s not found", viper.GetString("id"))
		return nil
	}
	backupSelect := promptui.Select{
		Label:     "Select backup",
		Items:     backups,
		Size:      10,
		Templates: template,
	}

	var err error
	idx, _, err := backupSelect.Run()
	if err != nil {
		log.Error(err)
		return nil
	}
	return backups[idx]
}

func setupLogging() {

	// Set log verbosity
	if viper.GetBool("verbose") {
		log.SetLevel(log.DebugLevel)
		log.SetReportCaller(true)
	} else {
		log.SetLevel(log.InfoLevel)
		log.SetReportCaller(false)
	}
	// Set log format
	if viper.GetBool("json-log") {
		log.SetFormatter(&log.JSONFormatter{})
	} else {
		log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	}
	// Logs are always goes to STDOUT
	log.SetOutput(os.Stdout)
}

func main() {

	setupCommands()
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
