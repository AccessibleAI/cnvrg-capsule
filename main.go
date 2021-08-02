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
	BuildVersion    string
	cliBackupParams = []param{
		{name: "list", shorthand: "l", value: false, usage: "list backups"},
		{name: "delete", shorthand: "", value: false, usage: "delete backup"},
		{name: "create", shorthand: "c", value: false, usage: "create backup manually"},
		{name: "download", shorthand: "d", value: false, usage: "download backup"},
		{name: "restore", shorthand: "r", value: false, usage: "request backup restore"},
		{name: "describe", shorthand: "", value: false, usage: "describe backup metadata"},
	}
	rootParams = []param{
		{name: "verbose", shorthand: "v", value: false, usage: "--verbose=true|false"},
		{name: "dumpdir", shorthand: "", value: "/tmp", usage: "place to download DB dumps before uploading to s3 bucket"},
		{name: "auto-discovery", shorthand: "", value: true, usage: "automatically detect pg creds based on K8s secret"},
		{name: "ns-whitelist", shorthand: "", value: "*", usage: "when auto-discovery is true, specify the namespaces list, by default lookup in all namespaces"},
		{name: "kubeconfig", shorthand: "", value: k8s.KubeconfigDefaultLocation(), usage: "absolute path to the kubeconfig file"},
	}
	startCapsuleParams = []param{
		{name: "api-bind-addr", shorthand: "", value: ":8080", usage: "The address to bind to the api service."},
		{name: "mode", shorthand: "", value: "allinone", usage: fmt.Sprintf("%s|%s|%s", AllInOne, Api, BackupEngine)},
	}
)

var rootCmd = &cobra.Command{
	Use:   "cnvrg-capsule",
	Short: "cnvrg-capsule - Cnvrg backup and restore service",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		setupLogging()
	},
}

var capsuleVersion = &cobra.Command{
	Use:   "version",
	Short: "Print cnvrg-capsule version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("🐾 version: %s\n", BuildVersion)
	},
}

var startCapsule = &cobra.Command{
	Use:   "start",
	Short: fmt.Sprintf("Start the capsule backup service (by default mode=allinone, supported modes: %s|%s|%s)", AllInOne, Api, BackupEngine),
	Run: func(cmd *cobra.Command, args []string) {
		stopChan := make(chan bool)
		runMode := RunMode(viper.GetString("mode"))
		log.Infof("starting capsule, mode: %s", runMode)
		if runMode == AllInOne || runMode == Api {
			go apiserver.RunApi()
		}
		if runMode == AllInOne || runMode == BackupEngine {
			go backup.Run()
		}
		<-stopChan
	},
}

var cliBackup = &cobra.Command{
	Use:   "backup",
	Short: "Run capsule backups from cli",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			log.Error("wrong parameter provided, must be one of the following: pg")
			_ = cmd.Help()
			os.Exit(1)
		}
		if args[0] != "pg" {
			log.Errorf("wrong paramter provided: %s, must be one of the following: pg", args[0])
			_ = cmd.Help()
			os.Exit(1)
		}
		log.Info("starting capsule...")
	},
}

var cliBackupPg = &cobra.Command{
	Use:   "pg",
	Short: "Backup PG",
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
	setParams(cliBackupParams, cliBackupPg)
	cliBackup.AddCommand(cliBackupPg)
	rootCmd.AddCommand(cliBackup)
	rootCmd.AddCommand(startCapsule)
	rootCmd.AddCommand(capsuleVersion)

}

func cliListBackups() {
	var backups []*backup.Backup
	for _, bucket := range backup.GetBackupBuckets() {
		backups = append(backups, bucket.ScanBucket(backup.PgService)...)
		for _, backup := range backups {
			formatted := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d",
				backup.Date.Year(), backup.Date.Month(), backup.Date.Day(),
				backup.Date.Hour(), backup.Date.Minute(), backup.Date.Second())
			log.Infof("backup: %s, name: %s, date:%s, status: %s", backup.BackupId, backup.Service.GetName(), formatted, backup.Status)
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
	selectBackupDeleteTemplate := &promptui.SelectTemplates{
		Label:    "{{ . }}?",
		Active:   "🙀 {{ .BackupId | cyan }} ({{ .Date | red }})",
		Inactive: "  {{ .BackupId | faint }} ({{ .Date | faint }})",
		Selected: "🙀 {{ .BackupId | red | cyan }}",
		Details: `
--------- Backup ----------
{{ "Id:" | faint }}	{{ .BackupId }}
{{ "Date:" | faint }}	{{ .Date }}
{{ "Type:" | faint }}	{{ .ServiceType }}
{{ "Status:" | faint }}	{{ .Status }}`,
	}
	if viper.GetString("id") == "" {
		var backups []*backup.Backup
		for _, bucket := range backup.GetBackupBuckets() {
			backups = append(backups, bucket.ScanBucket(backup.PgService)...)
		}
		backupSelect := promptui.Select{
			Label:     "Select backup for deletion",
			Items:     backups,
			Size:      10,
			Templates: selectBackupDeleteTemplate,
		}
		if len(backups) == 0 {
			log.Info("backups list is empty, nothing to delete")
			return
		}
		idx, _, err := backupSelect.Run()
		if err != nil {
			log.Error(err)
			return
		}
		confirmDelete := promptui.Select{
			Label:     fmt.Sprintf("Deleting %s, are you sure?", backups[idx].BackupId),
			Items:     []string{"No", "Yes"},
			Templates: confirmTemplate,
		}
		_, confirm, err := confirmDelete.Run()
		if err != nil {
			log.Error(err)
			return
		}
		if confirm == "Yes" {
			if err := backups[idx].Bucket.Remove(backups[idx].BackupId); err != nil {
				log.Error(err)
				return
			}
			log.Infof("%s removed", backups[idx].BackupId)
		}
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
	// create backup request
	period := ds.Period
	rotation := ds.Rotation
	backupServiceName := fmt.Sprintf("%s/%s", ds.PvcNamespace, ds.PvcName)
	pgBackupService := backup.NewPgBackupService(backupServiceName, *pgCreds)
	b := backup.NewBackup(bucket, pgBackupService, period, rotation)
	if err := b.Service.Dump(); err != nil {
		log.Error(err)
		return
	}
}

func cliDownloadBackup() {
	selectBackupDownloadTemplate := &promptui.SelectTemplates{
		Label:    "{{ . }}?",
		Active:   "> {{ .BackupId | cyan }} ({{ .Date | red }})",
		Inactive: "  {{ .BackupId | faint }} ({{ .Date | faint }})",
		Selected: "> {{ .BackupId | red | cyan }}",
		Details: `
--------- Backup ----------
{{ "Id:" | faint }}	{{ .BackupId }}
{{ "Date:" | faint }}	{{ .Date }}
{{ "Type:" | faint }}	{{ .ServiceType }}
{{ "Status:" | faint }}	{{ if eq .Status "finished" }}{{ printf "%s" .Status | green }} {{ else }} {{ printf "%s" .Status | red }} {{ end }} `,
	}
	var backups []*backup.Backup
	for _, bucket := range backup.GetBackupBuckets() {
		backups = append(backups, bucket.ScanBucket(backup.PgService)...)
	}

	backupSelect := promptui.Select{
		Label:     "Select backup for download",
		Items:     backups,
		Size:      10,
		Templates: selectBackupDownloadTemplate,
	}
	if len(backups) == 0 {
		log.Info("backups list is empty, nothing to delete")
		return
	}
	idx, _, err := backupSelect.Run()
	if err != nil {
		log.Error(err)
		return
	}
	s := spinner.New(spinner.CharSets[27], 50*time.Millisecond)
	s.Suffix = fmt.Sprintf("downloading DB dump to: %s", backups[idx].Service.DumpfileLocalPath())
	s.Color("green")
	s.Start()
	if err := backups[idx].Service.DownloadBackupAssets(backups[idx].Bucket, backups[idx].BackupId); err != nil {
		log.Error(err)
		return
	}
	s.Stop()
	fmt.Println("")
	log.Info("done")

}

func cliRestoreBackup() {
	selectBackupDownloadTemplate := &promptui.SelectTemplates{
		Label:    "{{ . }}?",
		Active:   "> {{ .BackupId | cyan }} ({{ .Date | red }})",
		Inactive: "  {{ .BackupId | faint }} ({{ .Date | faint }})",
		Selected: "> {{ .BackupId | red | cyan }}",
		Details: `
--------- Backup ----------
{{ "Id:" | faint }}	{{ .BackupId }}
{{ "Date:" | faint }}	{{ .Date }}
{{ "Type:" | faint }}	{{ .ServiceType }}
{{ "Status:" | faint }}	{{ if eq .Status "finished" }}{{ printf "%s" .Status | green }} {{ else }} {{ printf "%s" .Status | red }} {{ end }} `,
	}
	var backups []*backup.Backup
	for _, bucket := range backup.GetBackupBuckets() {
		backups = append(backups, bucket.ScanBucket(backup.PgService)...)
	}

	backupSelect := promptui.Select{
		Label:     "Select backup for download",
		Items:     backups,
		Size:      10,
		Templates: selectBackupDownloadTemplate,
	}
	if len(backups) == 0 {
		log.Info("backups list is empty, nothing to delete")
		return
	}
	idx, _, err := backupSelect.Run()
	if err != nil {
		log.Error(err)
		return
	}
	backupForRestore := backups[idx]
	// request backup restore
	if backupForRestore.Status != backup.Finished {
		log.Errorf("backup status: %s, backup restore request works only for Finished backups", backups[idx].Status)
	}
	restoreRequest := &backup.Restore{
		Date:   time.Now(),
		Status: backup.RestoreRequest,
	}
	if len(backupForRestore.Restores) == 0 {
		backupForRestore.Restores = []*backup.Restore{restoreRequest}
	} else {
		backupForRestore.Restores = append(backupForRestore.Restores, restoreRequest)
	}
	log.Info("done")

}

func cliDescribeBackup() {
	selectBackupTemplate := &promptui.SelectTemplates{
		Label:    "{{ . }}?",
		Active:   "> {{ .BackupId | cyan }} ({{ .Date | red }})",
		Inactive: "  {{ .BackupId | faint }} ({{ .Date | faint }})",
		Selected: "> {{ .BackupId | red | cyan }}",
		Details: `
--------- Backup ----------
{{ "Id:" | faint }}	{{ .BackupId }}
{{ "Date:" | faint }}	{{ .Date }}
{{ "Type:" | faint }}	{{ .ServiceType }}
{{ "Status:" | faint }}	{{ .Status }}`,
	}
	var backups []*backup.Backup
	for _, bucket := range backup.GetBackupBuckets() {
		backups = append(backups, bucket.ScanBucket(backup.PgService)...)
	}

	backupSelect := promptui.Select{
		Label:     "Select backup for download",
		Items:     backups,
		Size:      10,
		Templates: selectBackupTemplate,
	}
	if len(backups) == 0 {
		log.Info("backups list is empty...")
		return
	}
	idx, _, err := backupSelect.Run()
	if err != nil {
		log.Error(err)
		return
	}
	backupStr, err := backups[idx].Jsonify()
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
