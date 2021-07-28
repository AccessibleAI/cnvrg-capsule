package main

import (
	"fmt"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/apiserver"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/backup"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	"github.com/manifoldco/promptui"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"strings"
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
		{name: "delete", shorthand: "d", value: false, usage: "delete backup"},
		{name: "id", shorthand: "", value: "", usage: "backup id"},
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

var selectBackupTemplate = &promptui.SelectTemplates{
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

var confirmTemplate = &promptui.SelectTemplates{
	Label:    `{{ . }}?`,
	Active:   `> {{ . | red}}`,
	Inactive: `  {{ . | faint}} `,
	Selected: `> {{ . | red }}`,
}

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
			log.Info("getting pg backup")
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
		if viper.GetBool("delete") {
			log.Info("getting pg backup")
			if viper.GetString("id") == "" {
				var backups []*backup.Backup
				for _, bucket := range backup.GetBackupBuckets() {
					backups = append(backups, bucket.ScanBucket(backup.PgService)...)
				}
				backupSelect := promptui.Select{
					Label:     "Select backup for deletion",
					Items:     backups,
					Size:      10,
					Templates: selectBackupTemplate,
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
