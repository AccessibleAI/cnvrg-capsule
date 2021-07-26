package main

import (
	"fmt"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/apiserver"
	backup "github.com/AccessibleAI/cnvrg-capsule/pkg/backup"
	"github.com/AccessibleAI/cnvrg-capsule/pkg/k8s"
	"github.com/sirupsen/logrus"
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
	BuildVersion string
	rootParams   = []param{
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
		logrus.Infof("starting capsule, mode: %s", runMode)
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
			logrus.Error("wrong parameter provided, must be one of the following: pg")
			_ = cmd.Help()
			os.Exit(1)
		}
		if args[0] != "pg" {
			logrus.Errorf("wrong paramter provided: %s, must be one of the following: pg", args[0])
			_ = cmd.Help()
			os.Exit(1)
		}
		logrus.Info("starting capsule...")
	},
}

var cliBackupPg = &cobra.Command{
	Use:   "pg",
	Short: "Backup PG",
	Run: func(cmd *cobra.Command, args []string) {
		logrus.Info("starting pg backup...")
		if !viper.GetBool("auto-discovery") {
			logrus.Fatalf("currently only auto-discover=true is supported")
		}
		apps := k8s.GetCnvrgApps()
		for _, app := range apps.Items {
			if !backup.ShouldBackup(app) {
				continue
			}
			// discover pg creds
			pgCreds, err := backup.NewPgCredsWithAutoDiscovery(app.Spec.Dbs.Pg.Backup.CredsRef, app.Namespace)
			if err != nil {
				return
			}
			// discover destination bucket
			bucket, err := backup.NewBucketWithAutoDiscovery(app.Spec.Dbs.Pg.Backup.BucketRef, app.Namespace)
			if err != nil {
				return
			}
			// create backup request
			period := app.Spec.Dbs.Pg.Backup.Period
			rotation := app.Spec.Dbs.Pg.Backup.Rotation
			pgBackup := backup.NewPgBackupService(*pgCreds)
			backup := backup.NewBackup(bucket, pgBackup, period, rotation, "")
			if err := backup.Service.Dump(); err != nil {
				logrus.Fatalf("error dumping DB, err: %s", err)
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
	cliBackup.AddCommand(cliBackupPg)
	rootCmd.AddCommand(startCapsule)
	rootCmd.AddCommand(capsuleVersion)
	rootCmd.AddCommand(cliBackup)

}

func setupLogging() {

	// Set log verbosity
	if viper.GetBool("verbose") {
		logrus.SetLevel(logrus.DebugLevel)
		logrus.SetReportCaller(true)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
		logrus.SetReportCaller(false)
	}
	// Set log format
	if viper.GetBool("json-log") {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logrus.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	}
	// Logs are always goes to STDOUT
	logrus.SetOutput(os.Stdout)
}

func main() {

	setupCommands()
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
