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
	Informer     RunMode = "informer"
)

var (
	BuildVersion string
	rootParams   = []param{
		{name: "verbose", shorthand: "v", value: false, usage: "--verbose=true|false"},
		{name: "auto-discovery", shorthand: "", value: true, usage: "automatically detect pg creds based on K8s secret"},
		{name: "ns-whitelist", shorthand: "", value: "*", usage: "when auto-discovery is true, specify the namespaces list, by default lookup in all namespaces"},
		{name: "kubeconfig", shorthand: "", value: k8s.KubeconfigDefaultLocation(), usage: "absolute path to the kubeconfig file"},
	}
	startCapsuleParams = []param{
		{name: "api-bind-addr", shorthand: "", value: ":8080", usage: "The address to bind to the api service."},
		{name: "mode", shorthand: "", value: "allinone", usage: fmt.Sprintf("%s|%s|%s|%s", AllInOne, Api, BackupEngine, Informer)},
	}
	cliBackupParams = []param{
		{name: "endpoint", shorthand: "", value: "", usage: "S3 endpoint"},
		{name: "access-key", shorthand: "", value: "", usage: "S3 access key"},
		{name: "secret-key", shorthand: "", value: "", usage: "S3 secret key"},
		{name: "s3-tls", shorthand: "", value: false, usage: "Use secure S3 connection"},
		{name: "bucket", shorthand: "", value: "cnvrg-backups", usage: "S3 bucket for backups"},
		{name: "dst-dir", shorthand: "", value: "cnvrg-backups", usage: "Bucket destination directory"},
	}
	cliBackupPgParams = []param{
		{name: "upload", shorthand: "", value: true, usage: "set to false for skipping uploading db dump to S3"},
		{name: "local-dir", shorthand: "", value: "/tmp", usage: "local dir for saving the db dumps"},
		{name: "host", shorthand: "", value: "", usage: "database server host or socket directory, omit if auto-discovery set to true (default)"},
		{name: "port", shorthand: "", value: 5432, usage: "database server port number"},
		{name: "dbname", shorthand: "", value: "cnvrg_production", usage: "database to dump"},
		{name: "user", shorthand: "", value: "", usage: "PG user, omit if auto-discovery set to true (default)"},
		{name: "pass", shorthand: "", value: "", usage: "PG pass, omit if auto-discovery set to true (default)"},
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
	Short: fmt.Sprintf("Start the capsule backup service (by default mode=allinone, supported modes: %s|%s|%s|%s)", AllInOne, Api, BackupEngine, Informer),
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

var cliBackupCapsule = &cobra.Command{
	Use:   "backup",
	Short: "Run capsule backups from cli",
	Run: func(cmd *cobra.Command, args []string) {
		logrus.Info("starting capsule...")
	},
}

var cliBackupPgCapsule = &cobra.Command{
	Use:   "pg",
	Short: "Backup PG",
	Run: func(cmd *cobra.Command, args []string) {
		stopChan := make(chan bool)
		logrus.Info("starting capsule...")
		if viper.GetBool("auto-discovery") {
			//pgCreds := backup.NewPgCredsWithAutoDiscovery()
			//fmt.Println(pgCreds)
		}
		//b := backup.New(backup.PostgreSQL)
		//if viper.GetBool("upload") {
		//	if err := b.CreateBackupRequest(); err != nil {
		//		logrus.Fatal(err)
		//	}
		//}

		<-stopChan
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
	setParams(rootParams, rootCmd)
	setParams(startCapsuleParams, startCapsule)
	setParams(cliBackupPgParams, cliBackupPgCapsule)
	setParams(cliBackupParams, cliBackupCapsule)
	cliBackupCapsule.AddCommand(cliBackupPgCapsule)
	rootCmd.AddCommand(startCapsule)
	rootCmd.AddCommand(capsuleVersion)
	rootCmd.AddCommand(cliBackupCapsule)
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
