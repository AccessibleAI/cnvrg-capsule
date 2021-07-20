package backup_test

import (
	"github.com/spf13/viper"
	"os"
	"strings"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestBackup(t *testing.T) {
	os.Setenv("CNVRG_CAPSULE_DUMPDIR", "/tmp")
	viper.AutomaticEnv()
	viper.SetEnvPrefix("CNVRG_CAPSULE")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	RegisterFailHandler(Fail)
	RunSpecs(t, "Backup Suite")
}
