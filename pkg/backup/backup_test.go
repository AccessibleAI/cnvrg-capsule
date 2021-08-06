package backup

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/lithammer/shortuuid/v3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/teris-io/shortid"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

var (
	pgUser     = "cnvrg"
	minioUser  = "123qweasd"
	pgImage    = "cnvrg/postgresql-12-centos7:latest"
	minioImage = "cnvrg/minio:RELEASE.2021-05-22T02-34-39Z"
	accessKey  = "123qweasd"
	secretKey  = "123qweasd"
	endpoint   = "127.0.0.1:9000"
	pgCreds    = PgCreds{Host: "127.0.0.1", DbName: "cnvrg", User: "cnvrg", Pass: "cnvrg"}
)

var _ = Describe("Backup", func() {
	BeforeSuite(func() {
		pullImage(pgImage)
		pullImage(minioImage)
		runPgContainer()
		runMinioContainer()
		checkServiceReadiness("5432")
		checkServiceReadiness("9000")

	})

	AfterSuite(func() {
		log.Printf("finished testing, cleaning up test resources")
	})

	Describe("Backup testing", func() {
		Context("Minio bucket", func() {
			testBucket := initMinioBucket
			It("Test period parsing for seconds", func() {
				testPeriodParsingForSeconds(testBucket())
			})

			It("Test period parsing for minutes", func() {
				testPeriodParsingForMinutes(testBucket())
			})

			It("Test period parsing for hours", func() {
				testPeriodParsingForHours(testBucket())
			})

			It("Backup request - test period not expired", func() {
				testPeriodNotExpired(testBucket())
			})

			It("Backup request - test period expired", func() {
				testPeriodExpired(testBucket())
			})

			It("Test bucket ping", func() {
				testBucketPing(testBucket())
			})

			It("Test simple PostgreSQL backup", func() {
				testSimplePostgreSQLBackup(testBucket())
			})

			It("Test backup with Restore", func() {
				testBackupWithRestore(testBucket())
			})

			It("Test rotation of completed backups", func() {
				testRotationOfCompletedBackups(testBucket())
			})

			It("Test rotation of not completed backups", func() {
				testRotationOfNOtCompletedBackups(testBucket())
			})
		})

		Context("Amazon bucket", func() {
			testBucket := initAwsBucket
			It("Test period parsing for seconds", func() {
				testPeriodParsingForSeconds(testBucket())
			})

			It("Test period parsing for minutes", func() {
				testPeriodParsingForMinutes(testBucket())
			})

			It("Test period parsing for hours", func() {
				testPeriodParsingForHours(testBucket())
			})

			It("Backup request - test period not expired", func() {
				testPeriodNotExpired(testBucket())
			})

			It("Backup request - test period expired", func() {
				testPeriodExpired(testBucket())
			})

			It("Test bucket ping", func() {
				testBucketPing(testBucket())
			})

			It("Test simple PostgreSQL backup", func() {
				testSimplePostgreSQLBackup(testBucket())
			})

			It("Test backup with Restore", func() {
				testBackupWithRestore(testBucket())
			})

			It("Test rotation of completed backups", func() {
				testRotationOfCompletedBackups(testBucket())
			})

			It("Test rotation of not completed backups", func() {
				testRotationOfNOtCompletedBackups(testBucket())
			})
		})

		Context("Azure bucket", func() {
			testBucket := initAzureBucket
			It("Test period parsing for seconds", func() {
				testPeriodParsingForSeconds(testBucket())
			})

			It("Test period parsing for minutes", func() {
				testPeriodParsingForMinutes(testBucket())
			})

			It("Test period parsing for hours", func() {
				testPeriodParsingForHours(testBucket())
			})

			It("Backup request - test period not expired", func() {
				testPeriodNotExpired(testBucket())
			})

			It("Backup request - test period expired", func() {
				testPeriodExpired(testBucket())
			})

			It("Test bucket ping", func() {
				testBucketPing(testBucket())
			})

			It("Test simple PostgreSQL backup", func() {
				testSimplePostgreSQLBackup(testBucket())
			})

			It("Test backup with Restore", func() {
				testBackupWithRestore(testBucket())
			})

			It("Test rotation of completed backups", func() {
				testRotationOfCompletedBackups(testBucket())
			})

			It("Test rotation of not completed backups", func() {
				testRotationOfNOtCompletedBackups(testBucket())
			})
		})

		Context("GCP bucket", func() {
			testBucket := initGcpBucket
			It("Test period parsing for seconds", func() {
				testPeriodParsingForSeconds(testBucket())
			})

			It("Test period parsing for minutes", func() {
				testPeriodParsingForMinutes(testBucket())
			})

			It("Test period parsing for hours", func() {
				testPeriodParsingForHours(testBucket())
			})

			It("Backup request - test period not expired", func() {
				testPeriodNotExpired(testBucket())
			})

			It("Backup request - test period expired", func() {
				testPeriodExpired(testBucket())
			})

			It("Test bucket ping", func() {
				testBucketPing(testBucket())
			})

			It("Test simple PostgreSQL backup", func() {
				testSimplePostgreSQLBackup(testBucket())
			})

			It("Test backup with Restore", func() {
				testBackupWithRestore(testBucket())
			})

			It("Test rotation of completed backups", func() {
				testRotationOfCompletedBackups(testBucket())
			})

			It("Test rotation of not completed backups", func() {
				testRotationOfNOtCompletedBackups(testBucket())
			})
		})
	})
})

func stopContainer(name string) {
	args := []string{"stop", name}
	_ = shellCmd("docker", args)
	args = []string{"rm", name}
	_ = shellCmd("docker", args)
}

func runPgContainer() {
	stopContainer("pg")
	args := []string{
		"run",
		"-d",
		"--name=pg",
		"-p5432:5432",
		fmt.Sprintf("-ePOSTGRESQL_USER=%s", pgUser),
		fmt.Sprintf("-ePOSTGRESQL_PASSWORD=%s", pgUser),
		fmt.Sprintf("-ePOSTGRESQL_DATABASE=%s", pgUser),
		pgImage,
	}
	_ = shellCmd("docker", args)
}

func runMinioContainer() {
	stopContainer("minio")
	args := []string{
		"run",
		"-d",
		"--name=minio",
		"-p9000:9000",
		fmt.Sprintf("-eMINIO_ACCESS_KEY=%s", minioUser),
		fmt.Sprintf("-eMINIO_SECRET_KEY=%s", minioUser),
		minioImage,
		"minio",
		"gateway",
		"nas",
		"/tmp",
	}
	_ = shellCmd("docker", args)
}

func pullImage(image string) {
	args := []string{"pull", image}
	_ = shellCmd("docker", args)
}

func shellCmd(command string, args []string) error {
	cmd := exec.Command(command, args...)
	err := cmd.Run()
	log.Infof("executing: [%s %s]", command, strings.Join(args, " "))
	if err != nil {
		log.Printf("error executing [%s %s], err: %s", command, strings.Join(args, " "), err)
		return err
	}
	return nil
}

func checkServiceReadiness(port string) {
	for i := 0; i < 10; i++ {
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", port), time.Second)
		if err != nil {
			log.Printf("connecting error: %s", err)
		}
		if conn != nil {
			defer conn.Close()
			log.Printf("opened %s", net.JoinHostPort("127.0.0.1", port))
			return
		}
		time.Sleep(1 * time.Second)
	}
	log.Fatalf("service not ready: 127.0.0.1:%s", port)
}

func createBucket(bucket string) {
	connOptions := &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: false}
	mc, err := minio.New(endpoint, connOptions)
	if err != nil {
		log.Error(err)
		return
	}
	err = mc.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{Region: "useast2", ObjectLocking: false})
	if err != nil {
		log.Error(err)
		return
	}
	log.Printf("bucket: %s created", bucket)
}

func initMinioBucket() *MinioBucket {
	bn, _ := shortid.Generate()
	bn = strings.ReplaceAll(strings.ToLower(bn), "-", "z")
	bn = strings.ReplaceAll(bn, "_", "z")
	createBucket(bn)
	return NewMinioBucket(
		"127.0.0.1:9000",
		"useast2",
		"123qweasd",
		"123qweasd",
		bn,
		"")
}

func initAwsBucket() *MinioBucket {
	bn, _ := shortid.Generate()
	bn = strings.ReplaceAll(strings.ToLower(bn), "-", "z")
	bn = strings.ReplaceAll(bn, "_", "z")
	return NewAwsBucket(
		"us-east-2",
		os.Getenv("AWS_ACCESS_KEY"),
		os.Getenv("AWS_SECRET_KEY"),
		"cnvrg-capsule-test-bucket",
		bn,

	)
}

func initAzureBucket() Bucket {
	bn, _ := shortid.Generate()
	bn = strings.ReplaceAll(strings.ToLower(bn), "-", "z")
	bn = strings.ReplaceAll(bn, "_", "z")
	return NewAzureBucket(
		os.Getenv("AZURE_ACCOUNT_NAME"),
		os.Getenv("AZURE_ACCOUNT_KEY"),
		"jenkins",
		bn,
	)
}

func initGcpBucket() Bucket {
	bn, _ := shortid.Generate()
	bn = strings.ReplaceAll(strings.ToLower(bn), "-", "z")
	bn = strings.ReplaceAll(bn, "_", "z")

	keyJson, err := ioutil.ReadFile(os.Getenv("GOOGLE_CREDS"))
	if err != nil {
		log.Fatal(err)
	}
	return NewGcpBucket(
		string(keyJson),
		os.Getenv("CNVRG_STORAGE_PROJECT"),
		os.Getenv("CNVRG_STORAGE_BUCKET"),
		bn,
	)
}

func execSql(sql string) {
	dbUrl := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", pgCreds.User, pgCreds.Pass, pgCreds.Host, pgCreds.DbName)
	conn, err := pgx.Connect(context.Background(), dbUrl)
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)

	}
	defer conn.Close(context.Background())
	_, err = conn.Exec(context.Background(), sql)
	if err != nil {
		log.Fatalf("row query failed: %v", err)
	}
}

func validateSqlDataExists(tableName string) (foo, bar string) {

	dbUrl := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", pgCreds.User, pgCreds.Pass, pgCreds.Host, pgCreds.DbName)
	conn, err := pgx.Connect(context.Background(), dbUrl)
	if err != nil {
		log.Fatalf("unable to connect to database: %v", err)
	}
	r, err := conn.Query(context.Background(), fmt.Sprintf("select f1,f2 from %s limit 1", tableName))
	r.Next()
	r.Scan(&foo, &bar)
	defer conn.Close(context.Background())
	if err != nil {
		log.Fatalf("row query failed: %v", err)
	}
	return
}

func getPgBackupService() *PgBackupService {
	return NewPgBackupService("testing/test", pgCreds)
}

func testPeriodParsingForSeconds(bucket Bucket) {
	var seconds float64 = 10
	backup := NewBackup(bucket, getPgBackupService(), "10s", 3, PeriodicBackupRequest)
	Expect(seconds).To(Equal(backup.Period))
}

func testPeriodParsingForMinutes(bucket Bucket) {
	var seconds float64 = 60
	backup := NewBackup(bucket, getPgBackupService(), "1m", 3, PeriodicBackupRequest)
	Expect(seconds).To(Equal(backup.Period))
}

func testPeriodParsingForHours(bucket Bucket) {
	var seconds float64 = 3600
	backup := NewBackup(bucket, getPgBackupService(), "1h", 3, PeriodicBackupRequest)
	Expect(seconds).To(Equal(backup.Period))
}

func testPeriodNotExpired(bucket Bucket) {
	for i := 0; i < 5; i++ {
		backup := NewBackup(bucket, getPgBackupService(), "10m", 3, PeriodicBackupRequest)
		_ = backup.createBackupRequest()
	}
	Expect(len(bucket.ScanBucket(PgService, PeriodicBackupRequest))).To(Equal(1))
}

func testPeriodExpired(bucket Bucket) {

	backup := NewBackup(bucket, getPgBackupService(), "1s", 2, PeriodicBackupRequest)
	_ = backup.createBackupRequest()
	time.Sleep(1 * time.Second)
	backup = NewBackup(bucket, getPgBackupService(), "1s", 2, PeriodicBackupRequest)
	_ = backup.createBackupRequest()
	time.Sleep(1 * time.Second)
	backup = NewBackup(bucket, getPgBackupService(), "1s", 2, PeriodicBackupRequest)
	_ = backup.createBackupRequest()
	time.Sleep(1 * time.Second)
	backup = NewBackup(bucket, getPgBackupService(), "1s", 2, PeriodicBackupRequest)
	_ = backup.createBackupRequest()
	time.Sleep(1 * time.Second)

	Expect(len(bucket.ScanBucket(PgService, PeriodicBackupRequest))).To(Equal(3))
}

func testBucketPing(bucket Bucket) {
	Expect(bucket.Ping()).To(BeNil())
}

func testSimplePostgreSQLBackup(bucket Bucket) {
	backup := NewBackup(bucket, getPgBackupService(), "10m", 3, PeriodicBackupRequest)
	_ = backup.createBackupRequest()
	backups := bucket.ScanBucket(PgService, PeriodicBackupRequest)
	Expect(len(backups)).To(Equal(1))
	Expect(backup.backup()).To(BeNil())
}

func testRotationOfCompletedBackups(bucket Bucket) {

	backup0 := NewBackup(bucket, getPgBackupService(), "1s", 2, PeriodicBackupRequest)
	_ = backup0.createBackupRequest()
	time.Sleep(1 * time.Second)

	backup1 := NewBackup(bucket, getPgBackupService(), "1s", 2, PeriodicBackupRequest)
	_ = backup1.createBackupRequest()
	time.Sleep(1 * time.Second)

	backup2 := NewBackup(bucket, getPgBackupService(), "1s", 2, PeriodicBackupRequest)
	_ = backup2.createBackupRequest()

	backups := bucket.ScanBucket(PgService, PeriodicBackupRequest)
	Expect(len(backups)).To(Equal(3))

	_ = backup0.backup()
	_ = backup1.backup()
	_ = backup2.backup()

	backups = bucket.ScanBucket(PgService, PeriodicBackupRequest)
	Expect(rotateBackups(backups)).To(BeTrue())
	backups = bucket.ScanBucket(PgService, PeriodicBackupRequest)
	Expect(len(backups)).To(Equal(2))

	expected := []string{backups[0].BackupId, backups[1].BackupId}
	shouldBe := []string{backup2.BackupId, backup1.BackupId}
	Expect(expected).To(Equal(shouldBe))

}

func testRotationOfNOtCompletedBackups(bucket Bucket) {

	backup0 := NewBackup(bucket, getPgBackupService(), "1s", 2, PeriodicBackupRequest)
	_ = backup0.createBackupRequest()
	time.Sleep(1 * time.Second)

	backup1 := NewBackup(bucket, getPgBackupService(), "1s", 2, PeriodicBackupRequest)
	_ = backup1.createBackupRequest()
	time.Sleep(1 * time.Second)

	backup2 := NewBackup(bucket, getPgBackupService(), "1s", 2, PeriodicBackupRequest)
	_ = backup2.createBackupRequest()

	backups := bucket.ScanBucket(PgService, PeriodicBackupRequest)
	Expect(len(backups)).To(Equal(3))

	backups = bucket.ScanBucket(PgService, PeriodicBackupRequest)
	Expect(rotateBackups(backups)).To(BeFalse())

	backups = bucket.ScanBucket(PgService, PeriodicBackupRequest)
	Expect(len(backups)).To(Equal(3))
}

func testBackupWithRestore(bucket Bucket) {

	backup := NewBackup(bucket, getPgBackupService(), "10m", 3, PeriodicBackupRequest)
	tableName := fmt.Sprintf("auto_tests_minio_%s", shortuuid.New())
	execSql(fmt.Sprintf("create table %s(f1 varchar(255), f2 varchar(255));", tableName))
	execSql(fmt.Sprintf("insert into %s(f1, f2) values ('foo', 'bar');", tableName))
	_ = backup.createBackupRequest()
	backups := bucket.ScanBucket(PgService, PeriodicBackupRequest)
	Expect(len(backups)).To(Equal(1))
	Expect(backup.backup()).To(BeNil())
	execSql(fmt.Sprintf("drop table %s;", tableName))
	Expect(os.Remove(backup.Service.DumpfileLocalPath())).To(BeNil())
	Expect(backup.Service.DownloadBackupAssets(backup.Bucket, backup.BackupId)).To(BeNil())
	args := []string{"--dbname=postgresql://cnvrg:cnvrg@127.0.0.1:5432/postgres",
		"--clean",
		"--create",
		"--exit-on-error",
		"--format=t",
		backup.Service.DumpfileLocalPath()}
	Expect(shellCmd("pg_restore", args)).To(BeNil())
	foo, bar := validateSqlDataExists(tableName)
	Expect(foo).To(Equal("foo"))
	Expect(bar).To(Equal("bar"))
}
