package backup

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4"

	//"github.com/jackc/pgx/v4"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/teris-io/shortid"
	"net"
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

		Context("Test period parsing ", func() {

			It("Test period parsing for seconds", func() {
				var seconds float64 = 10
				bucket := initMinioBucket()
				backup := NewBackup(bucket, getPgBackupService(), "10s", 3)
				Expect(seconds).To(Equal(backup.Period))
			})

			It("Test period parsing for minutes", func() {
				var seconds float64 = 60
				bucket := initMinioBucket()
				backup := NewBackup(bucket, getPgBackupService(), "1m", 3)
				Expect(seconds).To(Equal(backup.Period))
			})

			It("Test period parsing for hours", func() {
				var seconds float64 = 3600
				bucket := initMinioBucket()
				backup := NewBackup(bucket, getPgBackupService(), "1h", 3)
				Expect(seconds).To(Equal(backup.Period))
			})
		})

		Context("Test period limits", func() {

			It("Backup request - test period not expired", func() {
				bucket := initMinioBucket()
				for i := 0; i < 5; i++ {
					backup := NewBackup(bucket, getPgBackupService(), "10m", 3)
					_ = backup.createBackupRequest()
				}
				Expect(len(bucket.ScanBucket(PgService))).To(Equal(1))
			})

			It("Backup request - test period expired", func() {
				bucket := initMinioBucket()

				backup := NewBackup(bucket, getPgBackupService(), "1s", 2)
				_ = backup.createBackupRequest()
				time.Sleep(1 * time.Second)
				backup = NewBackup(bucket, getPgBackupService(), "1s", 2)
				_ = backup.createBackupRequest()
				time.Sleep(1 * time.Second)
				backup = NewBackup(bucket, getPgBackupService(), "1s", 2)
				_ = backup.createBackupRequest()
				time.Sleep(1 * time.Second)
				backup = NewBackup(bucket, getPgBackupService(), "1s", 2)
				_ = backup.createBackupRequest()
				time.Sleep(1 * time.Second)
				
				Expect(len(bucket.ScanBucket(PgService))).To(Equal(3))
			})
		})

		Context("Minio bucket", func() {

			It("Test minio ping", func() {
				bucket := initMinioBucket()
				Expect(bucket.Ping()).To(BeNil())
			})

			It("Test simple PostgreSQL backup", func() {
				bucket := initMinioBucket()
				backup := NewBackup(bucket, getPgBackupService(), "10m", 3)
				_ = backup.createBackupRequest()
				backups := bucket.ScanBucket(PgService)
				Expect(len(backups)).To(Equal(1))
				Expect(backup.backup()).To(BeNil())
			})

			It("Test backup with restore", func() {
				tableName := "auto_tests_minio"
				bucket := initMinioBucket()

				backup := NewBackup(bucket, getPgBackupService(), "10m", 3)
				execSql(fmt.Sprintf("create table %s(f1 varchar(255), f2 varchar(255));", tableName))
				execSql(fmt.Sprintf("insert into %s(f1, f2) values ('foo', 'bar');", tableName))
				_ = backup.createBackupRequest()
				backups := bucket.ScanBucket(PgService)
				Expect(len(backups)).To(Equal(1))
				Expect(backup.backup()).To(BeNil())
				execSql(fmt.Sprintf("drop table %s;", tableName))

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

			})

			It("Test rotation Minio bucket", func() {
				bucket := initMinioBucket()

				backup0 := NewBackup(bucket, getPgBackupService(), "1s", 2)
				_ = backup0.createBackupRequest()
				time.Sleep(1 * time.Second)

				backup1 := NewBackup(bucket, getPgBackupService(), "1s", 2)
				_ = backup1.createBackupRequest()
				time.Sleep(1 * time.Second)

				backup2 := NewBackup(bucket, getPgBackupService(), "1s", 2)
				_ = backup2.createBackupRequest()

				backups := bucket.ScanBucket(PgService)
				Expect(len(backups)).To(Equal(3))
				backups = bucket.ScanBucket(PgService)
				Expect(bucket.RotateBackups(backups)).To(BeTrue())
				backups = bucket.ScanBucket(PgService)
				Expect(len(backups)).To(Equal(2))

				expected := []string{backups[0].BackupId, backups[1].BackupId}
				shouldBe := []string{backup2.BackupId, backup1.BackupId}
				Expect(expected).To(Equal(shouldBe))

			})
		})

		Context("AWS S3 bucket", func() {

			//It("Test simple backup (AWS S3)", func() {
			//	bucket := initS3Bucket()
			//	pgCreds := PgCreds{Host: "127.0.0.1", DbName: "cnvrg", User: "cnvrg", Pass: "cnvrg"}
			//	backup := NewPgBackup("my-prefix", "10m", 3, bucket, pgCreds)
			//	_ = backup.createBackupRequest()
			//	backups := bucket.ScanBucket()
			//	Expect(len(backups)).To(Equal(1))
			//	Expect(backup.backup()).To(BeNil())
			//})
			//
			//It("Test backup with restore (AWS S3)", func() {
			//	tableName := "auto_tests_aws_s3"
			//	bucket := initS3Bucket()
			//	pgCreds := PgCreds{Host: "127.0.0.1", DbName: "cnvrg", User: "cnvrg", Pass: "cnvrg"}
			//	backup := NewPgBackup("my-prefix", "10m", 3, bucket, pgCreds)
			//	execSql(*backup, fmt.Sprintf("create table %s(f1 varchar(255), f2 varchar(255));", tableName))
			//	execSql(*backup, fmt.Sprintf("insert into %s(f1, f2) values ('foo', 'bar');", tableName))
			//	_ = backup.createBackupRequest()
			//	backups := bucket.ScanBucket()
			//	Expect(len(backups)).To(Equal(1))
			//	Expect(backup.backup()).To(BeNil())
			//	execSql(*backup, fmt.Sprintf("drop table %s;", tableName))
			//
			//	args := []string{"--dbname=postgresql://cnvrg:cnvrg@127.0.0.1:5432/postgres",
			//		"--clean",
			//		"--create",
			//		"--exit-on-error",
			//		"--format=t",
			//		backup.LocalDumpPath}
			//	Expect(shellCmd("pg_restore", args)).To(BeNil())
			//	foo, bar := validateSqlDataExists(*backup, tableName)
			//	Expect(foo).To(Equal("foo"))
			//	Expect(bar).To(Equal("bar"))
			//
			//})
			//
			//It("Test rotation AWS S3 backup", func() {
			//	bucket := initS3Bucket()
			//	pgCreds := PgCreds{Host: "127.0.0.1", DbName: "cnvrg", User: "cnvrg", Pass: "cnvrg"}
			//
			//	backup0 := NewPgBackup("my-prefix", "1s", 2, bucket, pgCreds)
			//	_ = backup0.createBackupRequest()
			//	time.Sleep(1 * time.Second)
			//
			//	backup1 := NewPgBackup("my-prefix", "1s", 2, bucket, pgCreds)
			//	_ = backup1.createBackupRequest()
			//	time.Sleep(1 * time.Second)
			//
			//	backup2 := NewPgBackup("my-prefix", "1s", 2, bucket, pgCreds)
			//	_ = backup2.createBackupRequest()
			//	backups := bucket.ScanBucket()
			//	Expect(len(backups)).To(Equal(3))
			//	Expect(bucket.rotateBackups()).To(BeTrue())
			//	backups = bucket.ScanBucket()
			//	Expect(len(backups)).To(Equal(2))
			//	expected := []string{backups[0].BackupId, backups[1].BackupId}
			//	shouldBe := []string{backup2.BackupId, backup1.BackupId}
			//	Expect(expected).To(Equal(shouldBe))
			//
			//})

		})

		Context("Azure S3 bucket", func() {

			//FIt("Test simple backup (Azure S3)", func() {
			//	bucket := initAzureBucket()
			//	pgCreds := PgCreds{Host: "127.0.0.1", DbName: "cnvrg", User: "cnvrg", Pass: "cnvrg"}
			//	backup := NewPgBackup("my-prefix", "10m", 3, bucket, pgCreds)
			//	_ = backup.createBackupRequest()
			//	backups := bucket.ScanBucket()
			//	Expect(len(backups)).To(Equal(1))
			//	Expect(backup.backup()).To(BeNil())
			//})

			//It("Test backup with restore (AWS S3)", func() {
			//	tableName := "auto_tests_aws_s3"
			//	bucket := initS3Bucket()
			//	pgCreds := PgCreds{Host: "127.0.0.1", DbName: "cnvrg", User: "cnvrg", Pass: "cnvrg"}
			//	backup := NewPgBackup("my-prefix", "10m", 3, bucket, pgCreds)
			//	execSql(*backup, fmt.Sprintf("create table %s(f1 varchar(255), f2 varchar(255));", tableName))
			//	execSql(*backup, fmt.Sprintf("insert into %s(f1, f2) values ('foo', 'bar');", tableName))
			//	_ = backup.createBackupRequest()
			//	backups := bucket.ScanBucket()
			//	Expect(len(backups)).To(Equal(1))
			//	Expect(backup.backup()).To(BeNil())
			//	execSql(*backup, fmt.Sprintf("drop table %s;", tableName))
			//
			//	args := []string{"--dbname=postgresql://cnvrg:cnvrg@127.0.0.1:5432/postgres",
			//		"--clean",
			//		"--create",
			//		"--exit-on-error",
			//		"--format=t",
			//		backup.LocalDumpPath}
			//	Expect(shellCmd("pg_restore", args)).To(BeNil())
			//	foo, bar := validateSqlDataExists(*backup, tableName)
			//	Expect(foo).To(Equal("foo"))
			//	Expect(bar).To(Equal("bar"))
			//
			//})
			//
			//It("Test rotation AWS S3 backup", func() {
			//	bucket := initS3Bucket()
			//	pgCreds := PgCreds{Host: "127.0.0.1", DbName: "cnvrg", User: "cnvrg", Pass: "cnvrg"}
			//
			//	backup0 := NewPgBackup("my-prefix", "1s", 2, bucket, pgCreds)
			//	_ = backup0.createBackupRequest()
			//	time.Sleep(1 * time.Second)
			//
			//	backup1 := NewPgBackup("my-prefix", "1s", 2, bucket, pgCreds)
			//	_ = backup1.createBackupRequest()
			//	time.Sleep(1 * time.Second)
			//
			//	backup2 := NewPgBackup("my-prefix", "1s", 2, bucket, pgCreds)
			//	_ = backup2.createBackupRequest()
			//	backups := bucket.ScanBucket()
			//	Expect(len(backups)).To(Equal(3))
			//	Expect(bucket.rotateBackups()).To(BeTrue())
			//	backups = bucket.ScanBucket()
			//	Expect(len(backups)).To(Equal(2))
			//	expected := []string{backups[0].BackupId, backups[1].BackupId}
			//	shouldBe := []string{backup2.BackupId, backup1.BackupId}
			//	Expect(expected).To(Equal(shouldBe))
			//
			//})

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
	return NewMinioBackupBucket(
		"127.0.0.1:9000",
		"useast2",
		"123qweasd",
		"123qweasd",
		bn,
		"")
	//return Bucket{
	//	Id:         "my-bucket-id",
	//	Endpoint:   "127.0.0.1:9000",
	//	Region:     "useast2",
	//	AccessKey:  "123qweasd",
	//	SecretKey:  "123qweasd",
	//	UseSSL:     false,
	//	Bucket:     bn,
	//	DstDir:     "cnvrg-smart-backups",
	//	BucketType: MinioBucket,
	//}
}

//
//func initS3Bucket() Bucket {
//	bn, _ := shortid.Generate()
//	bn = strings.ReplaceAll(strings.ToLower(bn), "-", "z")
//	bn = strings.ReplaceAll(bn, "_", "z")
//	return Bucket{
//		Id:         bn,
//		Endpoint:   "",
//		Region:     "us-east-2",
//		AccessKey:  os.Getenv("AWS_ACCESS_KEY"),
//		SecretKey:  os.Getenv("AWS_SECRET_KEY"),
//		UseSSL:     true,
//		Bucket:     "cnvrg-capsule-test-bucket",
//		DstDir:     bn,
//		BucketType: AwsBucket,
//	}
//}
//
//func initAzureBucket() Bucket {
//	bn, _ := shortid.Generate()
//	bn = strings.ReplaceAll(strings.ToLower(bn), "-", "z")
//	bn = strings.ReplaceAll(bn, "_", "z")
//	return Bucket{
//		Id:         bn,
//		Endpoint:   "",
//		Region:     "",
//		AccessKey:  os.Getenv("AZURE_ACCESS_KEY"),
//		SecretKey:  os.Getenv("AZURE_SECRET_KEY"),
//		UseSSL:     true,
//		Bucket:     "jenkins",
//		DstDir:     bn,
//		BucketType: AzureBucket,
//	}
//}
//

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
	return NewPgBackupService(pgCreds)
}
