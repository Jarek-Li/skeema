package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/skeema/tengo"
)

func TestMain(m *testing.M) {
	tengo.UseFilteredDriverLogger()
	os.Exit(m.Run())
}

func TestIntegration(t *testing.T) {
	images := tengo.SplitEnv("TENGO_TEST_IMAGES")
	if len(images) == 0 {
		fmt.Println("TENGO_TEST_IMAGES env var is not set, so integration tests will be skipped!")
		fmt.Println("To run integration tests, you may set TENGO_TEST_IMAGES to a comma-separated")
		fmt.Println("list of Docker images. Example:\n# TENGO_TEST_IMAGES=\"mysql:5.6,mysql:5.7\" go test")
	}
	tengo.RunSuite(&SkeemaIntegrationSuite{}, t, images)
}

type SkeemaIntegrationSuite struct {
	d *tengo.DockerizedInstance
}

func (s *SkeemaIntegrationSuite) Setup(backend string) (err error) {
	s.d, err = tengo.CreateDockerizedInstance(backend)
	return err
}

func (s *SkeemaIntegrationSuite) Teardown(backend string) error {
	return s.d.Destroy()
}

func (s *SkeemaIntegrationSuite) BeforeTest(method string, backend string) error {
	if err := s.d.NukeData(); err != nil {
		return err
	}
	if _, err := s.d.SourceSQL("testdata/setup.sql"); err != nil {
		return err
	}

	// Create and clear workspace dir

	return nil
}

func (s *SkeemaIntegrationSuite) VerifyFiles(t *testing.T) {
	t.Helper()

	// compare workspace dir vs specified dir. used for init, pull, lint
}

func (s *SkeemaIntegrationSuite) VerifyInstance(t *testing.T) {
	t.Helper()

	// confirm empty diff of workspace dir vs instance. used for push, diff
}

// need helpers for comparing .skeema files, sql files, etc
