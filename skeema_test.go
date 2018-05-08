package main

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/skeema/mybase"
	"github.com/skeema/tengo"
)

func TestMain(m *testing.M) {
	// Suppress packet error output when attempting to connect to a Dockerized
	// mysql-server which is still starting up
	tengo.UseFilteredDriverLogger()

	// Add global options to the global command suite, just like in main()
	AddGlobalOptions(CommandSuite)

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
	d   *tengo.DockerizedInstance
	pwd string
}

func (s *SkeemaIntegrationSuite) Setup(backend string) (err error) {
	// Remember working directory, which should be the base dir for the repo
	s.pwd, err = os.Getwd()
	if err != nil {
		return err
	}

	// Spin up a Dockerized database server
	s.d, err = tengo.CreateDockerizedInstance(backend)
	if err != nil {
		return err
	}

	return nil
}

func (s *SkeemaIntegrationSuite) Teardown(backend string) error {
	if err := s.d.Destroy(); err != nil {
		return err
	}
	if err := os.Chdir(s.pwd); err != nil {
		return err
	}
	if err := os.RemoveAll("testdata/.tmpworkspace"); err != nil {
		return err
	}
	return nil
}

func (s *SkeemaIntegrationSuite) BeforeTest(method string, backend string) error {
	// Return to original dir
	if err := os.Chdir(s.pwd); err != nil {
		return err
	}

	// Clear data and re-source setup data
	if err := s.d.NukeData(); err != nil {
		return err
	}
	if _, err := s.d.SourceSQL("testdata/setup.sql"); err != nil {
		return err
	}

	// Create or recreate workspace dir
	if _, err := os.Stat("testdata/.tmpworkspace"); err == nil { // dir exists
		if err := os.RemoveAll("testdata/.tmpworkspace"); err != nil {
			return err
		}
	}
	if err := os.MkdirAll("testdata/.tmpworkspace", 0777); err != nil {
		return err
	}
	if err := os.Chdir("testdata/.tmpworkspace"); err != nil {
		return err
	}

	return nil
}

// VerifyFiles compares the files in testdata/.tmpworkspace to the files in the
// specified dir, and fails the test if any differences are found.
func (s *SkeemaIntegrationSuite) VerifyFiles(t *testing.T, cfg *mybase.Config, dirExpectedBase string) {
	t.Helper()

	var compareDirs func(*Dir, *Dir)
	compareDirs = func(a, b *Dir) {
		t.Helper()

		// Compare .skeema option files
		if a.HasOptionFile() != b.HasOptionFile() {
			t.Errorf("Presence of option files does not match between %s and %s", a, b)
		}
		if a.HasOptionFile() {
			aOptionFile, err := a.OptionFile()
			if err != nil {
				t.Fatalf(err.Error())
			}
			bOptionFile, err := b.OptionFile()
			if err != nil {
				t.Fatalf(err.Error())
			}
			// Force port number of a to equal port number in b, since b will use whatever
			// dynamic port was allocated to the Dockerized database instance
			aSectionsWithPort := aOptionFile.SectionsWithOption("port")
			bSectionsWithPort := bOptionFile.SectionsWithOption("port")
			if !reflect.DeepEqual(aSectionsWithPort, bSectionsWithPort) {
				t.Errorf("Sections with port option do not match between %s and %s", a, b)
			} else {
				for _, section := range bSectionsWithPort {
					bOptionFile.UseSection(section)
					forcedValue, _ := bOptionFile.OptionValue("port")
					aOptionFile.SetOptionValue(section, "port", forcedValue)
				}
			}

			// TODO COMPARE THE FILES !!!!
		}

		// Compare *.sql files
		aSQLFiles, err := a.SQLFiles()
		if err != nil {
			t.Fatalf("Unable to obtain *.sql from %s: %s", a, err)
		}
		bSQLFiles, err := b.SQLFiles()
		if err != nil {
			t.Fatalf("Unable to obtain *.sql from %s: %s", b, err)
		}
		if len(aSQLFiles) != len(bSQLFiles) {
			t.Errorf("Differing count of *.sql files between %s and %s", a, b)
		} else {
			for n := range aSQLFiles {
				if aSQLFiles[n].FileName != bSQLFiles[n].FileName || aSQLFiles[n].Contents != bSQLFiles[n].Contents {
					t.Errorf("Difference found in %s vs %s", aSQLFiles[n].Path(), bSQLFiles[n].Path())
				}
			}
		}

		// Compare subdirs and walk them
		aSubdirs, err := a.Subdirs()
		if err != nil {
			t.Fatalf("Unable to list subdirs of %s: %s", a, err)
		}
		bSubdirs, err := b.Subdirs()
		if err != nil {
			t.Fatalf("Unable to list subdirs of %s: %s", b, err)
		}
		if len(aSubdirs) != len(bSubdirs) {
			t.Errorf("Differing count of subdirs between %s and %s", a, b)
		} else {
			for n := range aSubdirs {
				if aSubdirs[n].BaseName() != bSubdirs[n].BaseName() {
					t.Errorf("Subdir name mismatch: %s vs %s", aSubdirs[n], bSubdirs[n])
				} else {
					compareDirs(aSubdirs[n], bSubdirs[n])
				}
			}
		}
	}

	expected, err := NewDir(dirExpectedBase, cfg)
	if err != nil {
		t.Fatalf("NewDir(%s) returned %s", dirExpectedBase, err)
	}
	actual, err := NewDir(filepath.Join(s.pwd, "testdata/.tmpworkspace"), cfg)
	if err != nil {
		t.Fatalf("NewDir(%s) returned %s", filepath.Join(s.pwd, "testdata/.tmpworkspace"), err)
	}
	compareDirs(expected, actual)
}

func (s *SkeemaIntegrationSuite) VerifyInstance(t *testing.T) {
	t.Helper()

	// confirm empty diff of workspace dir vs instance. used for push, diff
}
