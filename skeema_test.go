package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/pmezard/go-difflib/difflib"
	log "github.com/sirupsen/logrus"
	"github.com/skeema/mybase"
	"github.com/skeema/tengo"
)

func TestMain(m *testing.M) {
	// Suppress packet error output when attempting to connect to a Dockerized
	// mysql-server which is still starting up
	tengo.UseFilteredDriverLogger()

	// Omit skeema output unless running test with verbose flag
	flag.Parse()
	if !testing.Verbose() {
		log.SetLevel(log.PanicLevel)
	}

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

// HandleCommand exuctes the supplied Skeema command-line, and confirms its exit
// code matches the expected value.
func (s *SkeemaIntegrationSuite) HandleCommand(t *testing.T, expectedExitCode int, commandLine string, a ...interface{}) *mybase.Config {
	fullCommandLine := fmt.Sprintf(commandLine, a...)
	fakeFileSource := mybase.SimpleSource(map[string]string{"password": s.d.Instance.Password})
	cfg := mybase.ParseFakeCLI(t, CommandSuite, fullCommandLine, fakeFileSource)
	err := cfg.HandleCommand()
	var actualExitCode int
	if err == nil {
		actualExitCode = 0
	} else if ev, ok := err.(*ExitValue); ok {
		actualExitCode = ev.Code
	} else {
		t.Fatalf("Error return from `%s`: %s", fullCommandLine, err)
	}
	if actualExitCode != expectedExitCode {
		t.Fatalf("Unexpected exit code from `%s`: Expected code=%d, found code=%d, message=%s", fullCommandLine, expectedExitCode, actualExitCode, err)
	}
	return cfg
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
				t.Errorf("Sections with port option do not match between %s and %s", aOptionFile.Path(), bOptionFile.Path())
			} else {
				for _, section := range bSectionsWithPort {
					bOptionFile.UseSection(section)
					forcedValue, _ := bOptionFile.OptionValue("port")
					aOptionFile.SetOptionValue(section, "port", forcedValue)
				}
			}

			if !aOptionFile.SameContents(bOptionFile) {
				t.Errorf("File contents do not match between %s and %s", aOptionFile.Path(), bOptionFile.Path())
			}
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
					diff := difflib.UnifiedDiff{
						A:        difflib.SplitLines(aSQLFiles[n].Contents),
						B:        difflib.SplitLines(bSQLFiles[n].Contents),
						FromFile: aSQLFiles[n].Path(),
						ToFile:   bSQLFiles[n].Path(),
						Context:  0,
					}
					diffText, err := difflib.GetUnifiedDiffString(diff)
					if err == nil {
						for _, line := range strings.Split(diffText, "\n") {
							if len(line) > 0 {
								t.Log(line)
							}
						}
					}
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

func (s *SkeemaIntegrationSuite) ReinitAndVerifyFiles(t *testing.T, comparePath string) {
	t.Helper()

	if comparePath == "" {
		comparePath = "../golden/init"
	}
	if err := os.RemoveAll("mydb"); err != nil {
		t.Fatalf("Unable to clean directory: %s", err)
	}
	cfg := s.HandleCommand(t, CodeSuccess, "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	s.VerifyFiles(t, cfg, comparePath)
}

func (s *SkeemaIntegrationSuite) AssertExists(t *testing.T, schema, table, column string) {
	t.Helper()
	exists, phrase, err := s.objectExists(schema, table, column)
	if err != nil {
		t.Fatalf("Unexpected error checking existence of %s: %s", phrase, err)
	}
	if !exists {
		t.Errorf("Expected %s to exist, but it does not", phrase)
	}
}

func (s *SkeemaIntegrationSuite) AssertMissing(t *testing.T, schema, table, column string) {
	t.Helper()
	exists, phrase, err := s.objectExists(schema, table, column)
	if err != nil {
		t.Fatalf("Unexpected error checking existence of %s: %s", phrase, err)
	}
	if exists {
		t.Errorf("Expected %s to not exist, but it does", phrase)
	}
}

func (s *SkeemaIntegrationSuite) objectExists(schemaName, tableName, columnName string) (exists bool, phrase string, err error) {
	if schemaName == "" || (tableName == "" && columnName != "") {
		panic(errors.New("Invalid parameter combination"))
	}
	if tableName == "" && columnName == "" {
		phrase = fmt.Sprintf("schema %s", schemaName)
	} else if columnName == "" {
		phrase = fmt.Sprintf("table %s.%s", schemaName, columnName)
	} else {
		phrase = fmt.Sprintf("column %s.%s.%s", schemaName, tableName, columnName)
	}

	schema, err := s.d.Schema(schemaName)
	if tableName == "" && columnName == "" {
		return schema != nil, phrase, err
	} else if err != nil {
		return false, phrase, fmt.Errorf("Unable to obtain %s: %s", phrase, err)
	}

	table, err := schema.Table(tableName)
	if columnName == "" {
		return table != nil, phrase, err
	} else if err != nil {
		return false, phrase, fmt.Errorf("Unable to obtain %s: %s", phrase, err)
	}

	columns := table.ColumnsByName()
	_, exists = columns[columnName]
	return exists, phrase, nil
}
