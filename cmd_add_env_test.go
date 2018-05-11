package main

import (
	"testing"

	"github.com/skeema/mybase"
)

func (s *SkeemaIntegrationSuite) TestAddEnvHandler(t *testing.T) {
	getOptionFile := func(basePath string, baseConfig *mybase.Config) *mybase.File {
		t.Helper()
		dir, err := NewDir(basePath, baseConfig)
		if err != nil {
			t.Fatalf("Unable to obtain directory %s: %s", basePath, err)
		}
		file, err := dir.OptionFile()
		if err != nil {
			t.Fatalf("Unable to obtain %s/.skeema: %s", basePath, err)
		}
		return file
	}

	cfg := s.HandleCommand(t, CodeSuccess, "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// add-environment should fail on a dir that does not exist
	s.HandleCommand(t, CodeBadConfig, "skeema add-environment --host my.staging.db.com --dir does/not/exist staging")

	// add-environment should fail on a dir that does not already contain a .skeema file
	s.HandleCommand(t, CodeBadConfig, "skeema add-environment --host my.staging.db.com staging")

	// bad environment name should fail
	s.HandleCommand(t, CodeBadConfig, "skeema add-environment --host my.staging.db.com --dir mydb '[staging]'")

	// preexisting environment name should fail
	s.HandleCommand(t, CodeBadConfig, "skeema add-environment --host my.staging.db.com --dir mydb production")

	// non-host-level directory should fail
	s.HandleCommand(t, CodeBadConfig, "skeema add-environment --host my.staging.db.com --dir mydb/product staging")

	// lack of host on CLI should fail
	s.HandleCommand(t, CodeBadConfig, "skeema add-environment --dir mydb staging")

	// None of the above failed commands should have modified any files
	s.VerifyFiles(t, cfg, "../golden/init")
	origFile := getOptionFile("mydb", cfg)

	// valid dir should succeed and add the section to the .skeema file
	cfg = s.HandleCommand(t, CodeSuccess, "skeema add-environment --host my.staging.db.com --dir mydb staging")
	file := getOptionFile("mydb", cfg)
	origFile.SetOptionValue("staging", "host", "my.staging.db.com")
	origFile.SetOptionValue("staging", "port", "3306")
	if !origFile.SameContents(file) {
		t.Fatalf("File contents of %s do not match expectation", file.Path())
	}

	// Nonstandard port should work properly; ditto for user option persisting
	cfg = s.HandleCommand(t, CodeSuccess, "skeema add-environment --host my.ci.db.com -P 3307 -ufoobar --dir mydb ci")
	file = getOptionFile("mydb", cfg)
	origFile.SetOptionValue("ci", "host", "my.ci.db.com")
	origFile.SetOptionValue("ci", "port", "3307")
	origFile.SetOptionValue("ci", "user", "foobar")
	if !origFile.SameContents(file) {
		t.Fatalf("File contents of %s do not match expectation", file.Path())
	}

	// localhost and socket should work properly
	s.HandleCommand(t, CodeSuccess, "skeema add-environment -h localhost -S /var/lib/mysql/mysql.sock --dir mydb development")
	file = getOptionFile("mydb", cfg)
	origFile.SetOptionValue("development", "host", "localhost")
	origFile.SetOptionValue("development", "socket", "/var/lib/mysql/mysql.sock")
	if !origFile.SameContents(file) {
		t.Fatalf("File contents of %s do not match expectation", file.Path())
	}
}
