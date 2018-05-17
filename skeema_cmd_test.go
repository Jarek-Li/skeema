package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/skeema/mybase"
)

func (s *SkeemaIntegrationSuite) TestInitHandler(t *testing.T) {
	cfg := s.HandleCommand(t, CodeSuccess, "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	s.VerifyFiles(t, cfg, "../golden/init")
}

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

func (s *SkeemaIntegrationSuite) TestPullHandler(t *testing.T) {
	s.HandleCommand(t, CodeSuccess, "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// In product db, alter one table and drop one table;
	// In analytics db, add one table and alter the schema's charset and collation;
	// Create a new db and put one table in it
	if _, err := s.d.SourceSQL("../pull1.sql"); err != nil {
		t.Fatalf("Unable to setup test: %s", err)
	}

	cfg := s.HandleCommand(t, CodeSuccess, "skeema pull")
	s.VerifyFiles(t, cfg, "../golden/pull1")
}

func (s *SkeemaIntegrationSuite) TestLintHandler(t *testing.T) {
	s.HandleCommand(t, CodeSuccess, "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Initial lint should be a no-op that returns exit code 0
	cfg := s.HandleCommand(t, CodeSuccess, "skeema lint")
	s.VerifyFiles(t, cfg, "../golden/init")

	// Alter a few files in a way that is still valid SQL, but doesn't match
	// the database's native format. Lint should rewrite these files and then
	// return exit code CodeDifferencesFound.
	productDir, err := NewDir("mydb/product", cfg)
	if err != nil {
		t.Fatalf("Unable to obtain dir for mydb/product: %s", err)
	}
	sqlFiles, err := productDir.SQLFiles()
	if err != nil || len(sqlFiles) < 4 {
		t.Fatalf("Unable to obtain *.sql files from %s", productDir)
	}
	rewriteFiles := func(includeSyntaxError bool) {
		for n, sf := range sqlFiles {
			if sf.Error != nil {
				t.Fatalf("Unexpected error in file %s: %s", sf.Path(), sf.Error)
			}
			switch n {
			case 0:
				if includeSyntaxError {
					sf.Contents = strings.Replace(sf.Contents, "DEFAULT", "DEFALUT", 1)
				}
			case 1:
				sf.Contents = strings.ToLower(sf.Contents)
			case 2:
				sf.Contents = strings.Replace(sf.Contents, "`", "", -1)
			case 3:
				sf.Contents = strings.Replace(sf.Contents, "\n", " ", -1)
			}
			if _, err := sf.Write(); err != nil {
				t.Fatalf("Unable to rewrite %s: %s", sf.Path(), err)
			}
		}
	}
	rewriteFiles(false)
	s.HandleCommand(t, CodeDifferencesFound, "skeema lint")
	s.VerifyFiles(t, cfg, "../golden/init")

	// Add a new file with invalid SQL, and also make the previous valid rewrites.
	// Lint should rewrite the valid files but return exit code CodeFatalError due
	// to there being at least 1 file with invalid SQL.
	rewriteFiles(true)
	s.HandleCommand(t, CodeFatalError, "skeema lint")

	// Manually restore the file with invalid SQL; the files should now verify,
	// confirming that the fatal error did not prevent the other files from being
	// reformatted; re-linting should yield no changes.
	sqlFiles[0].Contents = strings.Replace(sqlFiles[0].Contents, "DEFALUT", "DEFAULT", 1)
	if _, err := sqlFiles[0].Write(); err != nil {
		t.Fatalf("Unable to rewrite %s: %s", sqlFiles[0].Path(), err)
	}
	s.VerifyFiles(t, cfg, "../golden/init")
	s.HandleCommand(t, CodeSuccess, "skeema lint")

	// Files with valid SQL, but not CREATE TABLE statements, should also trigger
	// CodeFatalError.
	sqlFiles[0].Contents = "INSERT INTO foo (col1, col2) VALUES (123, 456)"
	if _, err := sqlFiles[0].Write(); err != nil {
		t.Fatalf("Unable to rewrite %s: %s", sqlFiles[0].Path(), err)
	}
	s.HandleCommand(t, CodeFatalError, "skeema lint")
}

func (s *SkeemaIntegrationSuite) TestDiffHandler(t *testing.T) {
	s.HandleCommand(t, CodeSuccess, "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// no-op diff should yield no differences
	s.HandleCommand(t, CodeSuccess, "skeema diff")

	// --host and --schema have no effect if supplied on CLI
	s.HandleCommand(t, CodeSuccess, "skeema diff --host=1.2.3.4 --schema=whatever")

	// It isn't possible to disable --dry-run with diff
	cfg := s.HandleCommand(t, CodeSuccess, "skeema diff --skip-dry-run")
	if !cfg.GetBool("dry-run") {
		t.Error("Expected --skip-dry-run to have no effect on `skeema diff`, but it disabled dry-run")
	}

	s.execOrFatal(t, "analytics", "ALTER TABLE pageviews DROP COLUMN domain")
	s.HandleCommand(t, CodeDifferencesFound, "skeema diff")

	// Confirm --brief works as expected
	oldStdout := os.Stdout
	if outFile, err := os.Create("diff-brief.out"); err != nil {
		t.Fatalf("Unable to redirect stdout to a file: %s", err)
	} else {
		os.Stdout = outFile
		s.HandleCommand(t, CodeDifferencesFound, "skeema diff --brief")
		outFile.Close()
		os.Stdout = oldStdout
		expectOut := fmt.Sprintf("%s\n", s.d.Instance)
		if actualOut, err := ioutil.ReadFile("diff-brief.out"); err != nil {
			t.Fatalf("Unable to read diff-brief.out: %s", err)
		} else if string(actualOut) != expectOut {
			t.Errorf("Unexpected output from `skeema diff --brief`\nExpected:\n%sActual:\n%s", expectOut, string(actualOut))
		}
		if err := os.Remove("diff-brief.out"); err != nil {
			t.Fatalf("Unable to delete diff-brief.out: %s", err)
		}
	}
}

func (s *SkeemaIntegrationSuite) TestPushHandler(t *testing.T) {
	s.HandleCommand(t, CodeSuccess, "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Verify clean-slate operation: wipe the DB; push; wipe the files; re-init
	// the files; verify the files match. The push inherently verifies creation of
	// schemas and tables.
	if err := s.d.NukeData(); err != nil {
		t.Fatalf("Unable to nuke data: %s", err)
	}
	s.HandleCommand(t, CodeSuccess, "skeema push")
	s.ReinitAndVerifyFiles(t, "", "")

	// Test bad option values
	s.HandleCommand(t, CodeBadConfig, "skeema push --concurrent-instances=0")
	s.HandleCommand(t, CodeBadConfig, "skeema push --alter-algorithm=invalid")
	s.HandleCommand(t, CodeBadConfig, "skeema push --alter-lock=invalid")
	s.HandleCommand(t, CodeBadConfig, "skeema push --ignore-table='+'")

	// Make some changes on the db side, so that our next successful push attempt
	// will include dropping a table, dropping a col, adding a col, changing db
	// charset and collation
	if _, err := s.d.SourceSQL("../push1.sql"); err != nil {
		t.Fatalf("Unable to setup test: %s", err)
	}

	// push1.sql intentionally included no changes to the `product` schema, so push
	// from there should succeed and not impact anything in `analytics`
	if err := os.Chdir("mydb/product"); err != nil {
		t.Fatalf("Unable to cd to mydb/product: %s", err)
	}
	s.HandleCommand(t, CodeSuccess, "skeema push")
	s.AssertExists(t, "analytics", "widget_counts", "")
	s.AssertExists(t, "analytics", "activity", "rolled_up")
	s.AssertMissing(t, "analytics", "pageviews", "domain")

	// push from base dir, without any args, should succeed for safe changes but
	// not for unsafe ones. It also should not affect the `bonus` schema (which
	// exists on db but not on filesystem, but push should never drop schemas)
	if err := os.Chdir("../.."); err != nil {
		t.Fatalf("Unable to cd back to base dir: %s", err)
	}
	s.HandleCommand(t, CodeFatalError, "skeema push")       // CodeFatalError due to unsafe changes not being allowed
	s.AssertExists(t, "analytics", "widget_counts", "")     // not dropped by push (unsafe)
	s.AssertExists(t, "analytics", "activity", "rolled_up") // not dropped by push (unsafe)
	s.AssertExists(t, "analytics", "pageviews", "domain")   // re-created by push
	s.AssertExists(t, "bonus", "placeholder", "")           // not affected by push (never drops schemas)
	if analytics, err := s.d.Schema("analytics"); err != nil || analytics == nil {
		t.Fatalf("Unexpected error obtaining schema: %s", err)
	} else {
		serverCharSet, serverCollation, err := s.d.DefaultCharSetAndCollation()
		if err != nil {
			t.Fatalf("Unable to obtain server default charset and collation: %s", err)
		}
		if serverCharSet != analytics.CharSet || serverCollation != analytics.Collation {
			t.Errorf("Expected analytics schema to have charset/collation=%s/%s, instead found %s/%s", serverCharSet, serverCollation, analytics.CharSet, analytics.Collation)
		}
	}

	// push from base dir, with --safe-below-size=1, should allow the dropping of
	// col activity.rolled_up (table has no rows) but not table widget_counts
	// which has 1 row
	s.HandleCommand(t, CodeFatalError, "skeema push --safe-below-size=1") // CodeFatalError due to unsafe changes not being allowed
	s.AssertExists(t, "analytics", "widget_counts", "")
	s.AssertMissing(t, "analytics", "activity", "rolled_up")
	s.AssertExists(t, "analytics", "pageviews", "domain")
	s.AssertExists(t, "bonus", "placeholder", "")

	// push from base dir, with --allow-unsafe, will drop table widget_counts
	// despite it having 1 row
	s.HandleCommand(t, CodeSuccess, "skeema push --allow-unsafe")
	s.AssertMissing(t, "analytics", "widget_counts", "")
	s.AssertMissing(t, "analytics", "activity", "rolled_up")
	s.AssertExists(t, "analytics", "pageviews", "domain")
	s.AssertExists(t, "bonus", "placeholder", "")

}

func (s *SkeemaIntegrationSuite) TestAutoInc(t *testing.T) {
	// Insert 2 rows into product.users, so that next auto-inc value is now 3
	s.execOrFatal(t, "product", "INSERT INTO users (name) VALUES (?), (?)", "foo", "bar")

	// Normal init omits auto-inc values. diff views this as no differences.
	s.ReinitAndVerifyFiles(t, "", "")
	s.HandleCommand(t, CodeSuccess, "skeema diff")

	// pull and lint should make no changes
	cfg := s.HandleCommand(t, CodeSuccess, "skeema pull")
	s.VerifyFiles(t, cfg, "../golden/init")
	cfg = s.HandleCommand(t, CodeSuccess, "skeema lint")
	s.VerifyFiles(t, cfg, "../golden/init")

	// pull with --include-auto-inc should include auto-inc values greater than 1
	cfg = s.HandleCommand(t, CodeSuccess, "skeema pull --include-auto-inc")
	s.VerifyFiles(t, cfg, "../golden/autoinc")
	s.HandleCommand(t, CodeSuccess, "skeema diff")

	// Inserting another row should still be ignored by diffs
	s.execOrFatal(t, "product", "INSERT INTO users (name) VALUES (?)", "something")
	s.HandleCommand(t, CodeSuccess, "skeema diff")

	// However, if table's next auto-inc is LOWER than sqlfile's, this is a
	// difference.
	s.execOrFatal(t, "product", "DELETE FROM users WHERE id > 1")
	s.execOrFatal(t, "product", "ALTER TABLE users AUTO_INCREMENT=2")
	s.HandleCommand(t, CodeDifferencesFound, "skeema diff")
	s.HandleCommand(t, CodeSuccess, "skeema push")
	s.HandleCommand(t, CodeSuccess, "skeema diff")

	// init with --include-auto-inc should include auto-inc values greater than 1
	s.ReinitAndVerifyFiles(t, "--include-auto-inc", "../golden/autoinc")
}

func (s *SkeemaIntegrationSuite) TestUnsupportedAlter(t *testing.T) {
	s.execOrFatal(t, "product", "ALTER TABLE posts ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8")

	// init should work fine with an unsupported table
	s.ReinitAndVerifyFiles(t, "", "../golden/unsupported")

	// Back to clean slate
	if err := s.d.NukeData(); err != nil {
		t.Fatalf("Unable to clean slate: %s", err)
	}
	if _, err := s.d.SourceSQL("../setup.sql"); err != nil {
		t.Fatalf("Unable to re-source setup.sql: %s", err)
	}
	s.ReinitAndVerifyFiles(t, "", "../golden/init")

	// TODO: alter/add/drop; pull; diff; push; lint
}
