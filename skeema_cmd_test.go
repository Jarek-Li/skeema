package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func (s *SkeemaIntegrationSuite) TestInitHandler(t *testing.T) {
	s.handleCommand(t, CodeBadConfig, ".", "skeema init") // no host

	// Invalid environment name
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir mydb -h %s -P %d '[nope]'", s.d.Instance.Host, s.d.Instance.Port)

	// Specifying a single schema that doesn't exist on the instance
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir mydb -h %s -P %d --schema doesntexist", s.d.Instance.Host, s.d.Instance.Port)

	// Successful standard execution. Also confirm user is not persisted to .skeema
	// since not specified on CLI.
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	s.verifyFiles(t, cfg, "../golden/init")
	if _, setsOption := getOptionFile(t, "mydb", cfg).OptionValue("user"); setsOption {
		t.Error("Did not expect user to be persisted to .skeema, but it was")
	}

	// Specifying an unreachable host should fail with fatal error
	s.handleCommand(t, CodeFatalError, ".", "skeema init --dir baddb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port-100)

	// host-wrapper with no output should fail
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir baddb -h xyz --host-wrapper='echo'")

	// Test successful init with --user specified on CLI, persisting to .skeema
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema init --dir withuser -h %s -P %d --user root", s.d.Instance.Host, s.d.Instance.Port)
	if _, setsOption := getOptionFile(t, "withuser", cfg).OptionValue("user"); !setsOption {
		t.Error("Expected user to be persisted to .skeema, but it was not")
	}

	// Can't init into a dir with existing option file
	s.handleCommand(t, CodeBadConfig, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Can't init off of base dir that already specifies a schema
	s.handleCommand(t, CodeBadConfig, "mydb/product", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Test successful init for a single schema. Source a SQL file first that,
	// among other things, changes the default charset and collation for the
	// schema in question.
	s.sourceSQL(t, "push1.sql")
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema init --dir combined -h %s -P %d --schema analytics", s.d.Instance.Host, s.d.Instance.Port)
	dir, err := NewDir("combined", cfg)
	if err != nil {
		t.Fatalf("Unexpected error from NewDir: %s", err)
	}
	optionFile := getOptionFile(t, "combined", cfg)
	for _, option := range []string{"host", "schema", "default-character-set", "default-collation"} {
		if _, setsOption := optionFile.OptionValue(option); !setsOption {
			t.Errorf("Expected .skeema to contain %s, but it does not", option)
		}
	}
	if subdirs, err := dir.Subdirs(); err != nil {
		t.Fatalf("Unexpected error listing subdirs of %s: %s", dir, err)
	} else if len(subdirs) > 0 {
		t.Errorf("Expected %s to have no subdirs, but it has %d", dir, len(subdirs))
	}
	if sqlFiles, err := dir.SQLFiles(); err != nil {
		t.Fatalf("Unexpected error listing *.sql in %s: %s", dir, err)
	} else if len(sqlFiles) < 1 {
		t.Errorf("Expected %s to have *.sql files, but it does not", dir)
	}

	// Test successful init without a --dir
	expectDir := fmt.Sprintf("%s:%d", s.d.Instance.Host, s.d.Instance.Port)
	if _, err = os.Stat(expectDir); err == nil {
		t.Fatalf("Expected dir %s to not exist yet, but it does", expectDir)
	}
	s.handleCommand(t, CodeSuccess, ".", "skeema init -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	if _, err = os.Stat(expectDir); err != nil {
		t.Fatalf("Expected dir %s to exist now, but it does not", expectDir)
	}

	// init should fail if a parent dir has an invalid .skeema file
	makeDir(t, "hasbadoptions")
	writeFile(t, "hasbadoptions/.skeema", "invalid file will not parse")
	s.handleCommand(t, CodeFatalError, "hasbadoptions", "skeema init -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// init should fail if the --dir specifies an existing non-directory file; or
	// if the --dir already contains a subdir matching a schema name; or if the
	// --dir already contains a .sql file and --schema was used to only do 1 level
	writeFile(t, "nondir", "foo bar")
	s.handleCommand(t, CodeCantCreate, ".", "skeema init --dir nondir -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	makeDir(t, "alreadyexists/product")
	s.handleCommand(t, CodeCantCreate, ".", "skeema init --dir alreadyexists -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	makeDir(t, "hassql")
	writeFile(t, "hassql/foo.sql", "foo")
	s.handleCommand(t, CodeFatalError, ".", "skeema init --dir hassql --schema product -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
}

func (s *SkeemaIntegrationSuite) TestAddEnvHandler(t *testing.T) {
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// add-environment should fail on a dir that does not exist
	s.handleCommand(t, CodeBadConfig, ".", "skeema add-environment --host my.staging.db.com --dir does/not/exist staging")

	// add-environment should fail on a dir that does not already contain a .skeema file
	s.handleCommand(t, CodeBadConfig, ".", "skeema add-environment --host my.staging.db.com staging")

	// bad environment name should fail
	s.handleCommand(t, CodeBadConfig, ".", "skeema add-environment --host my.staging.db.com --dir mydb '[staging]'")

	// preexisting environment name should fail
	s.handleCommand(t, CodeBadConfig, ".", "skeema add-environment --host my.staging.db.com --dir mydb production")

	// non-host-level directory should fail
	s.handleCommand(t, CodeBadConfig, ".", "skeema add-environment --host my.staging.db.com --dir mydb/product staging")

	// lack of host on CLI should fail
	s.handleCommand(t, CodeBadConfig, ".", "skeema add-environment --dir mydb staging")

	// None of the above failed commands should have modified any files
	s.verifyFiles(t, cfg, "../golden/init")
	origFile := getOptionFile(t, "mydb", cfg)

	// valid dir should succeed and add the section to the .skeema file
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema add-environment --host my.staging.db.com --dir mydb staging")
	file := getOptionFile(t, "mydb", cfg)
	origFile.SetOptionValue("staging", "host", "my.staging.db.com")
	origFile.SetOptionValue("staging", "port", "3306")
	if !origFile.SameContents(file) {
		t.Fatalf("File contents of %s do not match expectation", file.Path())
	}

	// Nonstandard port should work properly; ditto for user option persisting
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema add-environment --host my.ci.db.com -P 3307 -ufoobar --dir mydb ci")
	file = getOptionFile(t, "mydb", cfg)
	origFile.SetOptionValue("ci", "host", "my.ci.db.com")
	origFile.SetOptionValue("ci", "port", "3307")
	origFile.SetOptionValue("ci", "user", "foobar")
	if !origFile.SameContents(file) {
		t.Fatalf("File contents of %s do not match expectation", file.Path())
	}

	// localhost and socket should work properly
	s.handleCommand(t, CodeSuccess, ".", "skeema add-environment -h localhost -S /var/lib/mysql/mysql.sock --dir mydb development")
	file = getOptionFile(t, "mydb", cfg)
	origFile.SetOptionValue("development", "host", "localhost")
	origFile.SetOptionValue("development", "socket", "/var/lib/mysql/mysql.sock")
	if !origFile.SameContents(file) {
		t.Fatalf("File contents of %s do not match expectation", file.Path())
	}
}

func (s *SkeemaIntegrationSuite) TestPullHandler(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// In product db, alter one table and drop one table;
	// In analytics db, add one table and alter the schema's charset and collation;
	// Create a new db and put one table in it
	s.sourceSQL(t, "pull1.sql")
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/pull1")

	// Revert db back to previous state, and pull again to test the opposite
	// behaviors: delete dir for new schema, remove charset/collation from .skeema,
	// etc
	s.cleanData(t, "setup.sql")
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/init")
}

func (s *SkeemaIntegrationSuite) TestLintHandler(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Initial lint should be a no-op that returns exit code 0
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema lint")
	s.verifyFiles(t, cfg, "../golden/init")

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
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema lint")
	s.verifyFiles(t, cfg, "../golden/init")

	// Add a new file with invalid SQL, and also make the previous valid rewrites.
	// Lint should rewrite the valid files but return exit code CodeFatalError due
	// to there being at least 1 file with invalid SQL.
	rewriteFiles(true)
	s.handleCommand(t, CodeFatalError, ".", "skeema lint")

	// Manually restore the file with invalid SQL; the files should now verify,
	// confirming that the fatal error did not prevent the other files from being
	// reformatted; re-linting should yield no changes.
	sqlFiles[0].Contents = strings.Replace(sqlFiles[0].Contents, "DEFALUT", "DEFAULT", 1)
	if _, err := sqlFiles[0].Write(); err != nil {
		t.Fatalf("Unable to rewrite %s: %s", sqlFiles[0].Path(), err)
	}
	s.verifyFiles(t, cfg, "../golden/init")
	s.handleCommand(t, CodeSuccess, ".", "skeema lint")

	// Files with valid SQL, but not CREATE TABLE statements, should also trigger
	// CodeFatalError.
	sqlFiles[0].Contents = "INSERT INTO foo (col1, col2) VALUES (123, 456)"
	if _, err := sqlFiles[0].Write(); err != nil {
		t.Fatalf("Unable to rewrite %s: %s", sqlFiles[0].Path(), err)
	}
	s.handleCommand(t, CodeFatalError, ".", "skeema lint")
}

func (s *SkeemaIntegrationSuite) TestDiffHandler(t *testing.T) {
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// no-op diff should yield no differences
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// --host and --schema have no effect if supplied on CLI
	s.handleCommand(t, CodeSuccess, ".", "skeema diff --host=1.2.3.4 --schema=whatever")

	// It isn't possible to disable --dry-run with diff
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema diff --skip-dry-run")
	if !cfg.GetBool("dry-run") {
		t.Error("Expected --skip-dry-run to have no effect on `skeema diff`, but it disabled dry-run")
	}

	s.dbExec(t, "analytics", "ALTER TABLE pageviews DROP COLUMN domain")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff")

	// Confirm --brief works as expected
	oldStdout := os.Stdout
	if outFile, err := os.Create("diff-brief.out"); err != nil {
		t.Fatalf("Unable to redirect stdout to a file: %s", err)
	} else {
		os.Stdout = outFile
		s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --brief")
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
	s.handleCommand(t, CodeSuccess, ".", "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Verify clean-slate operation: wipe the DB; push; wipe the files; re-init
	// the files; verify the files match. The push inherently verifies creation of
	// schemas and tables.
	s.cleanData(t)
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.reinitAndVerifyFiles(t, "", "")

	// Test bad option values
	s.handleCommand(t, CodeBadConfig, ".", "skeema push --concurrent-instances=0")
	s.handleCommand(t, CodeBadConfig, ".", "skeema push --alter-algorithm=invalid")
	s.handleCommand(t, CodeBadConfig, ".", "skeema push --alter-lock=invalid")
	s.handleCommand(t, CodeBadConfig, ".", "skeema push --ignore-table='+'")

	// Make some changes on the db side, so that our next successful push attempt
	// will include dropping a table, dropping a col, adding a col, changing db
	// charset and collation
	s.sourceSQL(t, "push1.sql")

	// push1.sql intentionally included no changes to the `product` schema, so push
	// from there should succeed and not impact anything in `analytics`
	s.handleCommand(t, CodeSuccess, "mydb/product", "skeema push")
	s.assertExists(t, "analytics", "widget_counts", "")
	s.assertExists(t, "analytics", "activity", "rolled_up")
	s.assertMissing(t, "analytics", "pageviews", "domain")

	// push from base dir, without any args, should succeed for safe changes but
	// not for unsafe ones. It also should not affect the `bonus` schema (which
	// exists on db but not on filesystem, but push should never drop schemas)
	s.handleCommand(t, CodeFatalError, ".", "skeema push")  // CodeFatalError due to unsafe changes not being allowed
	s.assertExists(t, "analytics", "widget_counts", "")     // not dropped by push (unsafe)
	s.assertExists(t, "analytics", "activity", "rolled_up") // not dropped by push (unsafe)
	s.assertExists(t, "analytics", "pageviews", "domain")   // re-created by push
	s.assertExists(t, "bonus", "placeholder", "")           // not affected by push (never drops schemas)
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
	s.handleCommand(t, CodeFatalError, ".", "skeema push --safe-below-size=1") // CodeFatalError due to unsafe changes not being allowed
	s.assertExists(t, "analytics", "widget_counts", "")
	s.assertMissing(t, "analytics", "activity", "rolled_up")
	s.assertExists(t, "analytics", "pageviews", "domain")
	s.assertExists(t, "bonus", "placeholder", "")

	// push from base dir, with --allow-unsafe, will drop table widget_counts
	// despite it having 1 row
	s.handleCommand(t, CodeSuccess, ".", "skeema push --allow-unsafe")
	s.assertMissing(t, "analytics", "widget_counts", "")
	s.assertMissing(t, "analytics", "activity", "rolled_up")
	s.assertExists(t, "analytics", "pageviews", "domain")
	s.assertExists(t, "bonus", "placeholder", "")
}

func (s *SkeemaIntegrationSuite) TestAutoInc(t *testing.T) {
	// Insert 2 rows into product.users, so that next auto-inc value is now 3
	s.dbExec(t, "product", "INSERT INTO users (name) VALUES (?), (?)", "foo", "bar")

	// Normal init omits auto-inc values. diff views this as no differences.
	s.reinitAndVerifyFiles(t, "", "")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// pull and lint should make no changes
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/init")
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema lint")
	s.verifyFiles(t, cfg, "../golden/init")

	// pull with --include-auto-inc should include auto-inc values greater than 1
	cfg = s.handleCommand(t, CodeSuccess, ".", "skeema pull --include-auto-inc")
	s.verifyFiles(t, cfg, "../golden/autoinc")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// Inserting another row should still be ignored by diffs
	s.dbExec(t, "product", "INSERT INTO users (name) VALUES (?)", "something")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// However, if table's next auto-inc is LOWER than sqlfile's, this is a
	// difference.
	s.dbExec(t, "product", "DELETE FROM users WHERE id > 1")
	s.dbExec(t, "product", "ALTER TABLE users AUTO_INCREMENT=2")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.handleCommand(t, CodeSuccess, ".", "skeema diff")

	// init with --include-auto-inc should include auto-inc values greater than 1
	s.reinitAndVerifyFiles(t, "--include-auto-inc", "../golden/autoinc")
}

func (s *SkeemaIntegrationSuite) TestUnsupportedAlter(t *testing.T) {
	s.sourceSQL(t, "unsupported1.sql")

	// init should work fine with an unsupported table
	s.reinitAndVerifyFiles(t, "", "../golden/unsupported")

	// Back to clean slate for db and files
	s.cleanData(t, "setup.sql")
	s.reinitAndVerifyFiles(t, "", "../golden/init")

	// apply change to db directly, and confirm pull still works
	s.sourceSQL(t, "unsupported1.sql")
	cfg := s.handleCommand(t, CodeSuccess, ".", "skeema pull")
	s.verifyFiles(t, cfg, "../golden/unsupported")

	// back to clean slate for db only
	s.cleanData(t, "setup.sql")

	// lint should be able to fix formatting problems in unsupported table files
	contents := readFile(t, "mydb/product/subscriptions.sql")
	writeFile(t, "mydb/product/subscriptions.sql", strings.Replace(contents, "`", "", -1))
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema lint")
	s.verifyFiles(t, cfg, "../golden/unsupported")

	// diff should return CodeDifferencesFound, vs push should return
	// CodePartialError
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --debug")
	s.handleCommand(t, CodePartialError, ".", "skeema push")

	// diff/push still ok if *creating* or *dropping* unsupported table
	s.dbExec(t, "product", "DROP TABLE subscriptions")
	s.assertMissing(t, "product", "subscriptions", "")
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff")
	s.handleCommand(t, CodeSuccess, ".", "skeema push")
	s.assertExists(t, "product", "subscriptions", "")
	s.sourceSQL(t, "unsupported1.sql")
	if err := os.Remove("mydb/product/subscriptions.sql"); err != nil {
		t.Fatalf("Unexpected error removing a file: %s", err)
	}
	s.handleCommand(t, CodeDifferencesFound, ".", "skeema diff --allow-unsafe")
	s.handleCommand(t, CodeSuccess, ".", "skeema push --allow-unsafe")
	s.assertMissing(t, "product", "subscriptions", "")
}
