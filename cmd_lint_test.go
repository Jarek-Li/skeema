package main

import (
	"strings"
	"testing"
)

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
