package main

import (
	"fmt"
	"testing"

	"github.com/skeema/mybase"
)

func (s *SkeemaIntegrationSuite) TestPullHandler(t *testing.T) {
	s.doInitSetup(t)

	// In product db, alter one table and drop one table;
	// In analytics db, add one table and alter the schema's charset and collation;
	// Create a new db and put one table in it
	if _, err := s.d.SourceSQL("../pull1.sql"); err != nil {
		t.Fatalf("Unable to setup test: %s", err)
	}

	commandLine := fmt.Sprintf("skeema pull -p%s", s.d.Instance.Password)
	cfg := mybase.ParseFakeCLI(t, CommandSuite, commandLine)
	if err := cfg.HandleCommand(); err != nil {
		t.Fatalf("Error returned: %s", err)
	}
	s.VerifyFiles(t, cfg, "../golden/pull1")
}
