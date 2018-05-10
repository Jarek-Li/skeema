package main

import (
	"testing"
)

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
