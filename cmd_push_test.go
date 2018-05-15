package main

import (
	"os"
	"testing"
)

func (s *SkeemaIntegrationSuite) TestPushHandler(t *testing.T) {
	s.HandleCommand(t, CodeSuccess, "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)

	// Verify clean-slate operation: wipe the DB; push; wipe the files; re-init
	// the files; verify the files match. The push inherently verifies creation of
	// schemas and tables.
	if err := s.d.NukeData(); err != nil {
		t.Fatalf("Unable to nuke data: %s", err)
	}
	s.HandleCommand(t, CodeSuccess, "skeema push")
	s.ReinitAndVerify(t, "")

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

}
