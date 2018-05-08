package main

import (
	"fmt"
	"testing"

	"github.com/skeema/mybase"
)

func (s *SkeemaIntegrationSuite) TestInitHandler(t *testing.T) {
	commandLine := fmt.Sprintf("skeema init --dir mydb -h %s -P %d -p%s", s.d.Instance.Host, s.d.Instance.Port, s.d.Instance.Password)
	cfg := mybase.ParseFakeCLI(t, CommandSuite, commandLine)
	if err := cfg.HandleCommand(); err != nil {
		t.Fatalf("Error returned: %s", err)
	}
	s.VerifyFiles(t, cfg, "../golden/init")
}
