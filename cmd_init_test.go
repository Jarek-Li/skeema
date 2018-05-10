package main

import (
	"testing"
)

func (s *SkeemaIntegrationSuite) TestInitHandler(t *testing.T) {
	cfg := s.HandleCommand(t, CodeSuccess, "skeema init --dir mydb -h %s -P %d", s.d.Instance.Host, s.d.Instance.Port)
	s.VerifyFiles(t, cfg, "../golden/init")
}
