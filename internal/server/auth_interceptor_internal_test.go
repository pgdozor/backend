package server

import (
	"strings"
	"testing"

	"github.com/querysheriff/backend/gen/querysheriff/v1/querysheriffv1connect"
)

func TestIsCollectorProcedure(t *testing.T) {
	t.Parallel()

	cases := []struct {
		procedure string
		want      bool
	}{
		{querysheriffv1connect.ActivityServiceReportActivityProcedure, true},
		{querysheriffv1connect.StatementServiceReportStatementsProcedure, true},
		{querysheriffv1connect.StatementServiceReportStatementTextsProcedure, true},
		{querysheriffv1connect.LogServiceReportLogsProcedure, true},
		{querysheriffv1connect.HealthServiceReportHealthProcedure, true},
		{querysheriffv1connect.ActivityServiceQueryTransactionsProcedure, false},
		{querysheriffv1connect.LogServiceQueryLogsProcedure, false},
		{querysheriffv1connect.AuthServiceLoginProcedure, false},
		{querysheriffv1connect.AdminServiceCreateUserProcedure, false},
	}

	for _, c := range cases {
		if got := isCollectorProcedure(c.procedure); got != c.want {
			t.Errorf("isCollectorProcedure(%q) = %v, want %v", c.procedure, got, c.want)
		}
	}
}

func TestAdminServicePrefixMatchesGeneratedNamespace(t *testing.T) {
	t.Parallel()

	admin := []string{
		querysheriffv1connect.AdminServiceCreateUserProcedure,
		querysheriffv1connect.AdminServiceUpdateUserProcedure,
		querysheriffv1connect.AdminServiceDeleteUserProcedure,
		querysheriffv1connect.AdminServiceListUsersProcedure,
		querysheriffv1connect.AdminServiceCreateCollectorTokenProcedure,
		querysheriffv1connect.AdminServiceListCollectorTokensProcedure,
		querysheriffv1connect.AdminServiceDeleteCollectorTokenProcedure,
	}
	for _, procedure := range admin {
		if !strings.HasPrefix(procedure, adminServicePrefix) {
			t.Errorf("admin procedure %q not covered by prefix %q", procedure, adminServicePrefix)
		}
	}

	nonAdmin := []string{
		querysheriffv1connect.ActivityServiceQueryTransactionsProcedure,
		querysheriffv1connect.AlertServiceUpdateAlertSettingsProcedure,
		querysheriffv1connect.AuthServiceCurrentUserProcedure,
	}
	for _, procedure := range nonAdmin {
		if strings.HasPrefix(procedure, adminServicePrefix) {
			t.Errorf("non-admin procedure %q unexpectedly matched prefix %q", procedure, adminServicePrefix)
		}
	}
}
