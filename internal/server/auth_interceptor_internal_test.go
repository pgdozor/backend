package server

import (
	"strings"
	"testing"

	"github.com/pgdozor/backend/gen/pgdozor/v1/pgdozorv1connect"
)

func TestIsCollectorProcedure(t *testing.T) {
	t.Parallel()

	cases := []struct {
		procedure string
		want      bool
	}{
		{pgdozorv1connect.ActivityServiceReportActivityProcedure, true},
		{pgdozorv1connect.StatementServiceReportStatementsProcedure, true},
		{pgdozorv1connect.LogServiceReportLogsProcedure, true},
		{pgdozorv1connect.HealthServiceReportHealthProcedure, true},
		{pgdozorv1connect.ActivityServiceQueryTransactionsProcedure, false},
		{pgdozorv1connect.LogServiceQueryLogsProcedure, false},
		{pgdozorv1connect.AuthServiceLoginProcedure, false},
		{pgdozorv1connect.AdminServiceCreateUserProcedure, false},
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
		pgdozorv1connect.AdminServiceCreateUserProcedure,
		pgdozorv1connect.AdminServiceUpdateUserProcedure,
		pgdozorv1connect.AdminServiceDeleteUserProcedure,
		pgdozorv1connect.AdminServiceListUsersProcedure,
		pgdozorv1connect.AdminServiceCreateCollectorTokenProcedure,
		pgdozorv1connect.AdminServiceListCollectorTokensProcedure,
		pgdozorv1connect.AdminServiceDeleteCollectorTokenProcedure,
	}
	for _, procedure := range admin {
		if !strings.HasPrefix(procedure, adminServicePrefix) {
			t.Errorf("admin procedure %q not covered by prefix %q", procedure, adminServicePrefix)
		}
	}

	nonAdmin := []string{
		pgdozorv1connect.ActivityServiceQueryTransactionsProcedure,
		pgdozorv1connect.AlertServiceUpdateAlertSettingsProcedure,
		pgdozorv1connect.AuthServiceCurrentUserProcedure,
	}
	for _, procedure := range nonAdmin {
		if strings.HasPrefix(procedure, adminServicePrefix) {
			t.Errorf("non-admin procedure %q unexpectedly matched prefix %q", procedure, adminServicePrefix)
		}
	}
}
