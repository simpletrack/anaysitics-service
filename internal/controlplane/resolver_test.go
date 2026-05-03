package controlplane

import "testing"

func TestMemoryResolverRejectsDuplicateWriteKeys(t *testing.T) {
	_, err := NewMemoryResolver([]SourceConfig{
		{
			WriteKey:       "wk_live",
			Enabled:        true,
			TenantID:       "tenant_1",
			ProjectID:      "project_1",
			SourceID:       "source_1",
			SessionSalt:    "session-salt-1",
			ClientHashSalt: "client-salt-1",
		},
		{
			WriteKey:       " wk_live ",
			Enabled:        true,
			TenantID:       "tenant_2",
			ProjectID:      "project_2",
			SourceID:       "source_2",
			SessionSalt:    "session-salt-2",
			ClientHashSalt: "client-salt-2",
		},
	})
	if err == nil {
		t.Fatalf("expected duplicate write key to be rejected")
	}
}

func TestMemoryResolverRequiresPrivateSalts(t *testing.T) {
	_, err := NewMemoryResolver([]SourceConfig{
		{
			WriteKey:  "wk_live",
			Enabled:   true,
			TenantID:  "tenant_1",
			ProjectID: "project_1",
			SourceID:  "source_1",
		},
	})
	if err == nil {
		t.Fatalf("expected missing private salts to be rejected")
	}
}
