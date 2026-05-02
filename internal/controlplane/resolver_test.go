package controlplane

import "testing"

func TestMemoryResolverRejectsDuplicateWriteKeys(t *testing.T) {
	_, err := NewMemoryResolver([]SourceConfig{
		{
			WriteKey:  "wk_live",
			Enabled:   true,
			TenantID:  "tenant_1",
			ProjectID: "project_1",
			SourceID:  "source_1",
		},
		{
			WriteKey:  " wk_live ",
			Enabled:   true,
			TenantID:  "tenant_2",
			ProjectID: "project_2",
			SourceID:  "source_2",
		},
	})
	if err == nil {
		t.Fatalf("expected duplicate write key to be rejected")
	}
}
