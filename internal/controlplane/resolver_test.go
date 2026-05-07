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
			VisitSalt:      "visit-salt-1",
			ClientHashSalt: "client-salt-1",
		},
		{
			WriteKey:       " wk_live ",
			Enabled:        true,
			TenantID:       "tenant_2",
			ProjectID:      "project_2",
			SourceID:       "source_2",
			SessionSalt:    "session-salt-2",
			VisitSalt:      "visit-salt-2",
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

func TestSourceConfigAllowsOnlyConfiguredPropertyFilters(t *testing.T) {
	source := SourceConfig{
		AllowedPropertyFilters: []AllowedPropertyFilter{
			{Scope: " Event ", Name: " button ", ValueTypes: []string{" String ", "NUMBER"}},
			{Scope: "user", Name: "plan"},
		},
	}.Normalize()

	if !source.AllowsPropertyFilter("event", "button", "string") {
		t.Fatalf("expected event.button string filter to be allowed")
	}
	if !source.AllowsPropertyFilter("event", "button", "number") {
		t.Fatalf("expected event.button number filter to be allowed")
	}
	if !source.AllowsPropertyFilter("user", "plan", "bool") {
		t.Fatalf("expected user.plan to allow any value type")
	}
	if source.AllowsPropertyFilter("event", "button", "bool") {
		t.Fatalf("expected event.button bool filter to be rejected")
	}
	if source.AllowsPropertyFilter("event", "unknown", "string") {
		t.Fatalf("expected unknown property filter to be rejected")
	}
}

func TestMemoryResolverRejectsInvalidPropertyFilterConfig(t *testing.T) {
	_, err := NewMemoryResolver([]SourceConfig{
		{
			WriteKey:       "wk_live",
			Enabled:        true,
			TenantID:       "tenant_1",
			ProjectID:      "project_1",
			SourceID:       "source_1",
			SessionSalt:    "session-salt",
			VisitSalt:      "visit-salt",
			ClientHashSalt: "client-salt",
			AllowedPropertyFilters: []AllowedPropertyFilter{
				{Scope: "account", Name: "plan", ValueTypes: []string{"string"}},
			},
		},
	})
	if err == nil {
		t.Fatalf("expected invalid property filter scope to be rejected")
	}
}
