package collectapi

import (
	"crypto/subtle"
	"strconv"
	"strings"
	"time"
)

// QueryCredential describes one internal readback bearer token and its lifecycle window.
type QueryCredential struct {
	ID        string    // ID is a non-secret alias emitted in runtime audit logs
	Token     string    // Token is the bearer secret accepted by the internal read API
	NotBefore time.Time // NotBefore optionally delays token activation until this instant
	ExpiresAt time.Time // ExpiresAt optionally rejects the token at or after this instant
}

type queryTokenAuthState string

const (
	queryTokenAuthUnknown      queryTokenAuthState = "unknown"
	queryTokenAuthAllowed      queryTokenAuthState = "allowed"
	queryTokenAuthAllowedGrace queryTokenAuthState = "allowed_grace"
	queryTokenAuthExpired      queryTokenAuthState = "expired"
	queryTokenAuthNotYetValid  queryTokenAuthState = "not_yet_valid"
)

type queryTokenAuthDecision struct {
	Credential QueryCredential
	State      queryTokenAuthState
}

func normalizeQueryCredentials(primary string, legacy []string, explicit []QueryCredential) []QueryCredential {
	// Prefer structured credentials so activation/expiry metadata survives, then
	// merge in legacy tokens for backward-compatible tests and callers.
	normalized := make([]QueryCredential, 0, len(explicit)+len(legacy)+1)
	seen := make(map[string]struct{}, len(explicit)+len(legacy)+1)
	appendCredential := func(credential QueryCredential, fallbackID string) {
		credential.Token = strings.TrimSpace(credential.Token)
		if credential.Token == "" {
			return
		}
		if _, ok := seen[credential.Token]; ok {
			return
		}
		seen[credential.Token] = struct{}{}
		credential.ID = strings.TrimSpace(credential.ID)
		if credential.ID == "" {
			credential.ID = fallbackID
		}
		normalized = append(normalized, credential)
	}

	for idx, credential := range explicit {
		fallbackID := "rotation-" + strconv.Itoa(idx)
		if idx == 0 {
			fallbackID = "current"
		}
		appendCredential(credential, fallbackID)
	}

	appendCredential(QueryCredential{ID: "current", Token: primary}, "current")
	for idx, token := range legacy {
		appendCredential(QueryCredential{Token: token}, "rotation-"+strconv.Itoa(idx+1))
	}
	return normalized
}

func authorizeQueryToken(value string, accepted []QueryCredential, now time.Time) queryTokenAuthDecision {
	// Compare all configured credentials so successful matches do not leak their
	// position through short-circuit string equality checks.
	value = strings.TrimSpace(value)
	if value == "" {
		return queryTokenAuthDecision{State: queryTokenAuthUnknown}
	}

	matched := -1
	for idx, credential := range accepted {
		if subtle.ConstantTimeCompare([]byte(value), []byte(credential.Token)) == 1 {
			matched = idx
		}
	}
	if matched < 0 {
		return queryTokenAuthDecision{State: queryTokenAuthUnknown}
	}

	credential := accepted[matched]
	if !credential.NotBefore.IsZero() && now.Before(credential.NotBefore) {
		return queryTokenAuthDecision{Credential: credential, State: queryTokenAuthNotYetValid}
	}
	if !credential.ExpiresAt.IsZero() && !now.Before(credential.ExpiresAt) {
		return queryTokenAuthDecision{Credential: credential, State: queryTokenAuthExpired}
	}
	if matched > 0 {
		return queryTokenAuthDecision{Credential: credential, State: queryTokenAuthAllowedGrace}
	}
	return queryTokenAuthDecision{Credential: credential, State: queryTokenAuthAllowed}
}
