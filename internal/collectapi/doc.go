// Package collectapi exposes the runtime HTTP boundary for analytics intake and readback.
//
// Readback means trusted server-side APIs that read already accepted analytics
// events from storage for Realtime, Events, Goal, and property screens, plus
// trusted operator diagnostics that inspect local runtime state. It is different
// from browser collection, and its bearer query tokens must never be sent to browsers.
package collectapi
