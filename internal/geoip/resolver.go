// Package geoip adapts an offline MaxMind-compatible database to collect enrichment.
package geoip

import (
	"net/netip"
	"strings"

	"github.com/oschwald/maxminddb-golang/v2"
	"github.com/simpletrack/analytics-core/collect"
)

// Resolver resolves client IP addresses through a local MaxMind DB file.
type Resolver struct {
	reader *maxminddb.Reader // reader owns the memory-mapped mmdb file
}

// NewResolver opens a local mmdb file for runtime geo enrichment.
func NewResolver(path string) (*Resolver, error) {
	// Keep deployment configuration outside source config. The SaaS control
	// plane may decide whether geo is useful, but only this runtime process knows
	// which offline file is mounted on disk.
	reader, err := maxminddb.Open(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}
	return &Resolver{reader: reader}, nil
}

// Resolve returns coarse country, region, and city dimensions for ip.
func (r *Resolver) Resolve(ip string) (collect.GeoLocation, bool) {
	if r == nil || r.reader == nil {
		return collect.GeoLocation{}, false
	}
	addr, err := netip.ParseAddr(strings.TrimSpace(ip))
	if err != nil {
		return collect.GeoLocation{}, false
	}

	// Decode only the fields needed by analytics-core. This keeps the runtime
	// path cheaper than decoding the full GeoIP2 record on every collect request.
	result := r.reader.Lookup(addr)
	if !result.Found() {
		return collect.GeoLocation{}, false
	}
	var record cityRecord
	if err := result.Decode(&record); err != nil {
		return collect.GeoLocation{}, false
	}
	location := collect.GeoLocation{
		Country: firstName(record.Country.Names, record.Country.ISOCode),
		Region:  firstSubdivisionName(record.Subdivisions),
		City:    firstName(record.City.Names, ""),
	}
	if location.Country == "" && location.Region == "" && location.City == "" {
		return collect.GeoLocation{}, false
	}
	return location, true
}

// Close releases the memory-mapped database file.
func (r *Resolver) Close() error {
	if r == nil || r.reader == nil {
		return nil
	}
	return r.reader.Close()
}

type cityRecord struct {
	Country      countryRecord       `maxminddb:"country"`      // Country is the country node in GeoLite2-City and GeoIP2-City databases
	Subdivisions []subdivisionRecord `maxminddb:"subdivisions"` // Subdivisions are ordered from broad to narrow region levels
	City         namedRecord         `maxminddb:"city"`         // City is the city node when the database contains city-level data
}

type countryRecord struct {
	ISOCode string            `maxminddb:"iso_code"` // ISOCode is used when localized names are unavailable
	Names   map[string]string `maxminddb:"names"`    // Names contains localized country names keyed by locale
}

type subdivisionRecord struct {
	ISOCode string            `maxminddb:"iso_code"` // ISOCode is used when localized names are unavailable
	Names   map[string]string `maxminddb:"names"`    // Names contains localized subdivision names keyed by locale
}

type namedRecord struct {
	Names map[string]string `maxminddb:"names"` // Names contains localized city names keyed by locale
}

func firstSubdivisionName(values []subdivisionRecord) string {
	if len(values) == 0 {
		return ""
	}
	return firstName(values[0].Names, values[0].ISOCode)
}

func firstName(names map[string]string, fallback string) string {
	if value := strings.TrimSpace(names["en"]); value != "" {
		return value
	}
	if value := strings.TrimSpace(names["zh-CN"]); value != "" {
		return value
	}
	if value := strings.TrimSpace(fallback); value != "" {
		return value
	}
	for _, value := range names {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}
