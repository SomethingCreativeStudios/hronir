package config

import (
	"strings"
	"testing"
)

func TestDecodeDefaultsAndRejectsUnknownFields(t *testing.T) {
	valid := `apiVersion: hronir.io/v1alpha1
kind: Harvester
target: {url: https://records.example}
sources:
  - id: primary
    url: https://connected.example/api
mapping: {profile: connected-systems/v1, overlays: {}, functions: []}
`
	cfg, err := Decode(strings.NewReader(valid))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Target.CatalogID != "connected-systems" || cfg.Runtime.PollInterval.String() != "1h0m0s" {
		t.Fatalf("defaults were not applied: %#v", cfg)
	}
	invalid := strings.Replace(valid, "target:", "unknown: true\ntarget:", 1)
	if _, err := Decode(strings.NewReader(invalid)); err == nil {
		t.Fatal("unknown field should fail")
	}
}

func TestLiteralObjectMappingIsAllowed(t *testing.T) {
	valid := `apiVersion: hronir.io/v1alpha1
kind: Harvester
target: {url: https://records.example}
sources: [{id: primary, url: https://connected.example}]
mapping:
  profile: connected-systems/v1
  overlays:
    systems:
      fields:
        /properties/example:
          value: {label: literal}
          schema: {type: object}
          queryable: false
          sortable: false
          facet: false
`
	if _, err := Decode(strings.NewReader(valid)); err != nil {
		t.Fatal(err)
	}
}
