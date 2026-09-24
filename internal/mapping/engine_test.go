package mapping

import (
	"testing"
	"time"

	"github.com/SomethingCreativeStudios/hronir/internal/config"
	"github.com/SomethingCreativeStudios/hronir/internal/model"
)

func TestProfileMapsMissingFieldsWithoutNullChurn(t *testing.T) {
	engine, err := New(config.MappingConfig{Profile: "connected-systems/v1"})
	if err != nil {
		t.Fatal(err)
	}
	record, err := engine.Map(model.Resource{Kind: "systems", ID: "one", UID: "urn:one", CanonicalURL: "https://source/systems/one", Name: "  System One  ", Metadata: map[string]any{}}, "primary", "https://source", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if record["properties"].(map[string]any)["title"] != "System One" {
		t.Fatalf("unexpected title: %#v", record)
	}
	if _, found := record["properties"].(map[string]any)["description"]; found {
		t.Fatal("missing description should be omitted")
	}
}

func TestOverlayExpression(t *testing.T) {
	engine, err := New(config.MappingConfig{Profile: "connected-systems/v1", Overlays: map[string]config.ResourceMapping{"systems": {Fields: map[string]config.FieldOverride{"/properties/region": {Expr: "= $.metadata.region | upper"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	record, err := engine.Map(model.Resource{Kind: "systems", ID: "one", CanonicalURL: "https://source/systems/one", Metadata: map[string]any{"region": "north"}}, "primary", "https://source", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if record["properties"].(map[string]any)["region"] != "NORTH" {
		t.Fatalf("unexpected record: %#v", record)
	}
}
