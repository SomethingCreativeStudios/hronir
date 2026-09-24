package catalog

import (
	"encoding/json"
	"testing"

	"github.com/SomethingCreativeStudios/hronir/internal/config"
)

func TestRenderAddsDeclaredCustomQueryable(t *testing.T) {
	value := true
	cfg := config.Config{Target: config.TargetConfig{CatalogID: "connected-systems"}, Mapping: config.MappingConfig{Overlays: map[string]config.ResourceMapping{"systems": {Fields: map[string]config.FieldOverride{"/properties/region": {Expr: "= $.metadata.region", Schema: map[string]any{"type": "string"}, Queryable: &value, Sortable: &value, Facet: &value}}}}}}
	data, err := RenderTlon(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var bundle map[string]any
	if err := json.Unmarshal(data, &bundle); err != nil {
		t.Fatal(err)
	}
	if _, found := bundle["queryables"].(map[string]any)["region"]; !found {
		t.Fatalf("missing region queryable: %s", data)
	}
}
