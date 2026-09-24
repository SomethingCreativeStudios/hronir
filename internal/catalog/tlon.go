// Package catalog renders the Records catalog bundle required by Hronir's target.
package catalog

import (
	"encoding/json"

	"github.com/SomethingCreativeStudios/hronir/internal/config"
)

func RenderTlon(cfg config.Config) ([]byte, error) {
	bundle := map[string]any{
		"catalog": map[string]any{"id": cfg.Target.CatalogID, "type": "Collection", "itemType": "record", "title": "Connected Systems metadata", "description": "Materialized Connected Systems metadata managed by Hronir."},
		"storage": map[string]any{"class": "transactional", "autoFacetIndexes": true},
		"queryables": map[string]any{
			"sourceId": map[string]any{"title": "Source", "type": "string", "x-tlon-path": "/properties/hronir/sourceId"},
			"kind":     map[string]any{"title": "Connected Systems kind", "type": "string", "x-tlon-path": "/properties/connectedSystems/kind"},
			"title":    map[string]any{"title": "Title", "type": "string", "x-tlon-path": "/properties/title"},
		},
		"sortables":        map[string]any{"title": map[string]any{"type": "string", "x-tlon-path": "/properties/title"}, "generatedAt": map[string]any{"type": "string", "format": "date-time", "x-tlon-path": "/properties/hronir/generatedAt"}},
		"defaultSortOrder": []any{map[string]any{"property": "title", "direction": "asc"}},
		"facets":           map[string]any{"sources": map[string]any{"type": "term", "property": "sourceId", "default": true, "bucketCount": 20, "sortedBy": "count", "minOccurs": 1}, "kinds": map[string]any{"type": "term", "property": "kind", "default": true, "bucketCount": 20, "sortedBy": "count", "minOccurs": 1}},
		"schema":           schema(),
	}
	for _, overlay := range cfg.Mapping.Overlays {
		for pointer, field := range overlay.Fields {
			if field.Schema == nil || field.Queryable == nil {
				continue
			}
			property := pointerName(pointer)
			if property == "" {
				continue
			}
			path := pointer
			if *field.Queryable {
				bundle["queryables"].(map[string]any)[property] = withPath(field.Schema, path)
			}
			if *field.Sortable {
				bundle["sortables"].(map[string]any)[property] = withPath(field.Schema, path)
			}
			if *field.Facet {
				bundle["facets"].(map[string]any)[property+"s"] = map[string]any{"type": "term", "property": property, "bucketCount": 20, "sortedBy": "count", "minOccurs": 1}
			}
		}
	}
	return json.MarshalIndent(bundle, "", "  ")
}
func schema() map[string]any {
	return map[string]any{"$schema": "https://json-schema.org/draft/2020-12/schema", "type": "object", "required": []any{"type", "geometry", "properties"}, "properties": map[string]any{"type": map[string]any{"const": "Feature"}, "geometry": map[string]any{"type": []any{"object", "null"}}, "properties": map[string]any{"type": "object", "required": []any{"type", "title"}, "properties": map[string]any{"type": map[string]any{"type": "string"}, "title": map[string]any{"type": "string", "minLength": 1}, "description": map[string]any{"type": "string"}, "keywords": map[string]any{"type": "array", "items": map[string]any{"type": "string"}}}, "additionalProperties": true}}, "additionalProperties": true}
}
func pointerName(pointer string) string {
	if len(pointer) < 2 {
		return ""
	}
	for i := len(pointer) - 1; i >= 0; i-- {
		if pointer[i] == '/' {
			return pointer[i+1:]
		}
	}
	return ""
}
func withPath(schema map[string]any, path string) map[string]any {
	result := map[string]any{}
	for key, value := range schema {
		result[key] = value
	}
	result["x-tlon-path"] = path
	return result
}
