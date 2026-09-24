// Package model defines Hronir's canonical source and target documents.
package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	OGCRecordProfile        = "http://www.opengis.net/def/profile/OGC/0/ogc-record"
	ConnectedSystemsProfile = "urn:hronir:profile:connected-systems:v1"
)

type Resource struct {
	Kind           string         `json:"kind"`
	ID             string         `json:"id"`
	UID            string         `json:"uid,omitempty"`
	CanonicalURL   string         `json:"canonicalUrl"`
	Name           string         `json:"name,omitempty"`
	Description    string         `json:"description,omitempty"`
	Geometry       any            `json:"geometry,omitempty"`
	ValidTime      any            `json:"validTime,omitempty"`
	PhenomenonTime any            `json:"phenomenonTime,omitempty"`
	ResultTime     any            `json:"resultTime,omitempty"`
	Keywords       []string       `json:"keywords,omitempty"`
	Identifiers    []Identifier   `json:"identifiers,omitempty"`
	Classifiers    []any          `json:"classifiers,omitempty"`
	Contacts       []any          `json:"contacts,omitempty"`
	Links          []Link         `json:"links,omitempty"`
	Associations   map[string]any `json:"associations,omitempty"`
	Metadata       map[string]any `json:"metadata,omitempty"`
	Extensions     map[string]any `json:"extensions,omitempty"`
}

type Identifier struct {
	Scheme string `json:"scheme,omitempty"`
	Value  string `json:"value"`
}
type Link struct {
	Href  string `json:"href"`
	Rel   string `json:"rel,omitempty"`
	Type  string `json:"type,omitempty"`
	Title string `json:"title,omitempty"`
}

type Record map[string]any

func RecordID(sourceID, kind, resourceID string) string {
	name, _ := json.Marshal([]string{"hronir/v1", sourceID, kind, resourceID})
	return uuid.NewSHA1(uuid.NameSpaceURL, name).String()
}

func CanonicalJSON(value any) ([]byte, error) {
	normalized := normalize(value)
	return json.Marshal(normalized)
}

func Hash(value any) (string, error) {
	data, err := CanonicalJSON(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func normalize(value any) any {
	switch current := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(current))
		for key := range current {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		result := make(map[string]any, len(current))
		for _, key := range keys {
			result[key] = normalize(current[key])
		}
		return result
	case []any:
		result := make([]any, len(current))
		for i := range current {
			result[i] = normalize(current[i])
		}
		return result
	case []string:
		result := make([]any, len(current))
		for i := range current {
			result[i] = current[i]
		}
		return result
	default:
		return value
	}
}

func NewRecord(resource Resource, sourceID, sourceURL, profileDigest string, now time.Time) (Record, error) {
	if resource.Kind == "" || resource.ID == "" || resource.CanonicalURL == "" {
		return nil, fmt.Errorf("resource kind, id, and canonical URL are required")
	}
	properties := map[string]any{
		"type":        recordType(resource.Kind),
		"title":       firstNonEmpty(resource.Name, resource.UID, resource.ID),
		"description": resource.Description,
		"keywords":    resource.Keywords,
		"connectedSystems": map[string]any{
			"kind": resource.Kind, "identifiers": resource.Identifiers, "classifiers": resource.Classifiers,
			"contacts": resource.Contacts, "associations": resource.Associations, "metadata": resource.Metadata,
			"extensions": resource.Extensions,
		},
		"hronir": map[string]any{
			"sourceId": sourceID, "sourceUrl": sourceURL, "sourceResourceId": resource.ID,
			"sourceResourceUrl": resource.CanonicalURL, "profile": ConnectedSystemsProfile,
			"profileDigest": profileDigest, "generatedAt": now.UTC().Format(time.RFC3339Nano),
		},
	}
	if resource.Description == "" {
		delete(properties, "description")
	}
	if len(resource.Keywords) == 0 {
		delete(properties, "keywords")
	}
	record := Record{
		"id": RecordID(sourceID, resource.Kind, resource.ID), "type": "Feature", "geometry": resource.Geometry,
		"properties": properties,
		"links":      []any{map[string]any{"rel": "describes", "href": resource.CanonicalURL, "type": "application/json"}},
		"conformsTo": []any{OGCRecordProfile, ConnectedSystemsProfile},
	}
	if resource.Geometry == nil {
		record["geometry"] = nil
	}
	if timeValue := ChooseTime(resource); timeValue != nil {
		record["time"] = timeValue
	}
	if identifiers := externalIDs(resource); len(identifiers) > 0 {
		record["externalIds"] = identifiers
	}
	return record, nil
}

func ChooseTime(resource Resource) any {
	if resource.Kind == "datastreams" {
		if resource.PhenomenonTime != nil {
			return recordTime(resource.PhenomenonTime)
		}
		if resource.ResultTime != nil {
			return recordTime(resource.ResultTime)
		}
	}
	return recordTime(resource.ValidTime)
}

func recordTime(value any) any {
	switch current := value.(type) {
	case nil:
		return nil
	case string:
		if current == "" {
			return nil
		}
		return map[string]any{"timestamp": current}
	case []any:
		if len(current) == 2 {
			return map[string]any{"interval": current}
		}
	case map[string]any:
		if interval, ok := current["interval"]; ok {
			return map[string]any{"interval": interval}
		}
		if start, ok := current["start"]; ok {
			if end, found := current["end"]; found {
				return map[string]any{"interval": []any{start, end}}
			}
		}
		if timestamp, ok := current["timestamp"]; ok {
			return map[string]any{"timestamp": timestamp}
		}
	}
	return nil
}

func SemanticHash(record Record) (string, error) {
	copy := clone(record)
	if properties, ok := copy["properties"].(map[string]any); ok {
		if hronir, ok := properties["hronir"].(map[string]any); ok {
			delete(hronir, "generatedAt")
			delete(hronir, "profileDigest")
			delete(hronir, "sourceHash")
			delete(hronir, "recordHash")
		}
	}
	return Hash(copy)
}

func SetHashes(record Record, sourceHash, recordHash string) {
	properties := record["properties"].(map[string]any)
	hronir := properties["hronir"].(map[string]any)
	hronir["sourceHash"] = sourceHash
	hronir["recordHash"] = recordHash
}

func clone(record Record) Record {
	raw, _ := json.Marshal(record)
	var result Record
	_ = json.Unmarshal(raw, &result)
	return result
}
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return "Untitled Connected Systems resource"
}
func recordType(kind string) string {
	if value, found := map[string]string{"systems": "system", "deployments": "deployment", "procedures": "procedure", "samplingFeatures": "samplingFeature", "properties": "property", "datastreams": "datastream", "controlstreams": "controlstream"}[kind]; found {
		return value
	}
	return kind
}
func externalIDs(resource Resource) []any {
	values := make([]any, 0, len(resource.Identifiers)+1)
	if resource.UID != "" {
		values = append(values, map[string]any{"scheme": "connected-systems:uid", "value": resource.UID})
	}
	for _, identifier := range resource.Identifiers {
		if identifier.Value != "" {
			values = append(values, map[string]any{"scheme": identifier.Scheme, "value": identifier.Value})
		}
	}
	return values
}
