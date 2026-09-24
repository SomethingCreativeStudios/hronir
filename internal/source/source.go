// Package source retrieves and normalizes Connected Systems metadata resources.
package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/SomethingCreativeStudios/hronir/internal/config"
	"github.com/SomethingCreativeStudios/hronir/internal/model"
	"github.com/SomethingCreativeStudios/hronir/internal/transport"
)

type Client struct {
	source     config.SourceConfig
	base       *url.URL
	http       *http.Client
	discovered map[string]*url.URL
}
type PageResult struct {
	Complete bool
	Skipped  bool
	Count    int
}

func New(ctx context.Context, source config.SourceConfig, timeout time.Duration) (*Client, error) {
	base, err := url.Parse(source.URL)
	if err != nil {
		return nil, err
	}
	httpClient, err := transport.HTTPClient(ctx, source.Auth, source.TLS, timeout)
	if err != nil {
		return nil, err
	}
	return &Client{source: source, base: base, http: httpClient, discovered: map[string]*url.URL{}}, nil
}

func (c *Client) SourceID() string  { return c.source.ID }
func (c *Client) SourceURL() string { return strings.TrimRight(c.base.String(), "/") }

// Discover reads landing-page links opportunistically. Normal crawling remains
// compatible with implementations that expose only the standard path layout.
func (c *Client) Discover(ctx context.Context) error {
	doc, _, err := c.getJSON(ctx, c.base, "application/json")
	if err != nil {
		return err
	}
	for _, link := range links(doc) {
		resolved, err := c.resolve(c.base, link.Href)
		if err != nil {
			continue
		}
		candidate := strings.ToLower(link.Rel + " " + link.Title + " " + resolved.Path)
		for _, kind := range config.ResourceKinds {
			if strings.Contains(strings.ToLower(candidate), strings.ToLower(kind)) {
				c.discovered[kind] = resolved
			}
		}
	}
	return nil
}

// CrawlKind follows every next page. A false Complete result always prevents
// mark-and-sweep callers from deleting previous materialized resources.
func (c *Client) CrawlKind(ctx context.Context, kind string, visit func(model.Resource) error) (PageResult, error) {
	mode := c.source.Resources[kind].Mode
	if mode == "disabled" {
		return PageResult{Complete: true}, nil
	}
	endpoint := c.endpoint(kind)
	if kind == "systems" {
		query := endpoint.Query()
		query.Set("recursive", "true")
		endpoint.RawQuery = query.Encode()
	}
	next := endpoint
	count := 0
	for next != nil {
		doc, responseURL, err := c.getJSON(ctx, next, acceptFor(kind))
		if err != nil {
			if errors.Is(err, ErrNotFound) && mode == "auto" {
				return PageResult{Complete: true, Skipped: true}, nil
			}
			return PageResult{Complete: false, Count: count}, fmt.Errorf("crawl %s: %w", kind, err)
		}
		items := collectionItems(doc)
		if items == nil {
			return PageResult{Complete: false, Count: count}, fmt.Errorf("crawl %s: response does not contain features or items", kind)
		}
		for _, raw := range items {
			resource, err := Normalize(kind, raw, responseURL)
			if err != nil {
				return PageResult{Complete: false, Count: count}, fmt.Errorf("normalize %s: %w", kind, err)
			}
			if err := visit(resource); err != nil {
				return PageResult{Complete: false, Count: count}, err
			}
			count++
		}
		next = nil
		for _, link := range links(doc) {
			if strings.EqualFold(link.Rel, "next") {
				resolved, err := c.resolve(responseURL, link.Href)
				if err != nil {
					return PageResult{Complete: false, Count: count}, fmt.Errorf("next link: %w", err)
				}
				next = resolved
				break
			}
		}
	}
	return PageResult{Complete: true, Count: count}, nil
}

// GetSubject is used for CloudEvent-driven point reconciliation.
func (c *Client) GetSubject(ctx context.Context, subject string, kind string) (model.Resource, bool, error) {
	resourceURL, err := c.resolve(c.base, subject)
	if err != nil {
		return model.Resource{}, false, err
	}
	if !sameOrigin(c.base, resourceURL) {
		return model.Resource{}, false, errors.New("event subject has a different origin")
	}
	doc, responseURL, err := c.getJSON(ctx, resourceURL, acceptFor(kind))
	if errors.Is(err, ErrNotFound) {
		return model.Resource{}, false, nil
	}
	if err != nil {
		return model.Resource{}, false, err
	}
	resource, err := Normalize(kind, doc, responseURL)
	return resource, true, err
}

var ErrNotFound = errors.New("resource not found")

func (c *Client) getJSON(ctx context.Context, target *url.URL, accept string) (map[string]any, *url.URL, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	request.Header.Set("Accept", accept)
	response, err := c.http.Do(request)
	if err != nil {
		return nil, nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return nil, nil, ErrNotFound
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 64<<10))
		return nil, nil, fmt.Errorf("GET %s: HTTP %d: %s", target, response.StatusCode, strings.TrimSpace(string(body)))
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 8<<20))
	decoder.UseNumber()
	var doc map[string]any
	if err := decoder.Decode(&doc); err != nil {
		return nil, nil, fmt.Errorf("decode JSON: %w", err)
	}
	return doc, response.Request.URL, nil
}

func (c *Client) endpoint(kind string) *url.URL {
	if discovered := c.discovered[kind]; discovered != nil {
		clone := *discovered
		return &clone
	}
	clone := *c.base
	clone.Path = path.Join(strings.TrimSuffix(c.base.Path, "/"), kind)
	return &clone
}
func (c *Client) resolve(base *url.URL, href string) (*url.URL, error) {
	parsed, err := url.Parse(href)
	if err != nil {
		return nil, err
	}
	return base.ResolveReference(parsed), nil
}
func sameOrigin(a, b *url.URL) bool { return a.Scheme == b.Scheme && a.Host == b.Host }

func acceptFor(kind string) string {
	switch kind {
	case "systems", "deployments", "procedures":
		return "application/sml+json, application/geo+json;q=0.5, application/json;q=0.1"
	case "samplingFeatures":
		return "application/geo+json, application/json;q=0.1"
	case "properties":
		return "application/sml+json, application/json;q=0.1"
	default:
		return "application/json"
	}
}

func collectionItems(doc map[string]any) []map[string]any {
	for _, key := range []string{"features", "items"} {
		if values, ok := doc[key].([]any); ok {
			result := make([]map[string]any, 0, len(values))
			for _, value := range values {
				if item, ok := value.(map[string]any); ok {
					result = append(result, item)
				}
			}
			return result
		}
	}
	return nil
}

// Normalize maps GeoJSON, SensorML JSON, and stream JSON into one stable model.
func Normalize(kind string, raw map[string]any, responseURL *url.URL) (model.Resource, error) {
	id := firstString(raw, "id", "@id", "uid", "uniqueId")
	properties, _ := raw["properties"].(map[string]any)
	if id == "" {
		id = firstString(properties, "id", "uid", "uniqueId")
	}
	if id == "" {
		return model.Resource{}, errors.New("source resource has no id or uid")
	}
	canonical := responseURL
	for _, link := range links(raw) {
		if strings.EqualFold(link.Rel, "self") {
			if resolved, err := responseURL.Parse(link.Href); err == nil {
				canonical = resolved
			}
			break
		}
	}
	if canonical == responseURL {
		if path.Base(strings.TrimSuffix(responseURL.Path, "/")) != id {
			clone := *responseURL
			clone.Path = path.Join(responseURL.Path, id)
			canonical = &clone
		}
	}
	uid := firstString(raw, "uid", "uniqueId")
	if uid == "" {
		uid = firstString(properties, "uid", "uniqueId")
	}
	name := firstString(raw, "name", "label", "title")
	if name == "" {
		name = firstString(properties, "name", "title", "label")
	}
	description := firstString(raw, "description", "abstract")
	if description == "" {
		description = firstString(properties, "description", "abstract")
	}
	geometry := firstValue(raw, "geometry", "location", "position")
	if geometry == nil {
		geometry = firstValue(properties, "geometry", "location", "position")
	}
	validTime := cleanTime(firstValue(raw, "validTime", "valid_time"))
	if validTime == nil {
		validTime = cleanTime(firstValue(properties, "validTime", "valid_time"))
	}
	phenomenonTime := cleanTime(firstValue(raw, "phenomenonTime"))
	resultTime := cleanTime(firstValue(raw, "resultTime"))
	identifiers := identifiers(raw, properties, uid)
	return model.Resource{Kind: kind, ID: id, UID: uid, CanonicalURL: canonical.String(), Name: name, Description: description, Geometry: geometry, ValidTime: validTime, PhenomenonTime: phenomenonTime, ResultTime: resultTime, Keywords: keywords(raw, properties), Identifiers: identifiers, Classifiers: array(raw, "classifiers"), Contacts: array(raw, "contacts"), Links: links(raw), Associations: associations(raw), Metadata: raw, Extensions: map[string]any{}}, nil
}

func firstValue(values map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := values[key]; ok {
			return value
		}
	}
	return nil
}
func firstString(values map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := values[key].(string); ok && strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
func cleanTime(value any) any {
	if items, ok := value.([]any); ok {
		if len(items) == 1 && items[0] == nil {
			return nil
		}
		if len(items) == 2 {
			return items
		}
	}
	return value
}
func array(values map[string]any, key string) []any {
	if result, ok := values[key].([]any); ok {
		return result
	}
	return nil
}
func keywords(raw, properties map[string]any) []string {
	values := firstValue(raw, "keywords", "keyword")
	if values == nil {
		values = firstValue(properties, "keywords", "keyword")
	}
	var result []string
	switch current := values.(type) {
	case string:
		result = []string{current}
	case []any:
		for _, item := range current {
			if text, ok := item.(string); ok {
				result = append(result, text)
			}
		}
	}
	sort.Strings(result)
	return result
}
func identifiers(raw, properties map[string]any, uid string) []model.Identifier {
	value := firstValue(raw, "identifiers")
	if value == nil {
		value = firstValue(properties, "identifiers")
	}
	var result []model.Identifier
	if values, ok := value.([]any); ok {
		for _, item := range values {
			if obj, ok := item.(map[string]any); ok {
				text := firstString(obj, "value", "id", "identifier")
				if text != "" {
					result = append(result, model.Identifier{Scheme: firstString(obj, "scheme", "name", "definition"), Value: text})
				}
			}
		}
	}
	if uid != "" {
		result = append(result, model.Identifier{Scheme: "connected-systems:uid", Value: uid})
	}
	return result
}
func links(values map[string]any) []model.Link {
	raw, ok := values["links"].([]any)
	if !ok {
		return nil
	}
	result := make([]model.Link, 0, len(raw))
	for _, item := range raw {
		if obj, ok := item.(map[string]any); ok {
			href := firstString(obj, "href")
			if href != "" {
				result = append(result, model.Link{Href: href, Rel: firstString(obj, "rel"), Type: firstString(obj, "type"), Title: firstString(obj, "title")})
			}
		}
	}
	return result
}
func associations(raw map[string]any) map[string]any {
	result := map[string]any{}
	for _, key := range []string{"system", "deployment", "procedure", "samplingFeature", "observedProperties", "controlledProperties", "parent"} {
		if value, ok := raw[key]; ok {
			result[key] = value
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
