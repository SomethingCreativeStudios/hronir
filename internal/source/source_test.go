package source

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/SomethingCreativeStudios/hronir/internal/config"
	"github.com/SomethingCreativeStudios/hronir/internal/model"
)

func TestCrawlFollowsRelativeNext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("page") == "2" {
			_, _ = w.Write([]byte(`{"type":"FeatureCollection","features":[{"id":"two","properties":{"name":"Two"},"geometry":null}]}`))
			return
		}
		_, _ = w.Write([]byte(`{"type":"FeatureCollection","features":[{"id":"one","properties":{"name":"One"},"geometry":null}],"links":[{"rel":"next","href":"/systems?page=2"}]}`))
	}))
	defer server.Close()
	client, err := New(context.Background(), config.SourceConfig{ID: "primary", URL: server.URL, Resources: map[string]config.ResourceConfig{"systems": {Mode: "auto"}}}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	result, err := client.CrawlKind(context.Background(), "systems", func(resource model.Resource) error { ids = append(ids, resource.ID); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if !result.Complete || result.Count != 2 || len(ids) != 2 {
		t.Fatalf("unexpected result: %#v ids=%v", result, ids)
	}
}

func TestNormalizeKeepsAnIndividualResourceURL(t *testing.T) {
	endpoint, _ := url.Parse("https://source.example/systems/one")
	resource, err := Normalize("systems", map[string]any{"id": "one", "properties": map[string]any{}}, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	if resource.CanonicalURL != endpoint.String() {
		t.Fatalf("wanted %s, got %s", endpoint, resource.CanonicalURL)
	}
}
