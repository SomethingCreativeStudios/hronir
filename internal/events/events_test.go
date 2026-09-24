package events

import "testing"

func TestParseAndRecognizeResourceEvent(t *testing.T) {
	event, err := Parse([]byte(`{"specversion":"1.0","id":"one","source":"https://source.example","subject":"https://source.example/systems/a","type":"org.ogc.api.consys.system.update"}`))
	if err != nil {
		t.Fatal(err)
	}
	kind, ok := ResourceKind(event.Type)
	if !ok || kind != "systems" {
		t.Fatalf("unexpected kind %q", kind)
	}
	if _, err := Parse([]byte(`{"specversion":"0.3"}`)); err == nil {
		t.Fatal("invalid CloudEvent accepted")
	}
}
