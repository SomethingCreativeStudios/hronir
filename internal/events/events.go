// Package events discovers MQTT lifecycle channels and validates CloudEvents.
package events

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/SomethingCreativeStudios/hronir/internal/config"
	"github.com/SomethingCreativeStudios/hronir/internal/transport"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type CloudEvent struct {
	SpecVersion string          `json:"specversion"`
	Type        string          `json:"type"`
	Source      string          `json:"source"`
	Subject     string          `json:"subject"`
	ID          string          `json:"id"`
	Time        string          `json:"time"`
	Data        json.RawMessage `json:"data"`
}
type Subscription struct{ client mqtt.Client }

func Parse(payload []byte) (CloudEvent, error) {
	var event CloudEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return event, err
	}
	if event.SpecVersion != "1.0" {
		return event, errors.New("CloudEvent specversion must be 1.0")
	}
	if event.Type == "" || event.Source == "" || event.Subject == "" || event.ID == "" {
		return event, errors.New("CloudEvent type, source, subject, and id are required")
	}
	return event, nil
}

func ResourceKind(eventType string) (string, bool) {
	const prefix = "org.ogc.api.consys."
	if !strings.HasPrefix(eventType, prefix) {
		return "", false
	}
	tail := strings.TrimPrefix(eventType, prefix)
	index := strings.LastIndex(tail, ".")
	if index < 1 {
		return "", false
	}
	action := tail[index+1:]
	if action != "create" && action != "update" && action != "delete" {
		return "", false
	}
	resource := strings.ToLower(tail[:index])
	aliases := map[string]string{"system": "systems", "subsystem": "systems", "systems": "systems", "deployment": "deployments", "subdeployment": "deployments", "deployments": "deployments", "procedure": "procedures", "procedures": "procedures", "samplingfeature": "samplingFeatures", "samplingfeatures": "samplingFeatures", "property": "properties", "properties": "properties", "datastream": "datastreams", "datastreams": "datastreams", "controlstream": "controlstreams", "controlstreams": "controlstreams"}
	kind, ok := aliases[resource]
	return kind, ok
}

func Discover(ctx context.Context, source config.SourceConfig, timeout time.Duration) (string, []string, error) {
	client, err := transport.HTTPClient(ctx, source.Auth, source.TLS, timeout)
	if err != nil {
		return "", nil, err
	}
	base, err := url.Parse(source.URL)
	if err != nil {
		return "", nil, err
	}
	async := base.ResolveReference(&url.URL{Path: strings.TrimSuffix(base.Path, "/") + "/asyncapi"})
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, async.String(), nil)
	if err != nil {
		return "", nil, err
	}
	response, err := client.Do(request)
	if err != nil {
		return "", nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return "", nil, fmt.Errorf("discover AsyncAPI: HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return "", nil, err
	}
	var doc struct {
		Servers map[string]struct {
			URL string `json:"url"`
		} `json:"servers"`
		Channels map[string]json.RawMessage `json:"channels"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		return "", nil, err
	}
	var broker string
	keys := make([]string, 0, len(doc.Servers))
	for key := range doc.Servers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if doc.Servers[key].URL != "" {
			broker = doc.Servers[key].URL
			break
		}
	}
	if broker == "" {
		return "", nil, errors.New("AsyncAPI declares no broker server")
	}
	topics := make([]string, 0)
	for channel := range doc.Channels {
		if strings.Contains(strings.ToLower(channel), "event") && !strings.Contains(strings.ToLower(channel), "batch") && !strings.Contains(strings.ToLower(channel), "data") {
			topics = append(topics, channel)
		}
	}
	sort.Strings(topics)
	if len(topics) == 0 {
		return "", nil, errors.New("AsyncAPI declares no lifecycle event channels")
	}
	return broker, topics, nil
}

func Subscribe(ctx context.Context, source config.SourceConfig, broker string, topics []string, onEvent func([]byte)) (*Subscription, error) {
	if broker == "" {
		return nil, errors.New("MQTT broker is required")
	}
	options := mqtt.NewClientOptions().AddBroker(broker).SetClientID("hronir-" + source.ID).SetAutoReconnect(true).SetConnectRetry(true).SetConnectRetryInterval(time.Second).SetCleanSession(false).SetResumeSubs(true)
	if source.MQTT.Username != "" {
		options.SetUsername(source.MQTT.Username)
	}
	if !source.MQTT.Password.Empty() {
		password, err := source.MQTT.Password.Resolve()
		if err != nil {
			return nil, err
		}
		options.SetPassword(password)
	}
	if source.MQTT.TLS.CAFile != "" || source.MQTT.TLS.CertFile != "" || source.MQTT.TLS.ServerName != "" {
		tlsConfig, err := transport.TLSConfig(source.MQTT.TLS)
		if err != nil {
			return nil, err
		}
		options.SetTLSConfig(tlsConfig)
	}
	callback := func(client mqtt.Client) {
		for _, topic := range topics {
			token := client.Subscribe(topic, 1, func(_ mqtt.Client, message mqtt.Message) {
				payload := append([]byte(nil), message.Payload()...)
				onEvent(payload)
			})
			if !token.WaitTimeout(15*time.Second) || token.Error() != nil {
				continue
			}
		}
	}
	options.SetOnConnectHandler(callback)
	client := mqtt.NewClient(options)
	token := client.Connect()
	if !token.WaitTimeout(15 * time.Second) {
		return nil, errors.New("MQTT connection timed out")
	}
	if err := token.Error(); err != nil {
		return nil, err
	}
	return &Subscription{client: client}, nil
}
func (s *Subscription) Close() {
	if s != nil && s.client != nil {
		s.client.Disconnect(250)
	}
}
