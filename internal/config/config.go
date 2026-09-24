// Package config loads and validates Hronir's strict YAML configuration.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	APIVersion = "hronir.io/v1alpha1"
	Kind       = "Harvester"
)

var sourceIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// Config is the complete, versioned Hronir configuration document.
type Config struct {
	APIVersion string         `yaml:"apiVersion"`
	Kind       string         `yaml:"kind"`
	Target     TargetConfig   `yaml:"target"`
	State      StateConfig    `yaml:"state"`
	Runtime    RuntimeConfig  `yaml:"runtime"`
	Sources    []SourceConfig `yaml:"sources"`
	Mapping    MappingConfig  `yaml:"mapping"`
}

type TargetConfig struct {
	URL       string     `yaml:"url"`
	CatalogID string     `yaml:"catalogId"`
	Auth      AuthConfig `yaml:"auth"`
	TLS       TLSConfig  `yaml:"tls"`
}

type StateConfig struct {
	Path string `yaml:"path"`
}

type RuntimeConfig struct {
	ListenAddress                string        `yaml:"listenAddress"`
	PollInterval                 time.Duration `yaml:"pollInterval"`
	TargetReconciliationInterval time.Duration `yaml:"targetReconciliationInterval"`
	DeleteAfterSuccessfulMisses  int           `yaml:"deleteAfterSuccessfulMisses"`
	Concurrency                  int           `yaml:"concurrency"`
	RequestTimeout               time.Duration `yaml:"requestTimeout"`
}

type SourceConfig struct {
	ID        string                    `yaml:"id"`
	URL       string                    `yaml:"url"`
	Auth      AuthConfig                `yaml:"auth"`
	TLS       TLSConfig                 `yaml:"tls"`
	Resources map[string]ResourceConfig `yaml:"resources"`
	MQTT      MQTTConfig                `yaml:"mqtt"`
}

type ResourceConfig struct {
	Mode string `yaml:"mode"`
}

type MQTTConfig struct {
	Enabled  *bool     `yaml:"enabled"`
	Required bool      `yaml:"required"`
	Broker   string    `yaml:"broker"`
	Topics   []string  `yaml:"topics"`
	Username string    `yaml:"username"`
	Password SecretRef `yaml:"password"`
	TLS      TLSConfig `yaml:"tls"`
}

// MappingConfig controls the built-in profile, typed field overlays, and JS files.
type MappingConfig struct {
	Profile   string                     `yaml:"profile"`
	Overlays  map[string]ResourceMapping `yaml:"overlays"`
	Functions []string                   `yaml:"functions"`
}

type ResourceMapping struct {
	Fields map[string]FieldOverride `yaml:"fields"`
}

// FieldOverride accepts either a literal scalar/list/object, an expression string
// beginning with '=', or an object with expr/schema/queryable/sortable/facet.
type FieldOverride struct {
	Remove    bool
	Expr      string
	Literal   any
	Schema    map[string]any
	Queryable *bool
	Sortable  *bool
	Facet     *bool
}

func (f *FieldOverride) UnmarshalYAML(value *yaml.Node) error {
	if value.Tag == "!!null" {
		f.Remove = true
		return nil
	}
	if value.Kind == yaml.MappingNode {
		metadata := false
		for index := 0; index < len(value.Content); index += 2 {
			switch value.Content[index].Value {
			case "expr", "value", "schema", "queryable", "sortable", "facet":
				metadata = true
			}
		}
		if !metadata {
			var literal any
			if err := value.Decode(&literal); err != nil {
				return err
			}
			f.Literal = literal
			return nil
		}
		var raw struct {
			Expr      string         `yaml:"expr"`
			Value     any            `yaml:"value"`
			Schema    map[string]any `yaml:"schema"`
			Queryable *bool          `yaml:"queryable"`
			Sortable  *bool          `yaml:"sortable"`
			Facet     *bool          `yaml:"facet"`
		}
		if err := value.Decode(&raw); err != nil {
			return err
		}
		hasValue := false
		for index := 0; index < len(value.Content); index += 2 {
			if value.Content[index].Value == "value" {
				hasValue = true
				break
			}
		}
		if raw.Expr == "" && !hasValue {
			return errors.New("field object requires expr or value")
		}
		f.Expr, f.Literal, f.Schema, f.Queryable, f.Sortable, f.Facet = raw.Expr, raw.Value, raw.Schema, raw.Queryable, raw.Sortable, raw.Facet
		return nil
	}
	var literal any
	if err := value.Decode(&literal); err != nil {
		return err
	}
	if text, ok := literal.(string); ok && strings.HasPrefix(text, "=") {
		f.Expr = text
	} else {
		f.Literal = literal
	}
	return nil
}

// SecretRef prevents accidentally committing literal credentials. Exactly one
// environment variable or file path must be set when a secret is required.
type SecretRef struct {
	Env  string `yaml:"env"`
	File string `yaml:"file"`
}

func (s SecretRef) Empty() bool { return s.Env == "" && s.File == "" }

func (s SecretRef) Resolve() (string, error) {
	if s.Empty() {
		return "", nil
	}
	if s.Env != "" && s.File != "" {
		return "", errors.New("secret reference must have exactly one of env or file")
	}
	if s.Env != "" {
		value, found := os.LookupEnv(s.Env)
		if !found {
			return "", fmt.Errorf("secret environment variable %q is not set", s.Env)
		}
		return value, nil
	}
	data, err := os.ReadFile(filepath.Clean(s.File))
	if err != nil {
		return "", fmt.Errorf("read secret file: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}

type AuthConfig struct {
	Bearer SecretRef          `yaml:"bearer"`
	OAuth2 *ClientCredentials `yaml:"oauth2"`
}

type ClientCredentials struct {
	TokenURL     string    `yaml:"tokenUrl"`
	ClientID     string    `yaml:"clientId"`
	ClientSecret SecretRef `yaml:"clientSecret"`
	Scopes       []string  `yaml:"scopes"`
}

type TLSConfig struct {
	CAFile     string `yaml:"caFile"`
	CertFile   string `yaml:"certFile"`
	KeyFile    string `yaml:"keyFile"`
	ServerName string `yaml:"serverName"`
}

func Load(path string) (Config, error) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return Config{}, err
	}
	defer file.Close()
	return Decode(file)
}

func Decode(reader io.Reader) (Config, error) {
	decoder := yaml.NewDecoder(reader)
	decoder.KnownFields(true)
	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode configuration: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return Config{}, errors.New("configuration must contain one YAML document")
		}
		return Config{}, fmt.Errorf("decode configuration: %w", err)
	}
	if err := cfg.NormalizeAndValidate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) NormalizeAndValidate() error {
	if c.APIVersion != APIVersion {
		return fmt.Errorf("apiVersion must be %q", APIVersion)
	}
	if c.Kind != Kind {
		return fmt.Errorf("kind must be %q", Kind)
	}
	if c.Target.CatalogID == "" {
		c.Target.CatalogID = "connected-systems"
	}
	if err := validateURL("target.url", c.Target.URL); err != nil {
		return err
	}
	if err := c.Target.Auth.Validate("target.auth"); err != nil {
		return err
	}
	if err := c.Target.TLS.Validate("target.tls"); err != nil {
		return err
	}
	if c.State.Path == "" {
		c.State.Path = "hronir.db"
	}
	c.Runtime.applyDefaults()
	if err := c.Runtime.Validate(); err != nil {
		return err
	}
	if len(c.Sources) == 0 {
		return errors.New("at least one source is required")
	}
	seen := map[string]struct{}{}
	for i := range c.Sources {
		source := &c.Sources[i]
		if !sourceIDPattern.MatchString(source.ID) {
			return fmt.Errorf("sources[%d].id must match %s", i, sourceIDPattern)
		}
		if _, exists := seen[source.ID]; exists {
			return fmt.Errorf("duplicate source id %q", source.ID)
		}
		seen[source.ID] = struct{}{}
		if err := validateURL("sources["+source.ID+"].url", source.URL); err != nil {
			return err
		}
		if err := source.Auth.Validate("sources[" + source.ID + "].auth"); err != nil {
			return err
		}
		if err := source.TLS.Validate("sources[" + source.ID + "].tls"); err != nil {
			return err
		}
		if err := source.MQTT.Validate("sources[" + source.ID + "].mqtt"); err != nil {
			return err
		}
		if source.Resources == nil {
			source.Resources = map[string]ResourceConfig{}
		}
		for _, kind := range ResourceKinds {
			item := source.Resources[kind]
			if item.Mode == "" {
				item.Mode = "auto"
			}
			if item.Mode != "auto" && item.Mode != "required" && item.Mode != "disabled" {
				return fmt.Errorf("sources[%s].resources[%s].mode must be auto, required, or disabled", source.ID, kind)
			}
			source.Resources[kind] = item
		}
		for kind := range source.Resources {
			if !isResourceKind(kind) {
				return fmt.Errorf("sources[%s].resources contains unsupported kind %q", source.ID, kind)
			}
		}
	}
	if c.Mapping.Profile == "" {
		c.Mapping.Profile = "connected-systems/v1"
	}
	if c.Mapping.Profile != "connected-systems/v1" {
		return fmt.Errorf("unsupported mapping profile %q", c.Mapping.Profile)
	}
	return c.validateMappings()
}

func (c Config) validateMappings() error {
	customSchemas := map[string][]byte{}
	for kind, overlay := range c.Mapping.Overlays {
		if !isResourceKind(kind) {
			return fmt.Errorf("mapping overlay has unsupported resource kind %q", kind)
		}
		for pointer, field := range overlay.Fields {
			if !strings.HasPrefix(pointer, "/") {
				return fmt.Errorf("mapping field %q must be a JSON Pointer", pointer)
			}
			if field.Remove {
				continue
			}
			if field.Expr != "" && !strings.HasPrefix(field.Expr, "=") {
				return fmt.Errorf("mapping field %q expression must begin with =", pointer)
			}
			if field.Schema != nil && field.Expr == "" && field.Literal == nil {
				return fmt.Errorf("mapping field %q schema requires expr or value", pointer)
			}
			if !field.Remove && !builtInMappingField(pointer) && field.Schema == nil {
				return fmt.Errorf("new mapping field %q requires schema, queryable, sortable, and facet", pointer)
			}
			if field.Schema != nil {
				if _, ok := field.Schema["type"]; !ok {
					return fmt.Errorf("mapping field %q schema requires type", pointer)
				}
				if field.Queryable == nil || field.Sortable == nil || field.Facet == nil {
					return fmt.Errorf("new mapping field %q requires queryable, sortable, and facet", pointer)
				}
				encoded, _ := json.Marshal(field.Schema)
				if previous, found := customSchemas[pointer]; found && string(previous) != string(encoded) {
					return fmt.Errorf("mapping field %q has conflicting schemas across resource kinds", pointer)
				}
				customSchemas[pointer] = encoded
			}
		}
	}
	return nil
}

func builtInMappingField(pointer string) bool {
	switch pointer {
	case "/properties/title", "/properties/description", "/properties/keywords", "/geometry", "/properties/connectedSystems/identifiers", "/properties/connectedSystems/classifiers", "/properties/connectedSystems/contacts", "/properties/connectedSystems/associations", "/properties/connectedSystems/metadata", "/properties/connectedSystems/extensions":
		return true
	default:
		return false
	}
}

func (r *RuntimeConfig) applyDefaults() {
	if r.ListenAddress == "" {
		r.ListenAddress = ":8081"
	}
	if r.PollInterval == 0 {
		r.PollInterval = time.Hour
	}
	if r.TargetReconciliationInterval == 0 {
		r.TargetReconciliationInterval = 24 * time.Hour
	}
	if r.DeleteAfterSuccessfulMisses == 0 {
		r.DeleteAfterSuccessfulMisses = 2
	}
	if r.Concurrency == 0 {
		r.Concurrency = 8
	}
	if r.RequestTimeout == 0 {
		r.RequestTimeout = 30 * time.Second
	}
}

func (r RuntimeConfig) Validate() error {
	if r.PollInterval <= 0 || r.TargetReconciliationInterval <= 0 || r.RequestTimeout <= 0 {
		return errors.New("runtime durations must be positive")
	}
	if r.DeleteAfterSuccessfulMisses < 1 {
		return errors.New("runtime.deleteAfterSuccessfulMisses must be at least 1")
	}
	if r.Concurrency < 1 || r.Concurrency > 128 {
		return errors.New("runtime.concurrency must be between 1 and 128")
	}
	return nil
}

func (a AuthConfig) Validate(path string) error {
	if !a.Bearer.Empty() && a.OAuth2 != nil {
		return fmt.Errorf("%s supports either bearer or oauth2, not both", path)
	}
	if !a.Bearer.Empty() {
		_, err := a.Bearer.Resolve()
		if err != nil {
			return fmt.Errorf("%s.bearer: %w", path, err)
		}
		return nil
	}
	if a.OAuth2 == nil {
		return nil
	}
	if err := validateURL(path+".oauth2.tokenUrl", a.OAuth2.TokenURL); err != nil {
		return err
	}
	if a.OAuth2.ClientID == "" {
		return fmt.Errorf("%s.oauth2.clientId is required", path)
	}
	if a.OAuth2.ClientSecret.Empty() {
		return fmt.Errorf("%s.oauth2.clientSecret is required", path)
	}
	_, err := a.OAuth2.ClientSecret.Resolve()
	if err != nil {
		return fmt.Errorf("%s.oauth2.clientSecret: %w", path, err)
	}
	return nil
}

func (t TLSConfig) Validate(path string) error {
	if (t.CertFile == "") != (t.KeyFile == "") {
		return fmt.Errorf("%s requires both certFile and keyFile", path)
	}
	return nil
}

func (m MQTTConfig) Validate(path string) error {
	if m.Enabled != nil && !*m.Enabled {
		return nil
	}
	if m.Broker != "" {
		if err := validateURL(path+".broker", m.Broker); err != nil {
			return err
		}
	}
	if err := m.TLS.Validate(path + ".tls"); err != nil {
		return err
	}
	if !m.Password.Empty() {
		if _, err := m.Password.Resolve(); err != nil {
			return fmt.Errorf("%s.password: %w", path, err)
		}
	}
	return nil
}

func validateURL(path, raw string) error {
	value, err := url.Parse(raw)
	if err != nil || value.Scheme == "" || value.Host == "" {
		return fmt.Errorf("%s must be an absolute URL", path)
	}
	if value.Scheme != "http" && value.Scheme != "https" && value.Scheme != "mqtt" && value.Scheme != "mqtts" && value.Scheme != "ws" && value.Scheme != "wss" {
		return fmt.Errorf("%s has unsupported scheme %q", path, value.Scheme)
	}
	return nil
}

var ResourceKinds = []string{"systems", "deployments", "procedures", "samplingFeatures", "properties", "datastreams", "controlstreams"}

func isResourceKind(value string) bool {
	for _, kind := range ResourceKinds {
		if value == kind {
			return true
		}
	}
	return false
}
