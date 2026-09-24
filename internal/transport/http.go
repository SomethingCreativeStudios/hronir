// Package transport constructs authenticated, mutually authenticated HTTP clients.
package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/SomethingCreativeStudios/hronir/internal/config"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func HTTPClient(ctx context.Context, auth config.AuthConfig, tlsConfig config.TLSConfig, timeout time.Duration) (*http.Client, error) {
	transport, err := tlsTransport(tlsConfig)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: transport, Timeout: timeout}
	if !auth.Bearer.Empty() {
		token, err := auth.Bearer.Resolve()
		if err != nil {
			return nil, err
		}
		client.Transport = bearerTransport{token: token, next: transport}
	}
	if auth.OAuth2 != nil {
		secret, err := auth.OAuth2.ClientSecret.Resolve()
		if err != nil {
			return nil, err
		}
		ctx = context.WithValue(ctx, oauth2.HTTPClient, client)
		oauth := clientcredentials.Config{ClientID: auth.OAuth2.ClientID, ClientSecret: secret, TokenURL: auth.OAuth2.TokenURL, Scopes: auth.OAuth2.Scopes}
		client = oauth.Client(ctx)
		client.Timeout = timeout
	}
	return client, nil
}

type bearerTransport struct {
	token string
	next  http.RoundTripper
}

func (b bearerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header.Set("Authorization", "Bearer "+b.token)
	return b.next.RoundTrip(clone)
}

func tlsTransport(config config.TLSConfig) (*http.Transport, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if config.CAFile == "" && config.CertFile == "" && config.ServerName == "" {
		return transport, nil
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: config.ServerName}
	if config.CAFile != "" {
		pem, err := os.ReadFile(filepath.Clean(config.CAFile))
		if err != nil {
			return nil, fmt.Errorf("read TLS CA: %w", err)
		}
		pool, _ := x509.SystemCertPool()
		if pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("TLS CA %q contains no certificates", config.CAFile)
		}
		tlsConfig.RootCAs = pool
	}
	if config.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	transport.TLSClientConfig = tlsConfig
	return transport, nil
}

// TLSConfig builds the same strict TLS configuration for MQTT clients.
func TLSConfig(config config.TLSConfig) (*tls.Config, error) {
	transport, err := tlsTransport(config)
	if err != nil {
		return nil, err
	}
	if transport.TLSClientConfig == nil {
		return &tls.Config{MinVersion: tls.VersionTLS12}, nil
	}
	return transport.TLSClientConfig, nil
}
