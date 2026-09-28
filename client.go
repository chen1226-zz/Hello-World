// Package apiclient provides an HTTP client for calling external HTTPS
// services with strict TLS certificate verification.
//
// Security notes:
//   - Certificate chain, hostname and validity period are always verified
//     (InsecureSkipVerify is never set, no custom verify callback is used).
//   - The minimum TLS version is 1.2 and only strong cipher suites are
//     enabled.
//   - Internal services using certificates issued by a private CA are
//     supported by injecting that CA via Config.CustomCAPEM or
//     Config.RootCAs — never by disabling verification.
package apiclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"time"
)

// DefaultTimeout is used when Config.Timeout is not set.
const DefaultTimeout = 10 * time.Second

// secureCipherSuites lists the TLS 1.2 cipher suites we accept. TLS 1.3
// cipher suites are not configurable and are always strong.
var secureCipherSuites = []uint16{
	tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
	tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
	tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
	tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
}

// Config configures a Client.
type Config struct {
	// Timeout is the maximum duration of a single request. Defaults to
	// DefaultTimeout.
	Timeout time.Duration

	// RootCAs, when non-nil, fully replaces the system root CA pool used
	// to verify server certificates. Mutually exclusive with CustomCAPEM.
	RootCAs *x509.CertPool

	// CustomCAPEM holds PEM-encoded CA certificates (for example an
	// internal CA that signs self-issued service certificates). They are
	// added on top of the system root CA pool, so public services keep
	// working while the internal CA is additionally trusted.
	CustomCAPEM []byte
}

// Client is an HTTP client hardened for HTTPS calls.
type Client struct {
	httpClient *http.Client
}

// NewClient builds a Client with strict TLS verification.
func NewClient(cfg Config) (*Client, error) {
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		CipherSuites: secureCipherSuites,
	}

	switch {
	case cfg.RootCAs != nil && len(cfg.CustomCAPEM) > 0:
		return nil, errors.New("apiclient: RootCAs and CustomCAPEM are mutually exclusive")
	case cfg.RootCAs != nil:
		tlsCfg.RootCAs = cfg.RootCAs
	case len(cfg.CustomCAPEM) > 0:
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(cfg.CustomCAPEM) {
			return nil, errors.New("apiclient: CustomCAPEM contains no valid PEM certificates")
		}
		tlsCfg.RootCAs = pool
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	return &Client{
		httpClient: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig:   tlsCfg,
				ForceAttemptHTTP2: true,
			},
		},
	}, nil
}

// Get issues an HTTP GET request.
func (c *Client) Get(url string) (*http.Response, error) {
	return c.httpClient.Get(url)
}

// Do sends an HTTP request and returns the response.
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	return c.httpClient.Do(req)
}
