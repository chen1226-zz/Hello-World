// Package apiclient 是调用外部 HTTPS 服务的小型 HTTP 客户端。
package apiclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

// Config 描述 apiclient 的连接配置。
type Config struct {
	// BaseURL 是外部 HTTPS 服务的根地址，例如 https://api.example.com。
	BaseURL string
	// CAPEM 是内部自签服务的 CA 证书 PEM 文件路径；为空时使用系统信任根。
	CAPEM string
	// RootCAs 允许调用方直接注入已解析的 CA 证书池（主要用于测试）。
	RootCAs *x509.CertPool
}

// Client 是外部 HTTPS 服务的客户端。
type Client struct {
	baseURL string
	http    *http.Client
}

// New 根据配置创建客户端。
func New(cfg Config) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("apiclient: BaseURL must not be empty")
	}
	if _, err := url.Parse(cfg.BaseURL); err != nil {
		return nil, fmt.Errorf("apiclient: invalid BaseURL %q: %w", cfg.BaseURL, err)
	}

	roots, err := buildRoots(cfg)
	if err != nil {
		return nil, err
	}

	// 严格证书校验：由 crypto/tls 默认完成主机名、有效期与证书链验证，
	// 严禁设置 InsecureSkipVerify 或恒返回 nil 的 VerifyPeerCertificate。
	tlsCfg := &tls.Config{
		RootCAs:      roots,
		MinVersion:   tls.VersionTLS12,
		CipherSuites: secureCipherSuites(),
	}

	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSClientConfig:     tlsCfg,
		TLSHandshakeTimeout: 10 * time.Second,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        10,
		IdleConnTimeout:     90 * time.Second,
	}

	return &Client{
		baseURL: cfg.BaseURL,
		http:    &http.Client{Transport: transport, Timeout: 15 * time.Second},
	}, nil
}

// Get 请求 BaseURL 下的 path，成功时返回响应体。
func (c *Client) Get(path string) ([]byte, error) {
	endpoint, err := url.JoinPath(c.baseURL, path)
	if err != nil {
		return nil, fmt.Errorf("apiclient: build url: %w", err)
	}
	resp, err := c.http.Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("apiclient: GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("apiclient: read response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, fmt.Errorf("apiclient: unexpected status %d from %s", resp.StatusCode, endpoint)
	}
	return body, nil
}

// buildRoots 构建客户端信任根：
// 默认使用系统信任根；RootCAs 非空时以调用方注入的证书池为基础；
// CAPEM 非空时把内部自签 CA 追加进信任根，而不是关闭证书校验。
func buildRoots(cfg Config) (*x509.CertPool, error) {
	var pool *x509.CertPool
	if cfg.RootCAs != nil {
		pool = cfg.RootCAs.Clone()
	} else {
		var err error
		pool, err = x509.SystemCertPool()
		if err != nil {
			// 读不到系统信任根时退回空池：只允许显式注入的 CA，
			// 绝不能因此放宽校验。
			pool = x509.NewCertPool()
		}
	}

	if cfg.CAPEM != "" {
		pemBytes, err := os.ReadFile(cfg.CAPEM)
		if err != nil {
			return nil, fmt.Errorf("apiclient: read CA file %q: %w", cfg.CAPEM, err)
		}
		if !pool.AppendCertsFromPEM(pemBytes) {
			return nil, fmt.Errorf("apiclient: no valid certificates in CA file %q", cfg.CAPEM)
		}
	}
	return pool, nil
}

// secureCipherSuites 仅保留支持前向保密的 AEAD 密码套件。
func secureCipherSuites() []uint16 {
	return []uint16{
		tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
		tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
		tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
	}
}
