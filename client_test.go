package apiclient

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

var serial int64 = 1000

func nextSerial() *big.Int {
	serial++
	return big.NewInt(serial)
}

func generateKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return key
}

// newTestCA creates a self-signed CA and returns its parsed certificate,
// private key and PEM encoding.
func newTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key := generateKey(t)
	tmpl := &x509.Certificate{
		SerialNumber:          nextSerial(),
		Subject:               pkix.Name{CommonName: "Test Internal CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return cert, key, pemBytes
}

// issueServerCert signs a server certificate with the given parent
// (pass the template itself as parent for a self-signed certificate).
func issueServerCert(t *testing.T, tmpl, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) tls.Certificate {
	t.Helper()
	key := generateKey(t)
	if parentKey == nil {
		parentKey = key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("key pair: %v", err)
	}
	return pair
}

func serverCertTemplate() *x509.Certificate {
	return &x509.Certificate{
		SerialNumber: nextSerial(),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
}

func startTLSServer(t *testing.T, cert tls.Certificate) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func mustClient(t *testing.T, cfg Config) *Client {
	t.Helper()
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func poolWith(t *testing.T, caPEM []byte) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		t.Fatal("append CA to pool failed")
	}
	return pool
}

// 1. Self-signed server certificate must be rejected.
func TestRejectsSelfSignedCert(t *testing.T) {
	tmpl := serverCertTemplate()
	cert := issueServerCert(t, tmpl, tmpl, nil) // self-signed
	srv := startTLSServer(t, cert)

	client := mustClient(t, Config{})
	_, err := client.Get(srv.URL)
	if err == nil {
		t.Fatal("expected self-signed certificate to be rejected, got connection success")
	}
	var uaErr x509.UnknownAuthorityError
	if !errors.As(err, &uaErr) {
		t.Fatalf("expected UnknownAuthorityError, got: %v", err)
	}
}

// 2. Expired certificate must be rejected even when its CA is trusted.
func TestRejectsExpiredCert(t *testing.T) {
	ca, caKey, caPEM := newTestCA(t)
	tmpl := serverCertTemplate()
	tmpl.NotBefore = time.Now().Add(-48 * time.Hour)
	tmpl.NotAfter = time.Now().Add(-24 * time.Hour) // expired yesterday
	cert := issueServerCert(t, tmpl, ca, caKey)
	srv := startTLSServer(t, cert)

	client := mustClient(t, Config{RootCAs: poolWith(t, caPEM)})
	_, err := client.Get(srv.URL)
	if err == nil {
		t.Fatal("expected expired certificate to be rejected, got connection success")
	}
	var invalidErr x509.CertificateInvalidError
	if !errors.As(err, &invalidErr) || invalidErr.Reason != x509.Expired {
		t.Fatalf("expected CertificateInvalidError(Expired), got: %v", err)
	}
}

// 3. Hostname mismatch must be rejected even when its CA is trusted.
func TestRejectsHostnameMismatch(t *testing.T) {
	ca, caKey, caPEM := newTestCA(t)
	tmpl := serverCertTemplate()
	tmpl.DNSNames = []string{"wrong-host.example.com"}
	tmpl.IPAddresses = nil // no SAN matches 127.0.0.1
	cert := issueServerCert(t, tmpl, ca, caKey)
	srv := startTLSServer(t, cert)

	client := mustClient(t, Config{RootCAs: poolWith(t, caPEM)})
	_, err := client.Get(srv.URL)
	if err == nil {
		t.Fatal("expected hostname-mismatched certificate to be rejected, got connection success")
	}
	var hostErr x509.HostnameError
	if !errors.As(err, &hostErr) {
		t.Fatalf("expected HostnameError, got: %v", err)
	}
}

// 4. A valid certificate (trusted chain, matching hostname, in validity
// window) must succeed.
func TestAcceptsValidCert(t *testing.T) {
	ca, caKey, caPEM := newTestCA(t)
	cert := issueServerCert(t, serverCertTemplate(), ca, caKey)
	srv := startTLSServer(t, cert)

	client := mustClient(t, Config{RootCAs: poolWith(t, caPEM)})
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("expected valid certificate to be accepted, got: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
	if resp.TLS == nil || resp.TLS.Version < tls.VersionTLS12 {
		t.Fatalf("expected TLS >= 1.2, got: %+v", resp.TLS)
	}
}

// 5. Internal self-issued scenario: inject the internal CA via CustomCAPEM
// instead of disabling verification.
func TestAcceptsInternalCertWithCustomCA(t *testing.T) {
	_, caKey, caPEM := newTestCA(t)
	caCert, err := x509.ParseCertificate(mustDecodePEM(t, caPEM))
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	cert := issueServerCert(t, serverCertTemplate(), caCert, caKey)
	srv := startTLSServer(t, cert)

	// Without the custom CA the internal certificate is untrusted.
	plain := mustClient(t, Config{})
	if _, err := plain.Get(srv.URL); err == nil {
		t.Fatal("expected internal certificate to be rejected without custom CA")
	}

	// With the custom CA injected, verification succeeds.
	client := mustClient(t, Config{CustomCAPEM: caPEM})
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("expected internal certificate to be accepted with custom CA, got: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status: %d", resp.StatusCode)
	}
}

// TLS versions below 1.2 must be refused by the client.
func TestRejectsTLSBelow12(t *testing.T) {
	ca, caKey, caPEM := newTestCA(t)
	cert := issueServerCert(t, serverCertTemplate(), ca, caKey)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS10,
		MaxVersion:   tls.VersionTLS11,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	client := mustClient(t, Config{RootCAs: poolWith(t, caPEM)})
	if _, err := client.Get(srv.URL); err == nil {
		t.Fatal("expected TLS < 1.2 handshake to be rejected")
	}
}

func mustDecodePEM(t *testing.T, pemBytes []byte) []byte {
	t.Helper()
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		t.Fatal("failed to decode PEM")
	}
	return block.Bytes
}
