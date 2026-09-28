package apiclient

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// loadCertPEM 从 testdata 读取 PEM 证书（可传多个文件拼接）。
func loadCertPEM(t *testing.T, paths ...string) []byte {
	t.Helper()
	var pem []byte
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("读取证书 %s 失败: %v", p, err)
		}
		pem = append(pem, data...)
	}
	return pem
}

func certPoolFrom(t *testing.T, paths ...string) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(loadCertPEM(t, paths...)) {
		t.Fatalf("无法解析 CA 证书: %v", paths)
	}
	return pool
}

func serverCert(t *testing.T, certPath, keyPath string) tls.Certificate {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("加载服务端证书失败: %v", err)
	}
	return cert
}

// newTLSServer 启动使用指定证书的 HTTPS 测试服务，监听 127.0.0.1。
// 证书中的 SAN 是测试保留域名，客户端通过 tls.Config.ServerName
// 指定要校验的主机名（TCP 仍拨号到测试服务器的 127.0.0.1 地址）。
func newTLSServer(t *testing.T, cert tls.Certificate, _ string) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	// 客户端拒绝坏证书时服务器会记录握手错误，属于预期行为，静默掉。
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// newClientForServer 创建指向测试服务器的 apiclient。
// serverName 是客户端校验证书时使用的主机名；
// roots 为客户端信任根（nil 表示系统信任根），caPEMPath 为自定义 CA 文件路径。
func newClientForServer(t *testing.T, srvURL, serverName string, roots *x509.CertPool, caPEMPath string) *Client {
	t.Helper()
	c, err := New(Config{
		BaseURL: srvURL,
		CAPEM:   caPEMPath,
		RootCAs: roots,
	})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	c.http.Transport.(*http.Transport).TLSClientConfig.ServerName = serverName
	return c
}

const testdataDir = "testdata"

// TestSelfSignedCertRejected 验证：不在信任根中的自签证书必须握手失败。
func TestSelfSignedCertRejected(t *testing.T) {
	cert := serverCert(t,
		filepath.Join(testdataDir, "selfsigned", "server.pem"),
		filepath.Join(testdataDir, "selfsigned", "server-key.pem"))
	srv := newTLSServer(t, cert, "self-signed.test")

	client := newClientForServer(t, srv.URL, "self-signed.test", nil, "")
	if _, err := client.Get("/"); err == nil {
		t.Fatal("自签证书不在信任根中，预期握手失败，但连接成功")
	} else {
		var authErr x509.UnknownAuthorityError
		if !errors.As(err, &authErr) {
			t.Fatalf("预期 x509.UnknownAuthorityError，实际: %T: %v", err, err)
		}
	}
}

// TestExpiredCertRejected 验证：由受信 CA 签发但已过期的证书必须握手失败。
func TestExpiredCertRejected(t *testing.T) {
	cert := serverCert(t,
		filepath.Join(testdataDir, "expired", "server.pem"),
		filepath.Join(testdataDir, "expired", "server-key.pem"))
	srv := newTLSServer(t, cert, "expired.test")
	roots := certPoolFrom(t, filepath.Join(testdataDir, "trusted", "ca.pem"))

	client := newClientForServer(t, srv.URL, "expired.test", roots, "")
	if _, err := client.Get("/"); err == nil {
		t.Fatal("证书已过期，预期握手失败，但连接成功")
	} else {
		var invalidErr x509.CertificateInvalidError
		if !errors.As(err, &invalidErr) {
			t.Fatalf("预期 x509.CertificateInvalidError，实际: %T: %v", err, err)
		}
		if invalidErr.Reason != x509.Expired {
			t.Fatalf("预期过期原因 x509.Expired，实际: %v", invalidErr.Reason)
		}
	}
}

// TestHostnameMismatchRejected 验证：SAN 与访问主机名不匹配的证书必须握手失败。
func TestHostnameMismatchRejected(t *testing.T) {
	cert := serverCert(t,
		filepath.Join(testdataDir, "badname", "server.pem"),
		filepath.Join(testdataDir, "badname", "server-key.pem"))
	srv := newTLSServer(t, cert, "internal-api.test")
	roots := certPoolFrom(t, filepath.Join(testdataDir, "trusted", "ca.pem"))

	// 客户端以 internal-api.test 的名义访问，但证书只包含 wrong-host.test。
	client := newClientForServer(t, srv.URL, "internal-api.test", roots, "")
	if _, err := client.Get("/"); err == nil {
		t.Fatal("证书主机名不匹配，预期握手失败，但连接成功")
	} else {
		var hostErr x509.HostnameError
		if !errors.As(err, &hostErr) {
			t.Fatalf("预期 x509.HostnameError，实际: %T: %v", err, err)
		}
	}
}

// TestValidCertSucceeds 验证：受信 CA 签发、在有效期内且主机名匹配的证书可以正常访问。
func TestValidCertSucceeds(t *testing.T) {
	cert := serverCert(t,
		filepath.Join(testdataDir, "trusted", "server.pem"),
		filepath.Join(testdataDir, "trusted", "server-key.pem"))
	srv := newTLSServer(t, cert, "internal-api.test")
	roots := certPoolFrom(t, filepath.Join(testdataDir, "trusted", "ca.pem"))

	client := newClientForServer(t, srv.URL, "internal-api.test", roots, "")
	body, err := client.Get("/health")
	if err != nil {
		t.Fatalf("正常证书预期连接成功，实际失败: %v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("预期响应体 ok，实际: %q", body)
	}
}

// TestCustomCAPEMSucceeds 验证：通过 CAPEM 配置文件注入内部自签 CA 后可以正常访问，
// 而未注入该 CA 时必须失败。
func TestCustomCAPEMSucceeds(t *testing.T) {
	cert := serverCert(t,
		filepath.Join(testdataDir, "trusted", "server.pem"),
		filepath.Join(testdataDir, "trusted", "server-key.pem"))
	srv := newTLSServer(t, cert, "internal-api.test")
	caPath := filepath.Join(testdataDir, "trusted", "ca.pem")

	// 注入自定义 CA：成功。
	client := newClientForServer(t, srv.URL, "internal-api.test", nil, caPath)
	if _, err := client.Get("/"); err != nil {
		t.Fatalf("注入内部 CA 后预期连接成功，实际失败: %v", err)
	}

	// 未注入自定义 CA（且系统信任根不含该内部 CA）：必须失败。
	plain, err := New(Config{BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	plain.http.Transport.(*http.Transport).TLSClientConfig.ServerName = "internal-api.test"
	if _, err := plain.Get("/"); err == nil {
		t.Fatal("未信任内部 CA 时预期握手失败，但连接成功")
	} else {
		var authErr x509.UnknownAuthorityError
		if !errors.As(err, &authErr) {
			t.Fatalf("预期 x509.UnknownAuthorityError，实际: %T: %v", err, err)
		}
	}
}

// TestInvalidCAPEMRejected 验证：CAPEM 文件损坏时 New 必须报错，而不是静默放行。
func TestInvalidCAPEMRejected(t *testing.T) {
	dir := t.TempDir()
	badPath := filepath.Join(dir, "bad-ca.pem")
	if err := os.WriteFile(badPath, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("写入坏 CA 文件失败: %v", err)
	}
	if _, err := New(Config{BaseURL: "https://internal-api.test", CAPEM: badPath}); err == nil {
		t.Fatal("CAPEM 文件内容非法时预期 New 返回错误")
	}
}

// TestSecureTLSDefaults 验证：客户端默认开启严格证书校验且未降低 TLS 安全基线。
func TestSecureTLSDefaults(t *testing.T) {
	c, err := New(Config{BaseURL: "https://example.com"})
	if err != nil {
		t.Fatalf("创建客户端失败: %v", err)
	}
	tlsCfg := c.http.Transport.(*http.Transport).TLSClientConfig

	if tlsCfg.InsecureSkipVerify {
		t.Fatal("InsecureSkipVerify 必须为 false")
	}
	if tlsCfg.VerifyPeerCertificate != nil {
		t.Fatal("不得设置恒返回 nil 的 VerifyPeerCertificate 回调")
	}
	if tlsCfg.MinVersion < tls.VersionTLS12 {
		t.Fatalf("TLS 最低版本必须 >= 1.2，实际: %#x", tlsCfg.MinVersion)
	}
	if len(tlsCfg.CipherSuites) == 0 {
		t.Fatal("必须显式限定安全密码套件")
	}
	for _, id := range tlsCfg.CipherSuites {
		if !isSecureCipher(id) {
			t.Fatalf("密码套件 %#x 不在安全套件白名单内", id)
		}
	}
}

func isSecureCipher(id uint16) bool {
	for _, secure := range secureCipherSuites() {
		if id == secure {
			return true
		}
	}
	return false
}
