// gen-test-certs 生成 make sec-test 所需的全部测试证书。
//
// 产物（写入 testdata/）：
//
//	trusted/ca.pem, trusted/server.pem, trusted/server-key.pem  —— 内部 CA 及由其签发的正常服务证书
//	selfsigned/server.pem, selfsigned/server-key.pem          —— 自签证书（不在信任根中）
//	expired/server.pem, expired/server-key.pem                —— 受信 CA 签发但已过期
//	badname/server.pem, badname/server-key.pem                —— 受信 CA 签发但 SAN 不匹配
//
// 所有密钥均为 EC P-256；证书仅供测试，切勿用于生产。
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

func main() {
	out := flag.String("out", "testdata", "输出目录")
	flag.Parse()

	now := time.Now().UTC()
	serial := int64(1)

	// 内部 CA。
	caCert, caKey := makeCA(&serial, now, "Test Internal Root CA")
	writeCert(filepath.Join(*out, "trusted", "ca.pem"), caCert.Raw)
	trustedCert, trustedKey := makeLeaf(&serial, caCert, caKey, leafOptions{
		commonName: "internal-api.test",
		dnsNames:   []string{"internal-api.test", "*.internal-api.test"},
		notBefore:  now.Add(-time.Hour),
		notAfter:   now.Add(24 * time.Hour),
	})
	writePair(filepath.Join(*out, "trusted"), "server", trustedCert, trustedKey)

	// 自签证书：自己是自己的 issuer，客户端信任根中没有它。
	selfCert, selfKey := makeLeaf(&serial, nil, nil, leafOptions{
		commonName: "self-signed.test",
		dnsNames:   []string{"self-signed.test"},
		notBefore:  now.Add(-time.Hour),
		notAfter:   now.Add(24 * time.Hour),
	})
	writePair(filepath.Join(*out, "selfsigned"), "server", selfCert, selfKey)

	// 已过期：由受信 CA 签发，但 NotAfter 在过去。
	expiredCert, expiredKey := makeLeaf(&serial, caCert, caKey, leafOptions{
		commonName: "expired.test",
		dnsNames:   []string{"expired.test"},
		notBefore:  now.Add(-72 * time.Hour),
		notAfter:   now.Add(-24 * time.Hour),
	})
	writePair(filepath.Join(*out, "expired"), "server", expiredCert, expiredKey)

	// 域名不匹配：由受信 CA 签发，但 SAN 与客户端访问的主机名不同。
	badNameCert, badNameKey := makeLeaf(&serial, caCert, caKey, leafOptions{
		commonName: "wrong-host.test",
		dnsNames:   []string{"wrong-host.test"},
		notBefore:  now.Add(-time.Hour),
		notAfter:   now.Add(24 * time.Hour),
	})
	writePair(filepath.Join(*out, "badname"), "server", badNameCert, badNameKey)

	log.Printf("测试证书已生成到 %s", *out)
}

type leafOptions struct {
	commonName string
	dnsNames   []string
	notBefore  time.Time
	notAfter   time.Time
}

func nextSerial(counter *int64) *big.Int {
	n := big.NewInt(*counter)
	*counter++
	return n
}

func makeCA(serial *int64, now time.Time, cn string) (*x509.Certificate, *ecdsa.PrivateKey) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatalf("生成 CA 私钥失败: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          nextSerial(serial),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		log.Fatalf("创建 CA 证书失败: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		log.Fatalf("解析 CA 证书失败: %v", err)
	}
	return cert, key
}

// makeLeaf 生成服务端叶子证书。parent/parentKey 为 nil 时生成自签证书。
func makeLeaf(serial *int64, parent *x509.Certificate, parentKey *ecdsa.PrivateKey,
	opt leafOptions,
) (*x509.Certificate, *ecdsa.PrivateKey) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		log.Fatalf("生成叶子私钥失败: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: nextSerial(serial),
		Subject:      pkix.Name{CommonName: opt.commonName},
		DNSNames:     opt.dnsNames,
		NotBefore:    opt.notBefore,
		NotAfter:     opt.notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	signerCert := parent
	signerKey := parentKey
	if parent == nil {
		// 自签：用自身作为 parent 和 signer。
		signerCert = tmpl
		signerKey = key
		tmpl.BasicConstraintsValid = true
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signerCert, &key.PublicKey, signerKey)
	if err != nil {
		log.Fatalf("创建叶子证书失败 (%s): %v", opt.commonName, err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		log.Fatalf("解析叶子证书失败 (%s): %v", opt.commonName, err)
	}
	return cert, key
}

func writePair(dir, name string, cert *x509.Certificate, key *ecdsa.PrivateKey) {
	writeCert(filepath.Join(dir, name+".pem"), cert.Raw)
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		log.Fatalf("编码私钥失败: %v", err)
	}
	writePEM(filepath.Join(dir, name+"-key.pem"), "EC PRIVATE KEY", keyDER)
}

func writeCert(path string, der []byte) {
	writePEM(path, "CERTIFICATE", der)
}

func writePEM(path, typ string, der []byte) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		log.Fatalf("创建目录失败: %v", err)
	}
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("创建文件 %s 失败: %v", path, err)
	}
	defer f.Close()
	if err := pem.Encode(f, &pem.Block{Type: typ, Bytes: der}); err != nil {
		log.Fatalf("写入 PEM %s 失败: %v", path, err)
	}
}
