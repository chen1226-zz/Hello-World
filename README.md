# apiclient

调用外部 HTTPS 服务的小型 Go 客户端（仅标准库，Go 1.22+）。

## 安全背景：为什么必须修复

原实现为了联调内部自签证书服务，在 `tls.Config` 中做了两件危险的事：

```go
tls.Config{
    InsecureSkipVerify: true, // 完全关闭证书校验
    VerifyPeerCertificate: func(...) error { return nil }, // 校验回调恒返回 nil
}
```

这带来三个直接后果：

1. **不校验证书链**：攻击者出示任意证书（自签、伪造）都会被接受。
2. **不校验有效期**：已过期、尚未生效的证书同样被接受。
3. **不校验主机名**：证书属于谁根本无所谓，可以随便张冠李戴。

组合起来的效果等同于明文 HTTP：中间人（MITM）可以解密、查看并篡改全部
流量，窃取 API 密钥、注入恶意响应。安全扫描正是因此报告了该问题。

`make sec-test` 用三张坏证书复现了风险：修复前，自签、已过期、域名不匹配
的证书全部「连接成功」。

## 修复内容

- **恢复严格校验**：删除 `InsecureSkipVerify` 与恒返回 nil 的
  `VerifyPeerCertificate`，由 `crypto/tls` 默认完成证书链、有效期、
  主机名（SAN）的完整校验。
- **内部自签场景改为注入自定义 CA**：通过 `Config.CAPEM`（PEM 文件路径）
  或 `Config.RootCAs`（`*x509.CertPool`）把内部 CA 加入信任根，
  而不是跳过校验。CA 文件损坏或不含证书时 `New` 直接报错。
- **不降低其它安全设置**：`MinVersion` 保持 TLS 1.2，密码套件白名单
  仅保留 ECDHE + AEAD（前向保密）套件，TLS 1.3 由标准库自动协商。

## 正确用法

```go
// 公网服务：使用系统信任根，默认严格校验。
c, err := apiclient.New(apiclient.Config{BaseURL: "https://api.example.com"})

// 内部自签服务：注入内部 CA，而不是关闭校验。
c, err := apiclient.New(apiclient.Config{
    BaseURL: "https://internal-api.test",
    CAPEM:   "/etc/pki/internal/ca.pem",
})
```

反面教材（请勿使用）：

```go
tls.Config{InsecureSkipVerify: true} // 等价于明文，任何中间人都能解密篡改
```

## 测试

```sh
make sec-test
```

该命令会：

1. 运行 `cmd/gen-test-certs` 在 `testdata/` 下生成测试证书：
   - `selfsigned/` 自签证书（不在信任根中）
   - `expired/` 受信 CA 签发但已过期
   - `badname/` 受信 CA 签发但 SAN 不匹配
   - `trusted/` 内部 CA 及其签发的正常证书
2. 运行安全测试，断言：
   - 三种坏证书全部握手失败（`UnknownAuthorityError` /
     `CertificateInvalidError(Expired)` / `HostnameError`）
   - 正常证书 + 受信 CA 连接成功
   - 通过 `CAPEM` 注入自定义 CA 后连接成功，未注入时失败
   - 默认配置未关闭校验、未降低 TLS 版本与密码套件基线

测试证书仅供本地测试，密钥已随仓库公开，**切勿用于生产**。
