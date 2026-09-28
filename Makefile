.PHONY: sec-test test certs clean

certs:
	go run ./cmd/gen-test-certs -out testdata

test:
	go test ./...

# sec-test：生成三张坏证书（自签/已过期/域名不匹配）及一张正常证书，
# 然后验证客户端严格校验证书：坏证书全部握手失败，正常证书与自定义 CA 成功。
sec-test: certs
	go test -v -run 'TestSelfSignedCertRejected|TestExpiredCertRejected|TestHostnameMismatchRejected|TestValidCertSucceeds|TestCustomCAPEMSucceeds|TestInvalidCAPEMRejected|TestSecureTLSDefaults' ./...

clean:
	rm -rf testdata
