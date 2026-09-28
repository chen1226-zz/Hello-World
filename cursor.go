package pager

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

// 游标相关错误。
var (
	// ErrCursorExpired 游标对应的翻页会话已过期（超过 TTL）或已被释放。
	ErrCursorExpired = errors.New("pager: cursor expired")
	// ErrCursorTampered 游标被篡改（签名校验失败或负载无法解析）。
	ErrCursorTampered = errors.New("pager: cursor tampered")
	// ErrCursorMismatch 游标指向的记录在当前快照中不存在。
	ErrCursorMismatch = errors.New("pager: cursor does not match snapshot")
	// ErrInvalidLimit 页大小非法。
	ErrInvalidLimit = errors.New("pager: invalid page limit")
)

const (
	cursorVersion   = 1
	cursorFixedLen  = 1 + 8 + 8 + 8 // 格式版本 + 快照版本 + UpdatedAt(nsec) + ID
	cursorSignature = sha256.Size
)

// Cursor 是不透明的翻页游标，仅在翻页会话内有效。
type Cursor struct {
	token string
}

// String 返回可随响应传输的游标字符串。
func (c Cursor) String() string { return c.token }

// ParseCursor 解析游标字符串（不校验签名，校验发生在 NextPage）。
func ParseCursor(s string) Cursor { return Cursor{token: s} }

// cursorPayload 是游标的明文负载。
type cursorPayload struct {
	snapVersion int64
	updatedAt   time.Time
	id          int64
}

// encodeCursor 生成带 HMAC 签名的游标 token。
func encodeCursor(secret []byte, p cursorPayload) string {
	body := make([]byte, cursorFixedLen)
	body[0] = cursorVersion
	binary.BigEndian.PutUint64(body[1:9], uint64(p.snapVersion))
	binary.BigEndian.PutUint64(body[9:17], uint64(p.updatedAt.UnixNano()))
	binary.BigEndian.PutUint64(body[17:25], uint64(p.id))
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	token := mac.Sum(body) // body || signature
	return base64.RawURLEncoding.EncodeToString(token)
}

// decodeCursor 校验签名并解析游标；任何篡改或格式错误都报 ErrCursorTampered。
func decodeCursor(secret []byte, token string) (cursorPayload, error) {
	var p cursorPayload
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != cursorFixedLen+cursorSignature {
		return p, ErrCursorTampered
	}
	body, sig := raw[:cursorFixedLen], raw[cursorFixedLen:]
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return p, ErrCursorTampered
	}
	if body[0] != cursorVersion {
		return p, ErrCursorTampered
	}
	p.snapVersion = int64(binary.BigEndian.Uint64(body[1:9]))
	p.updatedAt = time.Unix(0, int64(binary.BigEndian.Uint64(body[9:17])))
	p.id = int64(binary.BigEndian.Uint64(body[17:25]))
	return p, nil
}

// 供调试/测试使用。
func (p cursorPayload) String() string {
	return fmt.Sprintf("cursor{snap=%d, updatedAt=%s, id=%d}",
		p.snapVersion, p.updatedAt.Format(time.RFC3339Nano), p.id)
}
