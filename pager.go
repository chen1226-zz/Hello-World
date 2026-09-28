package pager

import (
	"crypto/rand"
	"sync"
	"time"
)

// DefaultCursorTTL 是游标（翻页会话）的默认有效期。
const DefaultCursorTTL = 10 * time.Minute

// Page 是一页结果。
type Page struct {
	// Records 是本页记录，按 (UpdatedAt, ID) 倒序。
	Records []Record
	// NextCursor 是下一页游标；HasMore 为 false 时为空游标。
	NextCursor Cursor
	// HasMore 表示是否还有下一页。
	HasMore bool
	// Version 是本页所属快照的存储版本号；同一次翻页会话的所有页相同。
	Version int64
}

// Pager 在 Store 之上提供游标分页。一次翻页会话固定读取同一快照，
// 因此翻页过程中的并发写入不会导致漏数据或重复数据。
type Pager struct {
	store  *Store
	ttl    time.Duration
	secret []byte

	mu       sync.Mutex
	sessions map[int64]*session // 快照版本 -> 会话
}

type session struct {
	snap     *Snapshot
	lastUsed time.Time
}

// Option 是 Pager 的可选配置。
type Option func(*Pager)

// WithCursorTTL 设置游标有效期；超过有效期未使用的游标会过期。
func WithCursorTTL(ttl time.Duration) Option {
	return func(p *Pager) { p.ttl = ttl }
}

// WithCursorSecret 设置游标签名密钥；不设置则每次创建 Pager 时随机生成。
func WithCursorSecret(secret []byte) Option {
	return func(p *Pager) {
		if len(secret) > 0 {
			p.secret = append([]byte(nil), secret...)
		}
	}
}

// NewPager 创建分页器。
func NewPager(store *Store, opts ...Option) *Pager {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic(err)
	}
	p := &Pager{
		store:    store,
		ttl:      DefaultCursorTTL,
		secret:   secret,
		sessions: make(map[int64]*session),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// FirstPage 开启一次翻页会话并返回第一页。此后用 Page.NextCursor 调用
// NextPage 逐页拉取，直到 HasMore 为 false。
func (p *Pager) FirstPage(limit int) (Page, error) {
	if limit <= 0 {
		return Page{}, ErrInvalidLimit
	}
	snap := p.store.Snapshot()
	recs, err := snap.Records()
	if err != nil {
		snap.Release()
		return Page{}, err
	}
	p.remember(snap)
	return p.pageFrom(snap, recs, 0, limit), nil
}

// NextPage 返回游标之后的下一页。
func (p *Pager) NextPage(cursor Cursor, limit int) (Page, error) {
	if limit <= 0 {
		return Page{}, ErrInvalidLimit
	}
	payload, err := decodeCursor(p.secret, cursor.token)
	if err != nil {
		return Page{}, err
	}
	sess, err := p.acquire(payload.snapVersion)
	if err != nil {
		return Page{}, err
	}
	recs, err := sess.snap.Records()
	if err != nil {
		return Page{}, err
	}
	// 游标指向「上一页最后一条」，下一页从它之后开始（复合键严格大于比较）。
	start := searchAfter(recs, payload.updatedAt, payload.id)
	if start < 0 {
		return Page{}, ErrCursorMismatch
	}
	return p.pageFrom(sess.snap, recs, start, limit), nil
}

// remember 登记会话，并顺带清理过期会话。
func (p *Pager) remember(snap *Snapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sweepLocked(time.Now())
	if old, ok := p.sessions[snap.version]; ok {
		old.snap.Release()
	}
	p.sessions[snap.version] = &session{snap: snap, lastUsed: time.Now()}
}

// acquire 查找并刷新会话；不存在（过期或从未登记）时报 ErrCursorExpired。
func (p *Pager) acquire(version int64) (*session, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sweepLocked(time.Now())
	sess, ok := p.sessions[version]
	if !ok {
		return nil, ErrCursorExpired
	}
	sess.lastUsed = time.Now()
	return sess, nil
}

func (p *Pager) sweepLocked(now time.Time) {
	for v, sess := range p.sessions {
		if now.Sub(sess.lastUsed) > p.ttl {
			sess.snap.Release()
			delete(p.sessions, v)
		}
	}
}

// pageFrom 从排序好的快照切片 recs 的 start 位置截取一页。
func (p *Pager) pageFrom(snap *Snapshot, recs []Record, start, limit int) Page {
	end := start + limit
	if end > len(recs) {
		end = len(recs)
	}
	page := Page{
		Records: recs[start:end],
		Version: snap.version,
	}
	if end < len(recs) {
		last := recs[end-1]
		page.HasMore = true
		page.NextCursor = Cursor{token: encodeCursor(p.secret, cursorPayload{
			snapVersion: snap.version,
			updatedAt:   last.UpdatedAt,
			id:          last.ID,
		})}
	}
	return page
}

// searchAfter 返回排序切片 recs 中严格位于 (updatedAt, id) 之后的第一条的
// 下标；若该键不存在于快照中则返回 -1。
func searchAfter(recs []Record, updatedAt time.Time, id int64) int {
	// recs 按 (UpdatedAt, ID) 倒序：先找第一个「排在 (updatedAt, id) 之后
	// 或相等」的位置。
	lo, hi := 0, len(recs)
	for lo < hi {
		mid := (lo + hi) / 2
		r := recs[mid]
		// r 排在游标键之前（更靠前）=> 答案在右侧
		if r.UpdatedAt.After(updatedAt) ||
			(r.UpdatedAt.Equal(updatedAt) && r.ID > id) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	// lo 是第一个 <= 游标键的位置；若恰好相等则下一页从 lo+1 开始。
	if lo < len(recs) && recs[lo].UpdatedAt.Equal(updatedAt) && recs[lo].ID == id {
		return lo + 1
	}
	return -1
}
