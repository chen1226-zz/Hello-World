package pager

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// drainAll 从第一页开始拉取全部页，返回拼接后的记录与每一页。
func drainAll(t *testing.T, p *Pager, limit int) ([]Record, []Page) {
	t.Helper()
	page, err := p.FirstPage(limit)
	if err != nil {
		t.Fatalf("FirstPage: %v", err)
	}
	pages := []Page{page}
	all := append([]Record(nil), page.Records...)
	for page.HasMore {
		page, err = p.NextPage(page.NextCursor, limit)
		if err != nil {
			t.Fatalf("NextPage: %v", err)
		}
		pages = append(pages, page)
		all = append(all, page.Records...)
	}
	return all, pages
}

// assertConsistent 断言拼接结果与快照一致：无重复、无遗漏、顺序正确。
func assertConsistent(t *testing.T, got, want []Record) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("记录数不一致: got %d, want %d", len(got), len(want))
	}
	seen := make(map[int64]int, len(got))
	for i, r := range got {
		seen[r.ID]++
		if seen[r.ID] > 1 {
			t.Fatalf("记录重复: id=%d 出现 %d 次", r.ID, seen[r.ID])
		}
		if r != want[i] {
			t.Fatalf("第 %d 条不一致: got %+v, want %+v", i, r, want[i])
		}
	}
}

// TestDuplicateSortKeysAcrossPages 大量记录共享同一 UpdatedAt，跨页不能漏/重。
func TestDuplicateSortKeysAcrossPages(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	// 3 个时间戳 x 40 个 ID，大量重复排序键。
	for i := 0; i < 120; i++ {
		store.Put(Record{
			ID:        int64(i + 1),
			UpdatedAt: base.Add(-time.Duration(i%3) * time.Minute),
			Value:     fmt.Sprintf("v%d", i),
		})
	}
	pager := NewPager(store)
	for _, limit := range []int{1, 2, 3, 7, 40, 41, 200} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			got, pages := drainAll(t, pager, limit)
			want, err := store.RecordsAt(store.Version())
			if err != nil {
				t.Fatal(err)
			}
			assertConsistent(t, got, want)
			// 同一会话所有页版本一致。
			for _, pg := range pages {
				if pg.Version != pages[0].Version {
					t.Fatalf("页版本不一致: %d vs %d", pg.Version, pages[0].Version)
				}
			}
			// 页大小符合预期。
			for i, pg := range pages[:len(pages)-1] {
				if len(pg.Records) != limit {
					t.Fatalf("第 %d 页大小 %d, want %d", i, len(pg.Records), limit)
				}
			}
		})
	}
}

// TestInsertAtPageBoundary 翻页过程中，新记录插入到当前页边界上。
// 快照隔离下：新记录不出现在本次翻页结果中，已有记录不漏不重。
func TestInsertAtPageBoundary(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	const n = 30
	for i := 0; i < n; i++ {
		store.Put(Record{ID: int64(i + 1), UpdatedAt: base, Value: "old"})
	}
	pager := NewPager(store)
	const limit = 10

	page, err := pager.FirstPage(limit)
	if err != nil {
		t.Fatal(err)
	}
	all := append([]Record(nil), page.Records...)
	// 在第一页中间位置的排序键上插入新记录（落在页边界/页内）。
	boundary := page.Records[limit/2]
	store.Put(Record{ID: 1000, UpdatedAt: boundary.UpdatedAt, Value: "new"})
	// 再插一条 UpdatedAt 更新的（会排在最前）。
	store.Put(Record{ID: 1001, UpdatedAt: base.Add(time.Minute), Value: "newer"})

	for page.HasMore {
		page, err = pager.NextPage(page.NextCursor, limit)
		if err != nil {
			t.Fatalf("NextPage: %v", err)
		}
		all = append(all, page.Records...)
	}
	if len(all) != n {
		t.Fatalf("快照内记录数 = %d, want %d（新插入的不应出现）", len(all), n)
	}
	for _, r := range all {
		if r.ID >= 1000 {
			t.Fatalf("翻页过程中插入的记录 id=%d 不应出现在本次会话", r.ID)
		}
	}
	// 新一轮翻页应能看到新记录。
	got, _ := drainAll(t, pager, limit)
	if len(got) != n+2 {
		t.Fatalf("新一轮翻页记录数 = %d, want %d", len(got), n+2)
	}
}

// TestDeletePageHeadAndTail 翻页过程中删除当前页的首尾记录，结果不受影响。
func TestDeletePageHeadAndTail(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	const n = 25
	for i := 0; i < n; i++ {
		store.Put(Record{ID: int64(i + 1), UpdatedAt: base, Value: "v"})
	}
	pager := NewPager(store)
	const limit = 10

	page, err := pager.FirstPage(limit)
	if err != nil {
		t.Fatal(err)
	}
	all := append([]Record(nil), page.Records...)
	// 删除当前页的第一条和最后一条（即上一页游标指向的记录！）。
	store.Delete(page.Records[0].ID)
	store.Delete(page.Records[len(page.Records)-1].ID)

	for page.HasMore {
		page, err = pager.NextPage(page.NextCursor, limit)
		if err != nil {
			t.Fatalf("NextPage: %v", err)
		}
		all = append(all, page.Records...)
	}
	// 快照时刻共 25 条，删除发生在快照之后，不应影响本次会话。
	if len(all) != n {
		t.Fatalf("快照内记录数 = %d, want %d", len(all), n)
	}
	seen := make(map[int64]bool)
	for _, r := range all {
		if seen[r.ID] {
			t.Fatalf("记录重复: id=%d", r.ID)
		}
		seen[r.ID] = true
	}
	// 新一轮翻页不应再看到被删除的记录。
	got, _ := drainAll(t, pager, limit)
	if len(got) != n-2 {
		t.Fatalf("新一轮翻页记录数 = %d, want %d", len(got), n-2)
	}
}

// TestCursorTampered 篡改游标的任何字节都必须被拒绝。
func TestCursorTampered(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		store.Put(Record{ID: int64(i + 1), UpdatedAt: base, Value: "v"})
	}
	pager := NewPager(store)
	page, err := pager.FirstPage(5)
	if err != nil {
		t.Fatal(err)
	}
	token := page.NextCursor.String()

	cases := map[string]Cursor{
		"空游标":      {},
		"非法base64": ParseCursor("!!!not-base64!!!"),
		"截断":       ParseCursor(token[:len(token)-4]),
		"翻转首字符":    ParseCursor(flipChar(token, 0)),
		"翻转中间字符":   ParseCursor(flipChar(token, len(token)/2)),
		"翻转末尾字符":   ParseCursor(flipChar(token, len(token)-1)),
		"其他Pager签发": func() Cursor {
			other := NewPager(store)
			pg, err := other.FirstPage(5)
			if err != nil {
				t.Fatal(err)
			}
			return pg.NextCursor
		}(),
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := pager.NextPage(c, 5)
			if !errors.Is(err, ErrCursorTampered) {
				t.Fatalf("err = %v, want ErrCursorTampered", err)
			}
		})
	}
}

func flipChar(s string, i int) string {
	b := []byte(s)
	if b[i] == 'A' {
		b[i] = 'B'
	} else {
		b[i] = 'A'
	}
	return string(b)
}

// TestCursorExpired 超过 TTL 未使用的游标过期。
func TestCursorExpired(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 20; i++ {
		store.Put(Record{ID: int64(i + 1), UpdatedAt: base, Value: "v"})
	}
	pager := NewPager(store, WithCursorTTL(30*time.Millisecond))
	page, err := pager.FirstPage(5)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	_, err = pager.NextPage(page.NextCursor, 5)
	if !errors.Is(err, ErrCursorExpired) {
		t.Fatalf("err = %v, want ErrCursorExpired", err)
	}
	// 过期后重新开启会话不受影响。
	if _, err := pager.FirstPage(5); err != nil {
		t.Fatalf("FirstPage after expiry: %v", err)
	}
}

// TestCursorMismatch 游标指向的记录不在快照中（构造场景：跨版本混用）。
func TestCursorMismatch(t *testing.T) {
	store := NewStore()
	base := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 10; i++ {
		store.Put(Record{ID: int64(i + 1), UpdatedAt: base, Value: "v"})
	}
	secret := []byte("test-secret")
	pager := NewPager(store, WithCursorSecret(secret))
	page, err := pager.FirstPage(3)
	if err != nil {
		t.Fatal(err)
	}
	// 用同一密钥伪造一个快照版本有效、但记录不存在的游标。
	fake := Cursor{token: encodeCursor(secret, cursorPayload{
		snapVersion: page.Version,
		updatedAt:   base,
		id:          9999,
	})}
	if _, err := pager.NextPage(fake, 3); !errors.Is(err, ErrCursorMismatch) {
		t.Fatalf("err = %v, want ErrCursorMismatch", err)
	}
}

// TestInvalidLimit 页大小必须为正数。
func TestInvalidLimit(t *testing.T) {
	pager := NewPager(NewStore())
	if _, err := pager.FirstPage(0); !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("FirstPage(0): err = %v, want ErrInvalidLimit", err)
	}
	if _, err := pager.NextPage(Cursor{}, -1); !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("NextPage(-1): err = %v, want ErrInvalidLimit", err)
	}
}

// TestEmptyStore 空存储返回空页。
func TestEmptyStore(t *testing.T) {
	pager := NewPager(NewStore())
	page, err := pager.FirstPage(10)
	if err != nil {
		t.Fatal(err)
	}
	if page.HasMore || len(page.Records) != 0 {
		t.Fatalf("空存储应返回空页: %+v", page)
	}
}
