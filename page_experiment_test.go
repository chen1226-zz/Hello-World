package pager

import (
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

// pageTestRuns 默认跑 1 轮；make page-test 通过 PAGE_TEST_RUNS=1000 加压。
func pageTestRuns(t *testing.T) int {
	t.Helper()
	if v := os.Getenv("PAGE_TEST_RUNS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			t.Fatalf("非法 PAGE_TEST_RUNS=%q", v)
		}
		return n
	}
	return 1
}

// TestConcurrentPaging 复现并验证修复：
// 后台 goroutine 持续插入/更新/删除，客户端用游标逐页拉取，
// 断言所有页拼起来恰好等于游标会话快照、无重复、无遗漏、顺序正确。
func TestConcurrentPaging(t *testing.T) {
	runs := pageTestRuns(t)
	for run := 0; run < runs; run++ {
		runConcurrentPaging(t, int64(run)+1)
	}
}

func runConcurrentPaging(t *testing.T, seed int64) {
	store := NewStore()
	pager := NewPager(store)

	const (
		initialRecords = 500
		sharedStamps   = 7 // 大量记录共享少量时间戳，制造重复排序键
		writers        = 4
		warmupOps      = 400 // 翻页开始前每个 writer 完成的操作数
		pageLimit      = 37  // 非整除，制造不规则页边界
	)
	base := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	stamp := func(i int) time.Time {
		return base.Add(-time.Duration(i%sharedStamps) * time.Second)
	}
	for i := 0; i < initialRecords; i++ {
		store.Put(Record{ID: int64(i + 1), UpdatedAt: stamp(i), Value: "init"})
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			wrng := rand.New(rand.NewSource(seed*1000 + int64(w)))
			nextID := int64(initialRecords + w*100000)
			ops := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				switch wrng.Intn(3) {
				case 0: // 插入：大量复用相同时间戳，也偶发新时间戳
					nextID++
					ts := stamp(int(nextID))
					if wrng.Intn(10) == 0 {
						ts = time.Now()
					}
					store.Put(Record{ID: nextID, UpdatedAt: ts, Value: "inserted"})
				case 1: // 更新既有记录（改变其排序位置）
					id := int64(wrng.Intn(initialRecords) + 1)
					store.Put(Record{ID: id, UpdatedAt: stamp(wrng.Intn(100)), Value: "updated"})
				default: // 删除既有记录
					id := int64(wrng.Intn(initialRecords) + 1)
					store.Delete(id)
				}
				ops++
				if ops == warmupOps {
					// 热身完成，让出 CPU 等待客户端开始翻页。
					runtime.Gosched()
				}
			}
		}(w)
	}

	// 客户端：游标逐页拉取。
	page, err := pager.FirstPage(pageLimit)
	if err != nil {
		t.Fatalf("seed=%d FirstPage: %v", seed, err)
	}
	snapVersion := page.Version
	var all []Record
	seen := make(map[int64]struct{})
	pages := 0
	for {
		if page.Version != snapVersion {
			t.Fatalf("seed=%d 页版本漂移: %d -> %d", seed, snapVersion, page.Version)
		}
		if len(page.Records) > pageLimit {
			t.Fatalf("seed=%d 页大小 %d 超过 limit %d", seed, len(page.Records), pageLimit)
		}
		for i, r := range page.Records {
			if _, dup := seen[r.ID]; dup {
				t.Fatalf("seed=%d 记录重复: id=%d", seed, r.ID)
			}
			seen[r.ID] = struct{}{}
			if i > 0 && less(r, page.Records[i-1]) {
				t.Fatalf("seed=%d 页内顺序错误: %+v 排在 %+v 之前", seed, r, page.Records[i-1])
			}
		}
		all = append(all, page.Records...)
		pages++
		if !page.HasMore {
			break
		}
		page, err = pager.NextPage(page.NextCursor, pageLimit)
		if err != nil {
			t.Fatalf("seed=%d NextPage: %v", seed, err)
		}
		runtime.Gosched() // 给后台写入制造交错机会
	}
	close(stop)
	wg.Wait()

	// 断言：拼接结果 == 会话快照时刻的可见集合。
	want, err := store.RecordsAt(snapVersion)
	if err != nil {
		t.Fatalf("seed=%d RecordsAt(%d): %v", seed, snapVersion, err)
	}
	if len(all) != len(want) {
		t.Fatalf("seed=%d 记录数不一致: got %d, want %d（快照版本 %d）",
			seed, len(all), len(want), snapVersion)
	}
	for i := range want {
		if all[i] != want[i] {
			t.Fatalf("seed=%d 第 %d 条不一致: got %+v, want %+v", seed, i, all[i], want[i])
		}
	}
	if pages < 2 {
		t.Fatalf("seed=%d 页数 %d，未覆盖跨页场景", seed, pages)
	}
	t.Logf("seed=%d 快照版本=%d 页数=%d 记录=%d 最终版本=%d",
		seed, snapVersion, pages, len(all), store.Version())
}
