// Package pager 提供在并发写入下不漏不重、按「更新时间倒序 + id 倒序」
// 排序的游标分页。
//
// 正确性依赖两点：
//  1. 复合游标 (updated_at, id)：id 作为决胜键，重复排序键跨页不漏不重；
//  2. 快照隔离（MVCC）：一次翻页会话的所有页都读取同一版本快照，
//     翻页期间的插入/更新/删除不影响该会话的结果。
package pager

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// ErrInvalidVersion 表示请求的存储版本号非法（大于当前版本）。
var ErrInvalidVersion = errors.New("pager: invalid version")

// Record 是一条记录。排序键为 (UpdatedAt, ID)，按 UpdatedAt 倒序、
// UpdatedAt 相同时按 ID 倒序。
type Record struct {
	ID        int64
	UpdatedAt time.Time
	Value     string
}

// less 定义排序：UpdatedAt 倒序，平局时 ID 倒序。
func less(a, b Record) bool {
	if !a.UpdatedAt.Equal(b.UpdatedAt) {
		return a.UpdatedAt.After(b.UpdatedAt)
	}
	return a.ID > b.ID
}

// entry 是同一 ID 的一次写入版本；Record 为空表示墓碑（删除）。
type entry struct {
	version int64
	rec     Record
	deleted bool
}

// Store 是内存 MVCC 存储。每次 Put/Delete 使版本号 +1，历史版本保留，
// 直到不再被任何快照引用后被压缩回收。
type Store struct {
	mu      sync.RWMutex
	version int64
	entries map[int64][]entry // 每个 ID 的版本链，按 version 严格降序
	refs    map[int64]int     // 被引用的版本号 -> 引用计数
}

// NewStore 创建空存储。
func NewStore() *Store {
	return &Store{
		entries: make(map[int64][]entry),
		refs:    make(map[int64]int),
	}
}

// Version 返回当前版本号。
func (s *Store) Version() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.version
}

// Put 插入或更新一条记录（按 ID 覆盖），版本号 +1。
func (s *Store) Put(r Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.version++
	s.entries[r.ID] = append([]entry{{version: s.version, rec: r}}, s.entries[r.ID]...)
}

// Delete 删除指定 ID 的记录，版本号 +1。删除不存在的 ID 同样生效（墓碑）。
func (s *Store) Delete(id int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.version++
	s.entries[id] = append([]entry{{version: s.version, deleted: true}}, s.entries[id]...)
}

// RecordsAt 返回 version 时刻可见的全部记录，按 (UpdatedAt, ID) 倒序。
// version 大于当前版本时报 ErrInvalidVersion。
func (s *Store) RecordsAt(version int64) ([]Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if version > s.version {
		return nil, ErrInvalidVersion
	}
	recs := make([]Record, 0, len(s.entries))
	for _, chain := range s.entries {
		// 版本链按 version 降序，取第一个 <= version 的版本。
		i := sort.Search(len(chain), func(i int) bool {
			return chain[i].version <= version
		})
		if i < len(chain) && !chain[i].deleted {
			recs = append(recs, chain[i].rec)
		}
	}
	sort.Slice(recs, func(i, j int) bool { return less(recs[i], recs[j]) })
	return recs, nil
}

// Snapshot 是某一版本的只读视图。查询结果在首次使用时缓存，
// 用完后必须调用 Release 释放，以便存储回收历史版本。
type Snapshot struct {
	store   *Store
	version int64

	once sync.Once
	recs []Record
	err  error
}

// Snapshot 返回当前版本的只读快照视图。
func (s *Store) Snapshot() *Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refs[s.version]++
	return &Snapshot{store: s, version: s.version}
}

// SnapshotAt 返回指定历史版本的只读快照视图；版本号大于当前版本时报错。
func (s *Store) SnapshotAt(version int64) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version > s.version {
		return nil, ErrInvalidVersion
	}
	s.refs[version]++
	return &Snapshot{store: s, version: version}, nil
}

// pin 为 version 增加一次内部引用，供读操作期间防止 GC 回收数据。
func (s *Store) pin(version int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version > s.version {
		return ErrInvalidVersion
	}
	s.refs[version]++
	return nil
}

// unpin 释放 pin 增加的引用。
func (s *Store) unpin(version int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releaseLocked(version)
}

// Version 返回快照对应的存储版本号。
func (snap *Snapshot) Version() int64 { return snap.version }

// Records 返回快照时刻可见的全部记录，按 (UpdatedAt, ID) 倒序。
// 结果在首次调用时计算并缓存，多次调用返回同一快照内容。
func (snap *Snapshot) Records() ([]Record, error) {
	snap.once.Do(func() {
		if err := snap.store.pin(snap.version); err != nil {
			snap.err = err
			return
		}
		defer snap.store.unpin(snap.version)
		snap.recs, snap.err = snap.store.RecordsAt(snap.version)
	})
	return snap.recs, snap.err
}

// Release 释放快照。释放后存储可能回收该版本的历史数据。
func (snap *Snapshot) Release() {
	snap.store.mu.Lock()
	defer snap.store.mu.Unlock()
	snap.store.releaseLocked(snap.version)
	snap.store.gcLocked()
}

func (s *Store) releaseLocked(version int64) {
	if n, ok := s.refs[version]; ok {
		if n <= 1 {
			delete(s.refs, version)
		} else {
			s.refs[version] = n - 1
		}
	}
}

// gcLocked 压缩不再被任何快照引用的历史版本。
// 对每个 ID 的版本链（version 降序），保留：
//   - 全局最新版本（当前视图需要）；
//   - 对每个被引用版本 t：链中第一个 version <= t 的版本（该快照的可见版本）。
func (s *Store) gcLocked() {
	if len(s.refs) == 0 {
		return
	}
	thresholds := make([]int64, 0, len(s.refs))
	for v := range s.refs {
		thresholds = append(thresholds, v)
	}
	sort.Slice(thresholds, func(i, j int) bool { return thresholds[i] > thresholds[j] })

	for id, chain := range s.entries {
		keep := make([]entry, 0, len(chain))
		if len(chain) > 0 {
			keep = append(keep, chain[0]) // 全局最新版本必须保留
		}
		for _, t := range thresholds {
			i := sort.Search(len(chain), func(i int) bool {
				return chain[i].version <= t
			})
			if i < len(chain) {
				keep = append(keep, chain[i])
			}
		}
		// 去重（保持降序）。
		dedup := keep[:0]
		var last int64 = -1
		for _, e := range keep {
			if e.version != last {
				dedup = append(dedup, e)
				last = e.version
			}
		}
		if len(dedup) == 0 {
			delete(s.entries, id)
		} else {
			s.entries[id] = dedup
		}
	}
}

// String 便于调试输出。
func (s *Store) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("Store{version=%d, ids=%d, refs=%d}", s.version, len(s.entries), len(s.refs))
}
