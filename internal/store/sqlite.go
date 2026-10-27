package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	_ "modernc.org/sqlite"
)

const schema = `
-- owner_epoch 每換一次 owner 就加一。寫入 change 時比對它，擋下已經失去 ownership 的舊 owner。
CREATE TABLE IF NOT EXISTS docs (
	doc_id      TEXT PRIMARY KEY,
	title       TEXT NOT NULL,
	created_by  TEXT NOT NULL,
	created_at  INTEGER NOT NULL,
	owner_node  TEXT NOT NULL DEFAULT '',
	owner_epoch INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS members (
	doc_id  TEXT NOT NULL,
	user_id TEXT NOT NULL,
	role    TEXT NOT NULL,
	PRIMARY KEY (doc_id, user_id)
) WITHOUT ROWID;

-- operation store。主鍵 (doc_id, seq) 對應 Cassandra 的 partition key 與 clustering key：
-- 同一份文件的 change 依序放在一起，同一個 seq 也只可能寫入一次。
CREATE TABLE IF NOT EXISTS changes (
	doc_id     TEXT NOT NULL,
	seq        INTEGER NOT NULL,
	client_id  TEXT NOT NULL,
	client_seq INTEGER NOT NULL,
	user_id    TEXT NOT NULL,
	op         TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY (doc_id, seq)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS snapshots (
	doc_id     TEXT NOT NULL,
	seq        INTEGER NOT NULL,
	text       TEXT NOT NULL,
	clients    TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	PRIMARY KEY (doc_id, seq)
) WITHOUT ROWID;
`

// Store 保存文件、成員、change 與 snapshot。
//
// 寫入走單一連線（SQLite 同時只允許一個 writer），讀取另開連線池：WAL 模式下讀不會擋寫。
// change 的寫入由 writeLoop 合併成批次，一次 commit（一次 fsync）涵蓋多份文件的多個編輯。
type Store struct {
	w *sql.DB
	r *sql.DB

	appends chan appendReq
	done    chan struct{}
	wg      sync.WaitGroup

	batches  atomic.Int64
	appended atomic.Int64
}

// Open 開啟資料庫。synchronous=FULL：commit 回傳前 WAL 已經 fsync，ack 過的編輯斷電也不會遺失。
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	base := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)"
	w, err := open(base+"&_pragma=synchronous(FULL)", 1)
	if err != nil {
		return nil, err
	}
	if _, err := w.Exec(schema); err != nil {
		w.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	r, err := open(base+"&_pragma=query_only(1)", 8)
	if err != nil {
		w.Close()
		return nil, err
	}
	s := &Store{w: w, r: r, appends: make(chan appendReq, 4096), done: make(chan struct{})}
	s.wg.Add(1)
	go s.writeLoop()
	return s, nil
}

func open(dsn string, conns int) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(conns)
	db.SetMaxIdleConns(conns)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	return db, nil
}

func (s *Store) Close() error {
	close(s.done)
	s.wg.Wait()
	s.r.Close()
	return s.w.Close()
}

// WriteStats 回傳 commit 的批次數與寫入的 change 數。
func (s *Store) WriteStats() (batches, changes int64) {
	return s.batches.Load(), s.appended.Load()
}
