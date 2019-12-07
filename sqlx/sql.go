package sqlx

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync/atomic"
)

const (
	retryBadConnNum = 2
)

//db support configurable retry connection number
type DB struct {
	*sql.DB
	maxBadConnRetries int64
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	var tx *sql.Tx

	var fn = func() error {
		var err error
		tx, err = db.DB.BeginTx(ctx, opts)
		return err
	}
	var err = db.retryFunc(fn)
	return tx, err
}

func (db *DB) Conn(ctx context.Context) (*sql.Conn, error) {
	var conn *sql.Conn

	var fn = func() error {
		var err error
		conn, err = db.DB.Conn(ctx)
		return err
	}
	var err = db.retryFunc(fn)
	return conn, err
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	var res sql.Result

	var fn = func() error {
		var err error
		res, err = db.DB.ExecContext(ctx, query, args...)
		return err
	}
	var err = db.retryFunc(fn)
	return res, err
}

func (db *DB) PingContext(ctx context.Context) error {
	var fn = func() error {
		var err error
		err = db.DB.PingContext(ctx)
		return err
	}
	return db.retryFunc(fn)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	var rows *sql.Rows
	var fn = func() error {
		var err error
		rows, err = db.DB.QueryContext(ctx, query, args...)
		return err
	}
	var err = db.retryFunc(fn)
	return rows, err
}

func (db *DB) SetMaxBadConnRetries(d int64) {
	if d <= retryBadConnNum {
		d = retryBadConnNum
	}
	atomic.StoreInt64(&(db.maxBadConnRetries), d)
}

func (db *DB) retryFunc(fn func() error) error {
	var err error

	var maxRetries = atomic.LoadInt64(&(db.maxBadConnRetries))
	for i := int64(0); i < maxRetries; i++ {
		err = fn()
		if err != driver.ErrBadConn {
			break
		}
	}
	return err
}

func NewDB(db *sql.DB) *DB {
	return &DB{
		DB:                db,
		maxBadConnRetries: retryBadConnNum,
	}
}
