package sql

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type SqlHealthChecker struct {
	db      *sql.DB
	name    string
	timeout time.Duration
}

func NewHealthChecker(db *sql.DB, name string, timeout time.Duration) *SqlHealthChecker {
	return &SqlHealthChecker{db, name, timeout}
}
func NewSqlHealthChecker(db *sql.DB, options ...string) *SqlHealthChecker {
	var name string
	if len(options) >= 1 && len(options[0]) > 0 {
		name = options[0]
	} else {
		name = "sql"
	}
	return NewHealthChecker(db, name, 4 * time.Second)
}

func (s *SqlHealthChecker) Name() string {
	return s.name
}

func (s *SqlHealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{}, 0)
	if s.timeout > 0 {
		ctx, _ = context.WithTimeout(ctx, s.timeout)
	}

	checkerChan := make(chan error)
	go func() {
		err := s.db.Ping()
		checkerChan <- err
	}()
	select {
	case err := <-checkerChan:
		if err != nil {
			return res, err
		}
		res["status"] = "success"
		return res, err
	case <-ctx.Done():
		return nil, errors.New("connection timout")
	}
}

func (s *SqlHealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if data == nil {
		data = make(map[string]interface{}, 0)
	}
	data["error"] = err.Error()
	return data
}
