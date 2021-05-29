package sql

import (
	"context"
	"database/sql"
)

type SqlWriter struct {
	db         *sql.DB
	tableName  string
	BuildParam func(i int) string
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewSqlWriterWithMap(db *sql.DB, tableName string, mp func(context.Context, interface{}) (interface{}, error), options ...func(i int) string) *SqlWriter {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	return &SqlWriter{db: db, tableName: tableName, BuildParam: buildParam, Map: mp}
}

func NewSqlWriter(db *sql.DB, tableName string, options ...func(ctx context.Context, model interface{}) (interface{}, error)) *SqlWriter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlWriterWithMap(db, tableName, mp)
}

func (w *SqlWriter) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, err := Save(ctx, w.db, w.tableName, m2)
		return err
	}
	_, err := Save(ctx, w.db, w.tableName, model)
	return err
}
