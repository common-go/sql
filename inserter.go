package sql

import (
	"context"
	"database/sql"
)

type Inserter struct {
	db         *sql.DB
	tableName  string
	BuildParam func(i int) string
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}
func NewInserter(db *sql.DB, tableName string, options ...func(context.Context, interface{}) (interface{}, error)) *Inserter {
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	return NewSqlInserter(db, tableName, mp)
}
func NewSqlInserter(db *sql.DB, tableName string, mp func(context.Context, interface{}) (interface{}, error), options...func(i int) string) *Inserter {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	return &Inserter{db: db, tableName: tableName, BuildParam: buildParam, Map: mp}
}

func (w *Inserter) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		queryInsert, values := BuildInsert(w.tableName, m2, 0, w.BuildParam)
		_, err := w.db.ExecContext(ctx, queryInsert, values...)
		return err
	}
	queryInsert, values := BuildInsert(w.tableName, model, 0, w.BuildParam)
	_, err := w.db.ExecContext(ctx, queryInsert, values...)
	return err
}
