package sql

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
)

type Writer struct {
	*Loader
	maps           map[string]string
	Mapper         Mapper
	versionField   string
	versionIndex   int
	versionDBField string
}

func NewWriterWithVersion(db *sql.DB, tableName string, modelType reflect.Type, versionField string, options ...Mapper) *Writer {
	var mapper Mapper
	if len(options) >= 1 {
		mapper = options[0]
	}
	return NewSqlWriterWithVersion(db, tableName, modelType, versionField, mapper)
}
func NewSqlWriterWithVersion(db *sql.DB, tableName string, modelType reflect.Type, versionField string, mapper Mapper, options ...func(i int) string) *Writer {
	var loader *Loader
	if mapper != nil {
		loader = NewSqlLoader(db, tableName, modelType, mapper.DbToModel, options...)
	} else {
		loader = NewSqlLoader(db, tableName, modelType, nil, options...)
	}
	if len(versionField) > 0 {
		index := FindFieldIndex(modelType, versionField)
		if index >= 0 {
			dbFieldName, exist := GetColumnNameByIndex(modelType, index)
			if !exist {
				dbFieldName = strings.ToLower(versionField)
			}
			return &Writer{Loader: loader, versionField: versionField, versionIndex: index, versionDBField: dbFieldName}
		}
	}
	return &Writer{Loader: loader, Mapper: mapper, versionField: versionField, versionIndex: -1}
}
func NewWriterWithMap(db *sql.DB, tableName string, modelType reflect.Type, mapper Mapper, options ...func(i int) string) *Writer {
	return NewSqlWriterWithVersion(db, tableName, modelType, "", mapper, options...)
}
func NewWriter(db *sql.DB, tableName string, modelType reflect.Type, options ...Mapper) *Writer {
	var mapper Mapper
	if len(options) >= 1 {
		mapper = options[0]
	}
	return NewWriterWithVersion(db, tableName, modelType, "", mapper)
}

func (s *Writer) Insert(ctx context.Context, model interface{}) (int64, error) {
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, model)
		if err != nil {
			return 0, err
		}
		if s.versionIndex >= 0 {
			return InsertWithVersion(ctx, s.Database, s.table, m2, s.versionIndex, s.BuildParam)
		}
		return Insert(ctx, s.Database, s.table, m2, s.BuildParam)
	}
	if s.versionIndex >= 0 {
		return InsertWithVersion(ctx, s.Database, s.table, model, s.versionIndex, s.BuildParam)
	}
	return Insert(ctx, s.Database, s.table, model, s.BuildParam)
}

func (s *Writer) Update(ctx context.Context, model interface{}) (int64, error) {
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
		if s.versionIndex >= 0 {
			return UpdateWithVersion(ctx, s.Database, s.table, m2, s.versionIndex, s.BuildParam)
		}
		return Update(ctx, s.Database, s.table, m2, s.BuildParam)
	}
	if s.versionIndex >= 0 {
		return UpdateWithVersion(ctx, s.Database, s.table, model, s.versionIndex, s.BuildParam)
	}
	return Update(ctx, s.Database, s.table, model, s.BuildParam)
}

func (s *Writer) Save(ctx context.Context, model map[string]interface{}) (int64, error) {
	if s.Mapper != nil {
		m2, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
		return Save(ctx, s.Database, s.table, m2)
	}
	return Save(ctx, s.Database, s.table, model)
}

func (s *Writer) Delete(ctx context.Context, id interface{}) (int64, error) {
	l := len(s.keys)
	if l == 1 {
		return Delete(ctx, s.Database, s.table, BuildQueryById(id, s.modelType, s.keys[0]), s.BuildParam)
	} else {
		ids := id.(map[string]interface{})
		return Delete(ctx, s.Database, s.table, MapToGORM(ids, s.modelType), s.BuildParam)
	}
}

func (s *Writer) Patch(ctx context.Context, model map[string]interface{}) (int64, error) {
	if s.Mapper != nil {
		_, err := s.Mapper.ModelToDb(ctx, &model)
		if err != nil {
			return 0, err
		}
	}
	MapToDB(&model, s.modelType)
	return Patch(ctx, s.Database, s.table, model, s.modelType, s.BuildParam)
}
