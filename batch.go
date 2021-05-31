package sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// Field model field definition
type Field struct {
	Tags  map[string]string
	Value reflect.Value
	Type  reflect.Type
}

func GetMapField(object interface{}) []Field {
	modelType := reflect.TypeOf(object)
	value := reflect.Indirect(reflect.ValueOf(object))
	var result []Field
	numField := modelType.NumField()

	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		selectField := Field{Value: value.Field(i), Type: modelType}
		gormTag, ok := field.Tag.Lookup("gorm")
		tag := make(map[string]string)
		tag["fieldName"] = field.Name
		if ok {
			str1 := strings.Split(gormTag, ";")
			for k := 0; k < len(str1); k++ {
				str2 := strings.Split(str1[k], ":")
				if len(str2) == 1 {
					tag[str2[0]] = str2[0]
					selectField.Tags = tag
				} else {
					tag[str2[0]] = str2[1]
					selectField.Tags = tag
				}
			}
			result = append(result, selectField)
		}
	}
	return result
}

func ExecuteStatements(ctx context.Context, db *sql.DB, sts []Statement) (int64, error) {
	return ExecuteBatch(ctx, db, sts, true, false)
}
func ExecuteAll(ctx context.Context, db *sql.DB, sts []Statement) (int64, error) {
	return ExecuteBatch(ctx, db, sts, false, true)
}
func ExecuteBatch(ctx context.Context, db *sql.DB, sts []Statement, firstRowSuccess bool, countAll bool) (int64, error) {
	if sts == nil || len(sts) == 0 {
		return 0, nil
	}
	driver := GetDriver(db)
	tx, er0 := db.Begin()
	if er0 != nil {
		return 0, er0
	}
	result, er1 := tx.ExecContext(ctx, sts[0].Query, sts[0].Args...)
	if er1 != nil {
		_ = tx.Rollback()
		str := er1.Error()
		if driver == DriverPostgres && strings.Contains(str, "pq: duplicate key value violates unique constraint") {
			return 0, nil
		} else if driver == DriverMysql && strings.Contains(str, "Error 1062: Duplicate entry") {
			return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
		} else if driver == DriverOracle && strings.Contains(str, "ORA-00001: unique constraint") {
			return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
		} else if driver == DriverMssql && strings.Contains(str, "Violation of PRIMARY KEY constraint") {
			return 0, nil //Violation of PRIMARY KEY constraint 'PK_aa'. Cannot insert duplicate key in object 'dbo.aa'. The duplicate key value is (b, 2).
		} else if driver == DriverSqlite3 && strings.Contains(str, "UNIQUE constraint failed") {
			return 0, nil
		} else {
			return 0, er1
		}
	}
	rowAffected, er2 := result.RowsAffected()
	if er2 != nil {
		tx.Rollback()
		return 0, er2
	}
	if firstRowSuccess {
		if rowAffected == 0 {
			return 0, nil
		}
	}
	count := rowAffected
	for i := 1; i < len(sts); i++ {
		r2, er3 := tx.ExecContext(ctx, sts[i].Query, sts[i].Args...)
		if er3 != nil {
			er4 := tx.Rollback()
			if er4 != nil {
				return count, er4
			}
			return count, er3
		}
		a2, er5 := r2.RowsAffected()
		if er5 != nil {
			tx.Rollback()
			return count, er5
		}
		count = count + a2
	}
	er6 := tx.Commit()
	if er6 != nil {
		return count, er6
	}
	if countAll {
		return count, nil
	}
	return 1, nil
}

type Statement struct {
	Query string        `mapstructure:"sql" json:"sql,omitempty" gorm:"column:sql" bson:"sql,omitempty" dynamodbav:"sql,omitempty" firestore:"sql,omitempty"`
	Args  []interface{} `mapstructure:"args" json:"args,omitempty" gorm:"column:args" bson:"args,omitempty" dynamodbav:"args,omitempty" firestore:"args,omitempty"`
}
type Statements interface {
	Exec(ctx context.Context, db *sql.DB) (int64, error)
	Add(sql string, args []interface{}) Statements
	Clear() Statements
}

func NewDefaultStatements(successFirst bool) *DefaultStatements {
	stms := make([]Statement, 0)
	s := &DefaultStatements{Statements: stms, SuccessFirst: successFirst}
	return s
}
func NewStatements(successFirst bool) Statements {
	return NewDefaultStatements(successFirst)
}

type DefaultStatements struct {
	Statements   []Statement
	SuccessFirst bool
}

func (s *DefaultStatements) Exec(ctx context.Context, db *sql.DB) (int64, error) {
	if s.SuccessFirst {
		return ExecuteStatements(ctx, db, s.Statements)
	} else {
		return ExecuteAll(ctx, db, s.Statements)
	}
}
func (s *DefaultStatements) Add(sql string, args []interface{}) Statements {
	var stm = Statement{Query: sql, Args: args}
	s.Statements = append(s.Statements, stm)
	return s
}
func (s *DefaultStatements) Clear() Statements {
	s.Statements = s.Statements[:0]
	return s
}

type fieldDB struct {
	json   string
	column string
	field  string
	index  int
	key    bool
	update bool
	true   string
	false  string
}

func makeSchema(modelType reflect.Type) ([]string, []string, map[string]fieldDB) {
	numField := modelType.NumField()
	columns := make([]string, 0)
	keys := make([]string, 0)
	schema := make(map[string]fieldDB, 0)
	for idx := 0; idx < numField; idx++ {
		field := modelType.Field(idx)
		tag, _ := field.Tag.Lookup("gorm")
		if !strings.Contains(tag, IgnoreReadWrite) {
			update := !strings.Contains(tag, "update:false")
			if has := strings.Contains(tag, "column"); has {
				json := field.Name
				col := json
				str1 := strings.Split(tag, ";")
				num := len(str1)
				for i := 0; i < num; i++ {
					str2 := strings.Split(str1[i], ":")
					for j := 0; j < len(str2); j++ {
						if str2[j] == "column" {
							isKey := strings.Contains(tag, "primary_key")
							col = str2[j+1]
							columns = append(columns, col)
							if isKey {
								keys = append(keys, col)
							}

							jTag, jOk := field.Tag.Lookup("json")
							if jOk {
								tagJsons := strings.Split(jTag, ",")
								json = tagJsons[0]
							}
							f := fieldDB{
								json: json,
								column: col,
								index: idx,
								key: isKey,
								update: update,
							}
							tTag, tOk := field.Tag.Lookup("true")
							if tOk {
								f.true = tTag
								fTag, fOk := field.Tag.Lookup("false")
								if fOk {
									f.false = fTag
								}
							}
							schema[col] = f
						}
					}
				}
			}
		}
	}
	return columns, keys, schema
}
func BuildUpdateBatch(table string, models interface{}, buildParam func(int) string, options ...int) ([]Statement, error) {
	i := 0
	if len(options) > 0 && options[0] > 0 {
		i = options[0]
	}
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() == 0 {
		return nil, nil
	}
	first := s.Index(i).Interface()
	modelType := reflect.TypeOf(first)
	cols, keys, schema := makeSchema(modelType)
	slen := s.Len()
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		mv := reflect.ValueOf(model)
		values := make([]string, 0)
		where := make([]string, 0)
		args := make([]interface{}, 0)
		for _, col := range cols {
			fdb := schema[col]
			if !fdb.key {
				f := mv.Field(fdb.index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if isNil {
					values = append(values, "null")
				} else {
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, col + "=" + v)
					} else {
						values = append(values, col + "=" + buildParam(i+1))
						i = i + 1
						args = append(args, fieldValue)
					}
				}
			}
		}
		for _, col := range keys {
			fdb := schema[col]
			f := mv.Field(fdb.index)
			fieldValue := f.Interface()
			if f.Kind() == reflect.Ptr {
				if !reflect.ValueOf(fieldValue).IsNil() {
					fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
				}
			}
			v, ok := GetDBValue(fieldValue)
			if ok {
				where = append(where, col + "=" + v)
			} else {
				where = append(where, col + "=" + buildParam(i+1))
				i = i + 1
				args = append(args, fieldValue)
			}
		}
		query := fmt.Sprintf("update %v set %v where %v", table, strings.Join(values, ","), strings.Join(where, ","))
		s := Statement{Query: query, Args: args}
		stmts = append(stmts, s)
	}
	return stmts, nil
}
func BuildInsertBatch(db *sql.DB, table string, models interface{}, i int, options ...func(int) string) (string, []interface{}, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}

	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return "", nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() == 0 {
		return "", nil, nil
	}
	placeholders := make([]string, 0)
	args := make([]interface{}, 0)
	first := s.Index(i).Interface()
	modelType := reflect.TypeOf(first)
	cols, _, schema := makeSchema(modelType)
	driver := GetDriver(db)
	slen := s.Len()
	if driver != DriverOracle {
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			values := make([]string, 0)
			for _, col := range cols {
				fdb := schema[col]
				f := mv.Field(fdb.index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if isNil {
					values = append(values, "null")
				} else {
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, v)
					} else {
						values = append(values, buildParam(i+1))
						i = i + 1
						args = append(args, fieldValue)
					}
				}
			}
			x := "(" + strings.Join(values, ",") + ")"
			placeholders = append(placeholders, x)
		}
		query := fmt.Sprintf(fmt.Sprintf("insert into %s (%s) values %s",
			table,
			strings.Join(cols, ","),
			strings.Join(placeholders, ","),
		))
		return query, args, nil
	} else {
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			iCols := make([]string, 0)
			values := make([]string, 0)
			for _, col := range cols {
				fdb := schema[col]
				f := mv.Field(fdb.index)
				fieldValue := f.Interface()
				isNil := false
				if f.Kind() == reflect.Ptr {
					if reflect.ValueOf(fieldValue).IsNil() {
						isNil = true
					} else {
						fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
					}
				}
				if !isNil {
					iCols = append(iCols, col)
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, v)
					} else {
						values = append(values, buildParam(i+1))
						i = i + 1
						args = append(args, fieldValue)
					}
				}
			}
			x := fmt.Sprintf("into %s(%s)values(%s)", table, strings.Join(iCols, ","), strings.Join(values, ","))
			placeholders = append(placeholders, x)
		}
		query := fmt.Sprintf("insert all %s select * from dual", strings.Join(placeholders, " "))
		return query, args, nil
	}
}

func InsertMany(ctx context.Context, db *sql.DB, tableName string, models interface{}, options ...func(int) string) (int64, error) {
	query, args, er1 := BuildInsertBatch(db, tableName, models, 0, options...)
	if er1 != nil {
		return 0, er1
	}
	x, er2 := db.ExecContext(ctx, query, args...)
	if er2 != nil {
		return 0, er2
	}
	return x.RowsAffected()
}
