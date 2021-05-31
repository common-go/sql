package sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

func ExecuteStatements(ctx context.Context, db *sql.DB, sts []Statement) (int64, error) {
	return ExecuteBatch(ctx, db, sts, true, false)
}
func ExecuteAll(ctx context.Context, db *sql.DB, stmts []Statement) (int64, error) {
	tx, er1 := db.Begin()
	if er1 != nil {
		return 0, er1
	}
	var count int64
	count = 0
	for _, stmt := range stmts {
		r2, er3 := tx.ExecContext(ctx, stmt.Query, stmt.Args...)
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
	return count, er6
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
	stmts := make([]Statement, 0)
	s := &DefaultStatements{Statements: stmts, SuccessFirst: successFirst}
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

type FieldDB struct {
	JSON   string
	column string
	field  string
	index  int
	key    bool
	Update bool
	true   string
	false  string
}

func MakeSchema(modelType reflect.Type) ([]string, []string, map[string]FieldDB) {
	numField := modelType.NumField()
	columns := make([]string, 0)
	keys := make([]string, 0)
	schema := make(map[string]FieldDB, 0)
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
							f := FieldDB{
								JSON: json,
								column: col,
								index: idx,
								key: isKey,
								Update: update,
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
func BuildUpdateBatch(table string, models interface{}, buildParam func(int) string) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() <= 0 {
		return nil, nil
	}
	first := s.Index(0).Interface()
	modelType := reflect.TypeOf(first)
	cols, keys, schema := MakeSchema(modelType)
	slen := s.Len()
	stmts := make([]Statement, 0)
	for j := 0; j < slen; j++ {
		model := s.Index(j).Interface()
		mv := reflect.ValueOf(model)
		values := make([]string, 0)
		where := make([]string, 0)
		args := make([]interface{}, 0)
		i := 1
		for _, col := range cols {
			fdb := schema[col]
			if !fdb.key && !fdb.Update {
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
					values = append(values, col + "=null")
				} else {
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, col + "=" + v)
					} else {
						values = append(values, col + "=" + buildParam(i))
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
func BuildInsertBatch(db *sql.DB, table string, models interface{}, options ...func(int) string) (string, []interface{}, error) {
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
	first := s.Index(0).Interface()
	modelType := reflect.TypeOf(first)
	cols, _, schema := MakeSchema(modelType)
	driver := GetDriver(db)
	slen := s.Len()
	if driver != DriverOracle {
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			values := make([]string, 0)
			i := 1
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
						values = append(values, buildParam(i))
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
			i := 1
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
						values = append(values, buildParam(i))
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
func BuildSaveBatch(db *sql.DB, table string, models interface{}) ([]Statement, error) {
	s := reflect.Indirect(reflect.ValueOf(models))
	if s.Kind() != reflect.Slice {
		return nil, fmt.Errorf("models is not a slice")
	}
	if s.Len() == 0 {
		return nil, nil
	}
	buildParam := GetBuild(db)
	first := s.Index(0).Interface()
	modelType := reflect.TypeOf(first)
	cols, keys, schema := MakeSchema(modelType)
	slen := s.Len()
	stmts := make([]Statement, 0)
	driver := GetDriver(db)
	if driver == DriverPostgres || driver == DriverMysql {
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			iCols := make([]string, 0)
			values := make([]string, 0)
			setColumns := make([]string, 0)
			args := make([]interface{}, 0)
			i := 1
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
						values = append(values, buildParam(i))
						i = i + 1
						args = append(args, fieldValue)
					}
				}
			}
			for _, col := range cols {
				fdb := schema[col]
				if !fdb.key && !fdb.Update {
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
						setColumns = append(setColumns, col + "=null")
					} else {
						v, ok := GetDBValue(fieldValue)
						if ok {
							setColumns = append(setColumns, col + "=" + v)
						} else {
							setColumns = append(setColumns, col + "=" + buildParam(i))
							i = i + 1
							args = append(args, fieldValue)
						}
					}
				}
			}
			var query string
			if driver == DriverPostgres {
				query = fmt.Sprintf("insert into %s(%s) values (%s) on conflict (%s) do update set %s",
					table,
					strings.Join(iCols, ","),
					strings.Join(values, ","),
					strings.Join(keys, ","),
					strings.Join(setColumns, ","),
				)
			} else {
				query = fmt.Sprintf("insert into %s(%s) values (%s) on duplicate key update %s",
					table,
					strings.Join(iCols, ","),
					strings.Join(values, ","),
					strings.Join(setColumns, ","),
				)
			}
			s := Statement{Query: query, Args: args}
			stmts = append(stmts, s)
		}
	} else if driver == DriverSqlite3 {
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			mv := reflect.ValueOf(model)
			iCols := make([]string, 0)
			values := make([]string, 0)
			args := make([]interface{}, 0)
			i := 1
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
				iCols = append(iCols, col)
				if isNil {
					values = append(values, "null")
				} else {
					v, ok := GetDBValue(fieldValue)
					if ok {
						values = append(values, v)
					} else {
						values = append(values, buildParam(i))
						i = i + 1
						args = append(args, fieldValue)
					}
				}
			}
			query := fmt.Sprintf("insert or replace into %s(%s) values (%s)", table, strings.Join(iCols, ","), strings.Join(values, ","))
			s := Statement{Query: query, Args: args}
			stmts = append(stmts, s)
		}
	} else if driver == DriverOracle {
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			uniqueCols := make([]string, 0)
			inColumns := make([]string, 0)
			variables := make([]string, 0)
			setColumns := make([]string, 0)
			values := make([]interface{}, 0)
			insertCols := make([]string, 0)
			attrs, unique, _, err := ExtractBySchema(model, cols, schema)
			sorted := SortedKeys(attrs)
			if err != nil {
				return nil, fmt.Errorf("cannot extract object's values: %w", err)
			}
			for v, key := range sorted {
				values = append(values, attrs[key])
				tkey := `"` + strings.Replace(key, `"`, `""`, -1) + `"`
				tkey = strings.ToUpper(tkey)
				setColumns = append(setColumns, "a."+tkey+" = temp."+tkey)
				inColumns = append(inColumns, "temp."+key)
				variables = append(variables, fmt.Sprintf(":%d "+tkey, v))
				insertCols = append(insertCols, tkey)
			}
			for key, val := range unique {
				tkey := `"` + strings.Replace(key, `"`, `""`, -1) + `"`
				tkey = strings.ToUpper(tkey)
				onDupe := "a." + tkey + " = " + "temp." + tkey
				uniqueCols = append(uniqueCols, onDupe)
				variables = append(variables, fmt.Sprintf(":%s "+tkey, key))
				inColumns = append(inColumns, "temp."+key)
				values = append(values, val)
				insertCols = append(insertCols, tkey)
			}
			query := fmt.Sprintf("MERGE INTO %s a USING (SELECT %s FROM dual) temp ON  (%s) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
				`"`+strings.Replace(table, `"`, `""`, -1)+`"`,
				strings.Join(variables, ", "),
				strings.Join(uniqueCols, " AND "),
				strings.Join(setColumns, ", "),
				strings.Join(insertCols, ", "),
				strings.Join(inColumns, ", "),
			)
			s := Statement{Query: query, Args: values}
			stmts = append(stmts, s)
		}
	} else if driver == DriverMssql {
		for j := 0; j < slen; j++ {
			model := s.Index(j).Interface()
			uniqueCols := make([]string, 0)
			dbColumns := make([]string, 0)
			variables := make([]string, 0)
			setColumns := make([]string, 0)
			values := make([]interface{}, 0)
			attrs, unique, _, err := ExtractBySchema(model, cols, schema)
			sorted := SortedKeys(attrs)
			if err != nil {
				return nil, fmt.Errorf("cannot extract object's values: %w", err)
			}
			for _, key := range sorted {
				//mainScope.AddToVars(attrs[key])
				tkey := `"` + strings.Replace(key, `"`, `""`, -1) + `"`
				dbColumns = append(dbColumns, tkey)
				variables = append(variables, "?")
				values = append(values, attrs[key])
				setColumns = append(setColumns, tkey+" = temp."+tkey)
			}
			for i, val := range unique {
				tkey := `"` + strings.Replace(i, `"`, `""`, -1) + `"`
				dbColumns = append(dbColumns, `"`+strings.Replace(tkey, `"`, `""`, -1)+`"`)
				variables = append(variables, "?")
				values = append(values, val)
				onDupe := table + "." + tkey + " = " + "temp." + tkey
				uniqueCols = append(uniqueCols, onDupe)
			}
			query := fmt.Sprintf("MERGE INTO %s USING (VALUES %s) AS temp (%s) ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES %s;",
				`"`+strings.Replace(table, `"`, `""`, -1)+`"`,
				strings.Join(variables, ", "),
				strings.Join(dbColumns, ", "),
				strings.Join(uniqueCols, " AND "),
				strings.Join(setColumns, ", "),
				strings.Join(dbColumns, ", "),
				strings.Join(variables, ", "),
			)
			s := Statement{Query: query, Args: values}
			stmts = append(stmts, s)
		}
	}
	return stmts, nil
}

func InsertMany(ctx context.Context, db *sql.DB, tableName string, models interface{}, options ...func(int) string) (int64, error) {
	query, args, er1 := BuildInsertBatch(db, tableName, models, options...)
	if er1 != nil {
		return 0, er1
	}
	x, er2 := db.ExecContext(ctx, query, args...)
	if er2 != nil {
		return 0, er2
	}
	return x.RowsAffected()
}
func UpdateMany(ctx context.Context, db *sql.DB, tableName string, models interface{}, options ...func(int) string) (int64, error) {
	var buildParam func(int) string
	if len(options) > 0 {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	stmts, er1 := BuildUpdateBatch(tableName, models, buildParam)
	if er1 != nil {
		return 0, er1
	}
	return ExecuteAll(ctx, db, stmts)
}
func SaveMany(ctx context.Context, db *sql.DB, tableName string, models interface{}) (int64, error) {
	stmts, er1 := BuildSaveBatch(db, tableName, models)
	if er1 != nil {
		return 0, er1
	}
	_, err := ExecuteAll(ctx, db, stmts)
	total := int64(len(stmts))
	return total, err
}
