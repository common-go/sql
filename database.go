package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DBName          = "column"
	PrimaryKey      = "primary_key"
	IgnoreReadWrite = "-"

	DriverPostgres   = "postgres"
	DriverMysql      = "mysql"
	DriverMssql      = "mssql"
	DriverOracle     = "oracle"
	DriverSqlite3    = "sqlite3"
	DriverNotSupport = "no support"
)

func OpenByConfig(c Config) (*sql.DB, error) {
	if c.Mock {
		return nil, nil
	}
	if c.Retry.Retry1 <= 0 {
		return open(c)
	} else {
		durations := DurationsFromValue(c.Retry, "Retry", 9)
		return Open(c, durations...)
	}
}
func open(c Config) (*sql.DB, error) {
	dsn := c.DataSourceName
	if len(dsn) == 0 {
		dsn = BuildDataSourceName(c)
	}
	db, err := sql.Open(c.Driver, dsn)
	if err != nil {
		return db, err
	}
	if c.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(time.Duration(c.ConnMaxLifetime) * time.Second)
	}
	if c.MaxIdleConns > 0 {
		db.SetMaxIdleConns(c.MaxIdleConns)
	}
	if c.MaxOpenConns > 0 {
		db.SetMaxOpenConns(c.MaxOpenConns)
	}
	return db, err
}
func Open(c Config, retries ...time.Duration) (*sql.DB, error) {
	if c.Mock {
		return nil, nil
	}
	if len(retries) == 0 {
		return open(c)
	} else {
		db, er1 := open(c)
		if er1 == nil {
			return db, er1
		}
		i := 0
		err := Retry(retries, func() (err error) {
			i = i + 1
			db2, er2 := open(c)
			if er2 == nil {
				db = db2
			}
			return er2
		})
		if err != nil {
			log.Printf("Cannot conect to database: %s.", err.Error())
		}
		return db, err
	}
}
func BuildDataSourceName(c Config) string {
	if c.Driver == "postgres" {
		uri := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%d sslmode=disable", c.User, c.Database, c.Password, c.Host, c.Port)
		return uri
	} else if c.Driver == "mysql" {
		uri := ""
		if c.MultiStatements {
			uri = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local&multiStatements=True", c.User, c.Password, c.Host, c.Port, c.Database)
			return uri
		}
		uri = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local", c.User, c.Password, c.Host, c.Port, c.Database)
		return uri
	} else if c.Driver == "mssql" { // mssql
		uri := fmt.Sprintf("sqlserver://%s:%s@%s:%d?Database=%s", c.User, c.Password, c.Host, c.Port, c.Database)
		return uri
	} else if c.Driver == "godror" || c.Driver == "oracle" {
		return fmt.Sprintf("user=\"%s\" password=\"%s\" connectString=\"%s:%d/%s\"", c.User, c.Password, c.Host, c.Port, c.Database)
	} else { //sqlite
		return c.Host // return sql.Open("sqlite3", c.Host)
	}
}

// for Loader

func BuildFindById(db *sql.DB, table string, id interface{}, mapJsonColumnKeys map[string]string, keys []string, options ...func(i int) string) (string, []interface{}) {
	var where = ""
	var values []interface{}
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	if len(keys) == 1 {
		where = fmt.Sprintf("where %s = %s", mapJsonColumnKeys[keys[0]], buildParam(1))
		values = append(values, id)
	} else {
		conditions := make([]string, 0)
		if ids, ok := id.(map[string]interface{}); ok {
			j := 0
			for _, keyJson := range keys {
				columnName := mapJsonColumnKeys[keyJson]
				if idk, ok1 := ids[keyJson]; ok1 {
					conditions = append(conditions, fmt.Sprintf("%s = %s", columnName, buildParam(j)))
					values = append(values, idk)
					j++
				}
			}
			where = "where " + strings.Join(conditions, " and ")
		}
	}
	return fmt.Sprintf("select * from %v %v", table, where), values
}

func BuildSelectAllQuery(table string) string {
	return fmt.Sprintf("select * from %v", table)
}

func InitSingleResult(modelType reflect.Type) interface{} {
	return reflect.New(modelType).Interface()
}

func InitArrayResults(modelsType reflect.Type) interface{} {
	return reflect.New(modelsType).Interface()
}

func setValue(model interface{}, index int, value interface{}) (interface{}, error) {
	valueObject := reflect.Indirect(reflect.ValueOf(model))
	switch reflect.ValueOf(model).Kind() {
	case reflect.Ptr:
		{
			valueObject.Field(index).Set(reflect.ValueOf(value))
			return model, nil
		}
	default:
		if modelWithTypeValue, ok := model.(reflect.Value); ok {
			_, err := setValueWithTypeValue(modelWithTypeValue, index, value)
			return modelWithTypeValue.Interface(), err
		}
	}
	return model, nil
}
func setValueWithTypeValue(model reflect.Value, index int, value interface{}) (reflect.Value, error) {
	trueValue := reflect.Indirect(model)
	switch trueValue.Kind() {
	case reflect.Struct:
		{
			val := reflect.Indirect(reflect.ValueOf(value))
			if trueValue.Field(index).Kind() == val.Kind() {
				trueValue.Field(index).Set(reflect.ValueOf(value))
				return trueValue, nil
			} else {
				return trueValue, fmt.Errorf("value's kind must same as field's kind")
			}
		}
	default:
		return trueValue, nil
	}
}
func FindFieldIndex(modelType reflect.Type, fieldName string) int {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		if field.Name == fieldName {
			return i
		}
	}
	return -1
}

func Insert(ctx context.Context, db *sql.DB, table string, model interface{}, options ...func(i int) string) (int64, error) {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	queryInsert, values := BuildInsert(table, model, 0, buildParam)

	result, err := db.ExecContext(ctx, queryInsert, values...)
	if err != nil {
		if err != nil {
			return handleDuplicate(db, err)
		}
	}
	return result.RowsAffected()
}

func handleDuplicate(db *sql.DB, err error) (int64, error) {
	x := err.Error()
	driver := GetDriver(db)
	if driver == DriverPostgres && strings.Contains(x, "pq: duplicate key value violates unique constraint") {
		return 0, nil
	} else if driver == DriverMysql && strings.Contains(x, "Error 1062: Duplicate entry") {
		return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
	} else if driver == DriverOracle && strings.Contains(x, "ORA-00001: unique constraint") {
		return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
	} else if driver == DriverMssql && strings.Contains(x, "Violation of PRIMARY KEY constraint") {
		return 0, nil //Violation of PRIMARY KEY constraint 'PK_aa'. Cannot insert duplicate key in object 'dbo.aa'. The duplicate key value is (b, 2).
	} else if driver == DriverSqlite3 && strings.Contains(x, "UNIQUE constraint failed") {
		return 0, nil
	}
	return 0, err
}

func InsertTx(ctx context.Context, db *sql.DB, tx *sql.Tx, table string, model interface{}, options ...func(i int) string) (int64, error) {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	queryInsert, values := BuildInsert(table, model, 0, buildParam)
	result, err := tx.ExecContext(ctx, queryInsert, values...)
	if err != nil {
		return handleDuplicate(db, err)
	}
	return result.RowsAffected()
}

func InsertWithVersion(ctx context.Context, db *sql.DB, table string, model interface{}, versionIndex int, options ...func(i int) string) (int64, error) {
	if versionIndex < 0 {
		return 0, errors.New("version index not found")
	}
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	queryInsert, values := BuildInsertWithVersion(table, model, 0, versionIndex, buildParam)

	result, err := db.ExecContext(ctx, queryInsert, values...)
	if err != nil {
		errstr := err.Error()
		driver := GetDriver(db)
		if driver == DriverPostgres && strings.Contains(errstr, "pq: duplicate key value violates unique constraint") {
			return 0, nil
		} else if driver == DriverMysql && strings.Contains(errstr, "Error 1062: Duplicate entry") {
			return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
		} else if driver == DriverOracle && strings.Contains(errstr, "ORA-00001: unique constraint") {
			return 0, nil //mysql Error 1062: Duplicate entry 'a-1' for key 'PRIMARY'
		} else if driver == DriverMssql && strings.Contains(errstr, "Violation of PRIMARY KEY constraint") {
			return 0, nil //Violation of PRIMARY KEY constraint 'PK_aa'. Cannot insert duplicate key in object 'dbo.aa'. The duplicate key value is (b, 2).
		} else if driver == DriverSqlite3 && strings.Contains(errstr, "UNIQUE constraint failed") {
			return 0, nil
		} else {
			return 0, err
		}
	}
	return result.RowsAffected()
}

func Exec(ctx context.Context, stmt *sql.Stmt, values ...interface{}) (int64, error) {
	result, err := stmt.ExecContext(ctx, values...)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func Update(ctx context.Context, db *sql.DB, table string, model interface{}, options ...func(i int) string) (int64, error) {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	query, values := BuildUpdate(table, model, 0, buildParam)
	r, err0 := db.ExecContext(ctx, query, values...)
	if err0 != nil {
		return -1, err0
	}
	return r.RowsAffected()
}

func UpdateTx(ctx context.Context, db *sql.DB, tx *sql.Tx, table string, model interface{}, options ...func(i int) string) (int64, error) {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	query, values := BuildUpdate(table, model, 0, buildParam)
	r, err0 := tx.ExecContext(ctx, query, values...)
	if err0 != nil {
		return -1, err0
	}
	return r.RowsAffected()
}

func UpdateWithVersion(ctx context.Context, db *sql.DB, table string, model interface{}, versionIndex int, options ...func(i int) string) (int64, error) {
	if versionIndex < 0 {
		return 0, errors.New("version's index not found")
	}

	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	query, values := BuildUpdateWithVersion(table, model, 0, versionIndex, buildParam)

	result, err := db.ExecContext(ctx, query, values...)

	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func Patch(ctx context.Context, db *sql.DB, table string, model map[string]interface{}, modelType reflect.Type, options ...func(i int) string) (int64, error) {
	idcolumNames, idJsonName := FindPrimaryKeys(modelType)
	columNames := FindJsonName(modelType)
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	query, value := BuildPatch(table, model, columNames, idJsonName, idcolumNames, buildParam)
	if query == "" {
		return 0, errors.New("fail to build query")
	}
	result, err := db.ExecContext(ctx, query, value...)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func PatchWithVersion(ctx context.Context, db *sql.DB, table string, model map[string]interface{}, modelType reflect.Type, versionIndex int, options ...func(i int) string) (int64, error) {
	if versionIndex < 0 {
		return 0, errors.New("version's index not found")
	}

	idcolumNames, idJsonName := FindPrimaryKeys(modelType)
	columNames := FindJsonName(modelType)
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	versionJsonName, ok := GetJsonNameByIndex(modelType, versionIndex)
	if !ok {
		return 0, errors.New("version's json not found")
	}
	versionColName, ok := GetColumnNameByIndex(modelType, versionIndex)
	if !ok {
		return 0, errors.New("version's column not found")
	}

	query, value := BuildPatchWithVersion(table, model, columNames, idJsonName, idcolumNames, buildParam, versionIndex, versionJsonName, versionColName)
	if query == "" {
		return 0, errors.New("fail to build query")
	}
	result, err := db.ExecContext(ctx, query, value...)
	if err != nil {
		return -1, err
	}
	return result.RowsAffected()
}

func Delete(ctx context.Context, db *sql.DB, table string, query map[string]interface{}, options ...func(i int) string) (int64, error) {
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	sql, values := BuildDelete(table, query, buildParam)

	result, err := db.ExecContext(ctx, sql, values...)

	if err != nil {
		return -1, err
	}
	return BuildResult(result.RowsAffected())
}

func GetFieldByJson(modelType reflect.Type, jsonName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		tag1, ok1 := field.Tag.Lookup("json")
		if ok1 && strings.Split(tag1, ",")[0] == jsonName {
			if tag2, ok2 := field.Tag.Lookup("gorm"); ok2 {
				if has := strings.Contains(tag2, "column"); has {
					str1 := strings.Split(tag2, ";")
					num := len(str1)
					for k := 0; k < num; k++ {
						str2 := strings.Split(str1[k], ":")
						for j := 0; j < len(str2); j++ {
							if str2[j] == "column" {
								return i, field.Name, str2[j+1]
							}
						}
					}
				}
			}
			return i, field.Name, ""
		}
	}
	return -1, jsonName, jsonName
}

func BuildUpdate(table string, model interface{}, i int, buildParam func(int) string) (string, []interface{}) {
	mapData, mapKey, columns, keys := BuildMapDataAndKeys(model, true)
	var values []interface{}
	colSet := make([]string, 0)
	colQuery := make([]string, 0)
	colNumber := 1
	for _, colName := range columns {
		if v1, ok := mapData[colName]; ok {
			v3, ok3 := GetDBValue(v1)
			if ok3 {
				colSet = append(colSet, QuoteColumnName(colName)+"="+v3)
			} else {
				values = append(values, v1)
				colSet = append(colSet, QuoteColumnName(colName)+"="+buildParam(colNumber+i))
				colNumber++
			}
		} else {
			colSet = append(colSet, BuildParamWithNull(colName))
		}
	}
	for _, colName := range keys {
		if v2, ok := mapKey[colName]; ok {
			v3, ok3 := GetDBValue(v2)
			if ok3 {
				colQuery = append(colQuery, QuoteColumnName(colName) + "=" + v3)
			} else {
				values = append(values, v2)
				colQuery = append(colQuery, QuoteColumnName(colName)+"="+buildParam(colNumber+i))
			}
			colNumber++
		}
	}
	queryWhere := strings.Join(colQuery, " and ")
	querySet := strings.Join(colSet, ",")
	query := fmt.Sprintf("update %v set %v where %v", table, querySet, queryWhere)
	return query, values
}
func GetDBValue(v interface{}) (string, bool) {
	switch v.(type) {
	case string:
		s0 := v.(string)
		if len(s0) == 0 {
			return "''", true
		}
		return "", false
	case int:
		return strconv.Itoa(v.(int)), true
	case int64:
		return strconv.FormatInt(v.(int64), 10), true
	case int32:
		return strconv.FormatInt(int64(v.(int32)), 10), true
	default:
		return "", false
	}
}
func BuildUpdateWithVersion(table string, model interface{}, i int, versionIndex int, buildParam func(int) string) (string, []interface{}) {
	if versionIndex < 0 {
		panic("version's index not found")
	}
	valueOfModel := reflect.Indirect(reflect.ValueOf(model))
	currentVersion := reflect.Indirect(valueOfModel.Field(versionIndex)).Int()
	nextVersion := currentVersion + 1
	_, err := setValue(model, versionIndex, &nextVersion)
	if err != nil {
		panic(err)
	}

	mapData, mapKey, columns, keys := BuildMapDataAndKeys(model, true)
	versionColName, exist := GetColumnNameByIndex(valueOfModel.Type(), versionIndex)
	if !exist {
		panic("version's column not found")
	}
	mapKey[versionColName] = currentVersion

	var values []interface{}
	colSet := make([]string, 0)
	colQuery := make([]string, 0)
	colNumber := 1
	for _, colName := range columns {
		if v1, ok := mapData[colName]; ok {
			v3, ok3 := GetDBValue(v1)
			if ok3 {
				colSet = append(colSet, fmt.Sprintf("%v = "+v3, colName))
			} else {
				values = append(values, v1)
				colQuery = append(colQuery, QuoteColumnName(colName) + "=" + buildParam(colNumber+i))
				colNumber++
			}
		} else {
			colSet = append(colSet, BuildParamWithNull(colName))
		}
	}
	for _, colName := range keys {
		if v2, ok := mapKey[colName]; ok {
			v3, ok3 := GetDBValue(v2)
			if ok3 {
				colQuery = append(colQuery, QuoteColumnName(colName) + "=" + v3)
			} else {
				values = append(values, v2)
				colQuery = append(colQuery, QuoteColumnName(colName) + "=" + buildParam(colNumber+i))
			}
			colNumber++
		}
	}
	queryWhere := strings.Join(colQuery, " and ")
	querySet := strings.Join(colSet, ",")
	query := fmt.Sprintf("update %v set %v where %v", table, querySet, queryWhere)
	return query, values
}

func BuildPatch(table string, model map[string]interface{}, mapJsonColum map[string]string, idTagJsonNames []string, idColumNames []string, buildParam func(int) string) (string, []interface{}) {
	scope := statement()
	// Append variables set column
	for key, _ := range model {
		if _, ok := Find(idTagJsonNames, key); !ok {
			if colName, ok2 := mapJsonColum[key]; ok2 {
				scope.Columns = append(scope.Columns, colName)
				scope.Values = append(scope.Values, model[key])
			}
		}
	}
	// Append variables where
	for i, key := range idTagJsonNames {
		scope.Values = append(scope.Values, model[key])
		scope.Keys = append(scope.Keys, idColumNames[i])
	}
	var value []interface{}

	n := len(scope.Columns)
	sets, val1, err1 := BuildSqlParametersAndValues(scope.Columns, scope.Values, &n, 0, ", ", buildParam)
	if err1 != nil {
		return "", nil
	}
	value = append(value, val1...)
	columnsKeys := len(scope.Keys)
	where, val2, err2 := BuildSqlParametersAndValues(scope.Keys, scope.Values, &columnsKeys, n, " and ", buildParam)
	if err2 != nil {
		return "", nil
	}
	value = append(value, val2...)
	query := fmt.Sprintf("update %s set %s where %s",
		table,
		sets,
		where,
	)
	return query, value
}

func BuildPatchWithVersion(table string, model map[string]interface{}, mapJsonColum map[string]string, idTagJsonNames []string, idColumNames []string, buildParam func(int) string, versionIndex int, versionJsonName, versionColName string) (string, []interface{}) {
	if versionIndex < 0 {
		panic("version's index not found")
	}

	currentVersion, ok := model[versionJsonName]
	if !ok {
		panic("version field not found")
	}
	nextVersion := currentVersion.(int64) + 1
	model[versionJsonName] = nextVersion

	scope := statement()
	var value []interface{}
	// Append variables set column
	for key, _ := range model {
		if _, ok := Find(idTagJsonNames, key); !ok {
			if columName, ok2 := mapJsonColum[key]; ok2 {
				scope.Columns = append(scope.Columns, columName)
				scope.Values = append(scope.Values, model[key])
			}
		}
	}
	// Append variables where
	for i, key := range idTagJsonNames {
		scope.Values = append(scope.Values, model[key])
		scope.Keys = append(scope.Keys, idColumNames[i])
	}
	scope.Values = append(scope.Values, currentVersion)
	scope.Keys = append(scope.Keys, versionColName)

	n := len(scope.Columns)
	sets, setVal, err1 := BuildSqlParametersAndValues(scope.Columns, scope.Values, &n, 0, ", ", buildParam)
	if err1 != nil {
		return "", nil
	}
	value = append(value, setVal...)
	numKeys := len(scope.Keys)
	where, whereVal, err2 := BuildSqlParametersAndValues(scope.Keys, scope.Values, &numKeys, n, " and ", buildParam)
	if err2 != nil {
		return "", nil
	}
	value = append(value, whereVal...)
	query := fmt.Sprintf("update %s set %s where %s",
		table,
		sets,
		where,
	)
	return query, value
}

func BuildDelete(table string, ids map[string]interface{}, buildParam func(int) string) (string, []interface{}) {
	var values []interface{}
	var queryArr []string
	i := 1
	for key, value := range ids {
		queryArr = append(queryArr, fmt.Sprintf("%v = %v", QuoteColumnName(key), buildParam(i)))
		values = append(values, value)
		i++
	}
	q := strings.Join(queryArr, " and ")
	return fmt.Sprintf("delete from %v where %v", table, q), values
}

func ExtractBySchema(value interface{}, columns []string, schema map[string]FieldDB) (map[string]interface{}, map[string]interface{}, map[string]interface{}, error) {
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
		value = rv.Interface()
	}
	if rv.Kind() != reflect.Struct {
		return nil, nil, nil, errors.New("value must be kind of Struct")
	}

	var attrs = map[string]interface{}{}
	var nAttrs = map[string]interface{}{}
	var attrsKey = map[string]interface{}{}

	for _, col := range columns {
		fdb, ok := schema[col]
		if ok {
			f := rv.Field(fdb.index)
			fieldValue := f.Interface()
			isNil := false
			if f.Kind() == reflect.Ptr {
				if reflect.ValueOf(fieldValue).IsNil() {
					isNil = true
				} else {
					fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
				}
			}
			if !fdb.key {
				if !isNil {
					if boolValue, ok := fieldValue.(bool); ok {
						if boolValue {
							attrs[col] = fdb.true
							nAttrs[col] = fdb.true
						} else {
							attrs[col] = fdb.false
							nAttrs[col] = fdb.false
						}
					} else {
						attrs[col] = fieldValue
						nAttrs[col] = fieldValue
					}
				} else {
					attrs[col] = fieldValue
				}
			} else {
				attrsKey[col] = fieldValue
				if !isNil {
					nAttrs[col] = fieldValue
				}
			}
		}
	}
	return attrs, attrsKey, nAttrs, nil
}
// Obtain columns and values required for insert from interface
func ExtractMapValue(value interface{}, excludeColumns *[]string, ignoreNull bool) (map[string]interface{}, map[string]interface{}, map[string]interface{}, error) {
	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
		value = rv.Interface()
	}
	if rv.Kind() != reflect.Struct {
		return nil, nil, nil, errors.New("value must be kind of Struct")
	}

	var attrs = map[string]interface{}{}
	var nAttrs = map[string]interface{}{}
	var attrsKey = map[string]interface{}{}

	for index, field := range GetMapField(value) {
		if GetTag(field, IgnoreReadWrite) == IgnoreReadWrite {
			continue
		}
		kind := field.Value.Kind()
		fieldValue := field.Value.Interface()
		isNil := false
		if kind == reflect.Ptr {
			if reflect.ValueOf(fieldValue).IsNil() {
				if ignoreNull {
					*excludeColumns = append(*excludeColumns, field.Tags["fieldName"])
				}
				isNil = true
			} else {
				fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
			}
		}
		if !ContainString(*excludeColumns, GetTag(field, "fieldName")) && !IsPrimary(field) {
			if dBName, ok := field.Tags[DBName]; ok {
				if !isNil {
					if boolValue, ok := fieldValue.(bool); ok {
						bv := field.Type.Field(index).Tag.Get(strconv.FormatBool(boolValue))
						attrs[dBName] = bv
						nAttrs[dBName] = bv
					} else {
						attrs[dBName] = fieldValue
						nAttrs[dBName] = fieldValue
					}
				} else {
					attrs[dBName] = fieldValue
				}

			}
		}
		if IsPrimary(field) {
			if dBName, ok := field.Tags[DBName]; ok {
				attrsKey[dBName] = fieldValue
				if !isNil {
					nAttrs[dBName] = fieldValue
				}
			}
		}
	}
	return attrs, attrsKey, nAttrs, nil
}

func GetIndexByTag(tag, key string, modelType reflect.Type) (index int) {
	for i := 0; i < modelType.NumField(); i++ {
		f := modelType.Field(i)
		v := strings.Split(f.Tag.Get(tag), ",")[0]
		if v == key {
			return i
		}
	}
	return -1
}

// For ViewDefaultRepository
func GetColumnName(modelType reflect.Type, jsonName string) (col string, colExist bool) {
	index := GetIndexByTag("json", jsonName, modelType)
	if index == -1 {
		return jsonName, false
	}
	field := modelType.Field(index)
	ormTag, ok2 := field.Tag.Lookup("gorm")
	if !ok2 {
		return "", true
	}
	if has := strings.Contains(ormTag, "column"); has {
		str1 := strings.Split(ormTag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == "column" {
					return str2[j+1], true
				}
			}
		}
	}
	return jsonName, false
}

func GetColumnNameByIndex(ModelType reflect.Type, index int) (col string, colExist bool) {
	fields := ModelType.Field(index)
	tag, _ := fields.Tag.Lookup("gorm")

	if has := strings.Contains(tag, "column"); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == "column" {
					return str2[j+1], true
				}
			}
		}
	}
	return "", false
}

func GetJsonNameByIndex(ModelType reflect.Type, index int) (string, bool) {
	field := ModelType.Field(index)
	if tagJson, ok := field.Tag.Lookup("json"); ok {
		arrValue := strings.Split(tagJson, ",")
		if len(arrValue) > 0 {
			return arrValue[0], true
		}
	}

	return "", false
}

func FindFieldByName(modelType reflect.Type, fieldName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		if field.Name == fieldName {
			name1 := fieldName
			name2 := fieldName
			tag1, ok1 := field.Tag.Lookup("json")
			tag2, ok2 := field.Tag.Lookup("gorm")
			if ok1 {
				name1 = strings.Split(tag1, ",")[0]
			}
			if ok2 {
				if has := strings.Contains(tag2, "column"); has {
					str1 := strings.Split(tag2, ";")
					num := len(str1)
					for k := 0; k < num; k++ {
						str2 := strings.Split(str1[k], ":")
						for j := 0; j < len(str2); j++ {
							if str2[j] == "column" {
								return i, name1, str2[j+1]
							}
						}
					}
				}
			}
			return i, name1, name2
		}
	}
	return -1, fieldName, fieldName
}

func FindIdFields(modelType reflect.Type) []string {
	numField := modelType.NumField()
	var idFields []string
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				idFields = append(idFields, field.Name)
			}
		}
	}
	return idFields
}

func FindIdColumns(modelType reflect.Type) []string {
	numField := modelType.NumField()
	var idFields = make([]string, 0)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				if has := strings.Contains(ormTag, "column"); has {
					str1 := strings.Split(ormTag, ";")
					num := len(str1)
					for i := 0; i < num; i++ {
						str2 := strings.Split(str1[i], ":")
						for j := 0; j < len(str2); j++ {
							if str2[j] == "column" {
								idFields = append(idFields, str2[j+1])
							}
						}
					}
				}
			}
		}
	}
	return idFields
}

func BuildQueryById(id interface{}, modelType reflect.Type, idName string) (query map[string]interface{}) {
	columnName, _ := GetColumnName(modelType, idName)
	return map[string]interface{}{columnName: id}
}

func MapToGORM(ids map[string]interface{}, modelType reflect.Type) (query map[string]interface{}) {
	queryGen := make(map[string]interface{})
	var columnName string
	for colName, value := range ids {
		columnName, _ = GetColumnName(modelType, colName)
		queryGen[columnName] = value
	}
	return queryGen
}

// For DefaultGenericService
func BuildQueryByIdFromObject(object interface{}, modelType reflect.Type, idNames []string) (query map[string]interface{}) {
	queryGen := make(map[string]interface{})
	var value interface{}
	for _, colId := range idNames {
		value = reflect.Indirect(reflect.ValueOf(object)).FieldByName(colId).Interface()
		queryGen[colId] = value
	}
	return MapToGORM(queryGen, modelType)
}

func BuildQueryByIdFromMap(object map[string]interface{}, modelType reflect.Type, idNames []string) (query map[string]interface{}) {
	queryGen := make(map[string]interface{})
	//var value interface{}
	for _, colId := range idNames {
		queryGen[colId] = object[colId]
	}
	return MapToGORM(queryGen, modelType)
}

// For Search
func GetSqlBuilderTags(modelType reflect.Type) []QueryType {
	numField := modelType.NumField()
	//queries := make([]QueryType, 0)
	var sqlQueries []QueryType
	for i := 0; i < numField; i++ {
		sqlQuery := QueryType{}
		field := modelType.Field(i)
		sqlTags := field.Tag.Get("sql_builder")
		tags := strings.Split(sqlTags, ";")
		for _, tag := range tags {
			key := strings.Split(tag, ":")
			switch key[0] {
			case "join":
				sqlQuery.Join = key[1]
				break
			case "select":
				sqlQuery.Select = key[1]
				break
			case "select_count":
				sqlQuery.SelectCount = key[1]
			}
		}
		if sqlQuery.Select != "" || sqlQuery.Join != "" || sqlQuery.SelectCount != "" {
			sqlQueries = append(sqlQueries, sqlQuery)
		}
	}
	return sqlQueries
}

func MapColumnToJson(query map[string]interface{}) interface{} {
	result := make(map[string]interface{})
	for k, v := range query {
		dem := strings.Count(k, "_")
		for i := 0; i < dem; i++ {
			if strings.Index(k, "_") > -1 {
				hoa := []rune(strings.ToUpper(string(k[strings.Index(k, "_")+1])))
				k = ReplaceAtIndex(k, hoa[0], strings.Index(k, "_")+1)
				k = strings.Replace(k, "_", "", 1)
			}
		}
		result[k] = v
	}
	return result
}
func ReplaceAtIndex(str string, replacement rune, index int) string {
	out := []rune(str)
	out[index] = replacement
	return string(out)
}

func GetTableName(object interface{}) string {
	vo := reflect.Indirect(reflect.ValueOf(object))
	tableName := vo.MethodByName("TableName").Call([]reflect.Value{})
	return tableName[0].String()
}
func BuildParametersFrom(i int, numCol int, buildParam func(int) string) string {
	var arrValue []string
	for j := 0; j < numCol; j++ {
		arrValue = append(arrValue, buildParam(i+j+1))
	}
	return strings.Join(arrValue, ",")
}
func EscapeString(value string) string {
	//replace := map[string]string{"'": `\'`, "\\0": "\\\\0", "\n": "\\n", "\r": "\\r", `"`: `\"`, "\x1a": "\\Z"}
	//if strings.Contains(value, `\\`) {
	//	value = strings.Replace(value, "\\", "\\\\", -1)
	//}
	//for b, a := range replace {
	//	if strings.Contains(value, b) {
	//		value = strings.Replace(value, b, a, -1)
	//	}
	//}
	return strings.NewReplacer("\\", "\\\\", "'", `\'`, "\\0", "\\\\0", "\n", "\\n", "\r", "\\r", `"`, `\"`, "\x1a", "\\Z" /*We have more here*/).Replace(value)
}

func EscapeStringForSelect(value string) string {
	//replace := map[string]string{"'": `''`, "\\0": "\\\\0", "\n": "\\n", "\r": "\\r", `"`: `\"`, "\x1a": "\\Z"}
	//if strings.Contains(value, `\\`) {
	//	value = strings.Replace(value, "\\", "\\\\", -1)
	//}
	//
	//for b, a := range replace {
	//	if strings.Contains(value, b) {
	//		value = strings.Replace(value, b, a, -1)
	//	}
	//}
	return strings.NewReplacer("'", `''` /*We have more here*/).Replace(value)
}

// Check if string value is contained in slice
func ContainString(s []string, value string) bool {
	for _, v := range s {
		if v == value {
			return true
		}
	}
	return false
}

// Enable map keys to be retrieved in same order when iterating
func SortedKeys(val map[string]interface{}) []string {
	var keys []string
	for key := range val {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func GetTag(field Field, tagName string) string {
	if tag, ok := field.Tags[tagName]; ok {
		return tag
	}
	return ""
}

func IsPrimary(field Field) bool {
	return GetTag(field, PrimaryKey) != ""
}
func ReplaceQueryArgs(driver string, query string) string {
	if driver == DriverOracle || driver == DriverPostgres || driver == DriverSqlite3 {
		var x string
		if driver == DriverOracle {
			x = ":val"
		} else {
			x = "$"
		}
		i := 1
		k := strings.Index(query, "?")
		if k >= 0 {
			for {
				query = strings.Replace(query, "?", x+fmt.Sprintf("%v", i), 1)
				i = i + 1
				k := strings.Index(query, "?")
				if k < 0 {
					return query
				}
			}
		}
	}
	return query
}
func Exist(ctx context.Context, db *sql.DB, sql string, args ...interface{}) (bool, error) {
	rows, err := db.QueryContext(ctx, sql, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		return true, nil
	}
	return false, nil
}
func MapModels(ctx context.Context, models interface{}, mp func(context.Context, interface{}) (interface{}, error)) (interface{}, error) {
	vo := reflect.Indirect(reflect.ValueOf(models))
	if vo.Kind() == reflect.Ptr {
		vo = reflect.Indirect(vo)
	}
	if vo.Kind() == reflect.Slice {
		le := vo.Len()
		for i := 0; i < le; i++ {
			x := vo.Index(i)
			k := x.Kind()
			if k == reflect.Struct {
				y := x.Addr().Interface()
				mp(ctx, y)
			} else {
				y := x.Interface()
				mp(ctx, y)
			}

		}
	}
	return models, nil
}
func BuildPlaceHolders(n int, buildParam func(int) string) string {
	ss := make([]string, 0)
	for i := 1; i <= n; i++ {
		s := buildParam(i)
		ss = append(ss, s)
	}
	return strings.Join(ss, ",")
}
func BuildParam(i int) string {
	return "?"
}
func BuildOracleParam(i int) string {
	return ":val" + strconv.Itoa(i)
}
func BuildMsSqlParam(i int) string {
	return "@p" + strconv.Itoa(i)
}
func BuildDollarParam(i int) string {
	return "$" + strconv.Itoa(i)
}
func GetBuild(db *sql.DB) func(i int) string {
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return BuildDollarParam
	case "*godror.drv":
		return BuildOracleParam
	case "*mssql.Driver":
		return BuildMsSqlParam
	default:
		return BuildParam
	}
}

func MapToDB(model *map[string]interface{}, modelType reflect.Type) {
	for colName, value := range *model {
		if boolValue, boolOk := value.(bool); boolOk {
			index := GetIndexByTag("json", colName, modelType)
			if index > -1 {
				valueS := modelType.Field(index).Tag.Get(strconv.FormatBool(boolValue))
				valueInt, err := strconv.Atoi(valueS)
				if err != nil {
					(*model)[colName] = valueS
				} else {
					(*model)[colName] = valueInt
				}
				continue
			}
		}
		(*model)[colName] = value
	}
}

func StructScan(s interface{}, columns []string, fieldsIndex map[string]int, indexIgnore int) (r []interface{}, swapValues map[int]interface{}) {
	if s != nil {
		modelType := reflect.TypeOf(s).Elem()
		swapValues = make(map[int]interface{}, 0)
		maps := reflect.Indirect(reflect.ValueOf(s))

		if columns == nil {
			for i := 0; i < maps.NumField(); i++ {
				tagBool := modelType.Field(i).Tag.Get("true")
				if tagBool == "" {
					r = append(r, maps.Field(i).Addr().Interface())
				} else {
					var str string
					swapValues[i] = reflect.New(reflect.TypeOf(str)).Elem().Addr().Interface()
					r = append(r, swapValues[i])
				}
			}
			return
		}

		for i, columnsName := range columns {
			if i == indexIgnore {
				continue
			}
			var index int
			var ok bool
			var modelField reflect.StructField
			var valueField reflect.Value
			if fieldsIndex == nil {
				if modelField, ok = modelType.FieldByName(columnsName); !ok {
					var t interface{}
					r = append(r, &t)
					continue
				}
				valueField = maps.FieldByName(columnsName)
			} else {
				if index, ok = fieldsIndex[columnsName]; !ok {
					var t interface{}
					r = append(r, &t)
					continue
				}
				modelField = modelType.Field(index)
				valueField = maps.Field(index)
			}
			tagBool := modelField.Tag.Get("true")
			if tagBool == "" {
				r = append(r, valueField.Addr().Interface())
			} else {
				var str string
				swapValues[index] = reflect.New(reflect.TypeOf(str)).Elem().Addr().Interface()
				r = append(r, swapValues[index])
			}

		}
	}
	return
}

func SwapValuesToBool(s interface{}, swap *map[int]interface{}) {
	if s != nil {
		modelType := reflect.TypeOf(s).Elem()
		maps := reflect.Indirect(reflect.ValueOf(s))
		for index, element := range *swap {
			var isBool bool
			boolStr := modelType.Field(index).Tag.Get("true")
			var dbValue = element.(*string)
			isBool = *dbValue == boolStr
			if maps.Field(index).Kind() == reflect.Ptr {
				maps.Field(index).Set(reflect.ValueOf(&isBool))
			} else {
				maps.Field(index).SetBool(isBool)
			}
		}
	}
}
