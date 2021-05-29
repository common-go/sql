package sql

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

func GetDriver(db *sql.DB) string {
	if db == nil {
		return DriverNotSupport
	}
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return DriverPostgres
	case "*godror.drv":
		return DriverOracle
	case "*mysql.MySQLDriver":
		return DriverMysql
	case "*mssql.Driver":
		return DriverMssql
	case "*sqlite3.SQLiteDriver":
		return DriverSqlite3
	default:
		return DriverNotSupport
	}
}
func Count(ctx context.Context, db *sql.DB, sql string, values ...interface{}) (int64, error) {
	var total int64
	row := db.QueryRowContext(ctx, sql, values...)
	err2 := row.Scan(&total)
	if err2 != nil {
		return total, err2
	}
	return total, nil
}
func Query(ctx context.Context, db *sql.DB, results interface{}, sql string, values ...interface{}) error {
	rows, er1 := db.QueryContext(ctx, sql, values...)
	if er1 != nil {
		return er1
	}
	defer rows.Close()
	modelType := reflect.TypeOf(results).Elem().Elem()

	fieldsIndex, er2 := GetColumnIndexes(modelType)
	if er2 != nil {
		return er2
	}

	tb, er3 := Scans(rows, modelType, fieldsIndex)
	if er3 != nil {
		return er3
	}
	for _, element := range tb {
		appendToArray(results, element)
	}
	er4 := rows.Close()
	if er4 != nil {
		return er4
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if er5 := rows.Err(); er5 != nil {
		return er5
	}
	return nil
}
func QueryTx(ctx context.Context, tx *sql.Tx, results interface{}, sql string, values ...interface{}) error {
	rows, er1 := tx.QueryContext(ctx, sql, values...)
	if er1 != nil {
		return er1
	}
	defer rows.Close()

	modelType := reflect.TypeOf(results).Elem().Elem()
	fieldsIndex, er2 := GetColumnIndexes(modelType)
	if er2 != nil {
		return er2
	}

	tb, er3 := Scans(rows, modelType, fieldsIndex)
	if er3 != nil {
		return er3
	}
	for _, element := range tb {
		appendToArray(results, element)
	}
	er4 := rows.Close()
	if er4 != nil {
		return er4
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if er5 := rows.Err(); er5 != nil {
		return er5
	}
	return nil
}
func QueryByStatement(ctx context.Context, stm *sql.Stmt, results interface{}, values ...interface{}) error {
	rows, er1 := stm.QueryContext(ctx, values...)
	if er1 != nil {
		return er1
	}
	defer rows.Close()

	modelType := reflect.TypeOf(results).Elem().Elem()
	fieldsIndex, er2 := GetColumnIndexes(modelType)
	if er2 != nil {
		return er2
	}

	tb, er3 := Scans(rows, modelType, fieldsIndex)
	if er3 != nil {
		return er3
	}
	for _, element := range tb {
		appendToArray(results, element)
	}
	er4 := rows.Close()
	if er4 != nil {
		return er4
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if er5 := rows.Err(); er5 != nil {
		return er5
	}
	return nil
}
func QueryAndCount(ctx context.Context, db *sql.DB, results interface{}, count *int64, sql string, values ...interface{}) error {
	rows, er1 := db.QueryContext(ctx, sql, values...)
	if er1 != nil {
		return er1
	}
	defer rows.Close()
	modelType := reflect.TypeOf(results).Elem().Elem()

	fieldsIndex, er2 := GetColumnIndexes(modelType)
	if er2 != nil {
		return er2
	}

	tb, c, er3 := ScansAndCount(rows, modelType, fieldsIndex)
	*count = c
	if er3 != nil {
		return er3
	}
	for _, element := range tb {
		appendToArray(results, element)
	}
	er4 := rows.Close()
	if er4 != nil {
		return er4
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if er5 := rows.Err(); er5 != nil {
		return er5
	}
	return nil
}
func QueryRow(ctx context.Context, db *sql.DB, modelType reflect.Type, fieldsIndex map[string]int, sql string, values ...interface{}) (interface{}, error) {
	strSQL := "limit 1"
	driver := GetDriver(db)
	if driver == DriverOracle {
		strSQL = "AND ROWNUM = 1"
	}
	s := sql + " " + strSQL
	rows, er1 := db.QueryContext(ctx, s, values...)
	if er1 != nil {
		return nil, er1
	}
	tb, er2 := Scan(rows, modelType, fieldsIndex)
	if er2 != nil {
		return nil, er2
	}
	er3 := rows.Close()
	if er3 != nil {
		return nil, er3
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if er4 := rows.Err(); er4 != nil {
		return nil, er3
	}
	return tb, nil
}
func QueryRowTx(ctx context.Context, tx *sql.Tx, modelType reflect.Type, fieldsIndex map[string]int, sql string, values ...interface{}) (interface{}, error) {
	rows, er1 := tx.QueryContext(ctx, sql, values...)
	if er1 != nil {
		return nil, er1
	}
	tb, er2 := Scan(rows, modelType, fieldsIndex)
	if er2 != nil {
		return nil, er2
	}
	er3 := rows.Close()
	if er3 != nil {
		return nil, er3
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if er4 := rows.Err(); er4 != nil {
		return nil, er3
	}
	return tb, nil
}
func QueryRowByStatement(ctx context.Context, stm *sql.Stmt, modelType reflect.Type, fieldsIndex map[string]int, values ...interface{}) (interface{}, error) {
	rows, er1 := stm.QueryContext(ctx, values...)
	// rows, er1 := db.Query(s, values...)
	if er1 != nil {
		return nil, er1
	}
	tb, er2 := Scan(rows, modelType, fieldsIndex)
	if er2 != nil {
		return nil, er2
	}
	er3 := rows.Close()
	if er3 != nil {
		return nil, er3
	}
	// Rows.Err will report the last error encountered by Rows.Scan.
	if er4 := rows.Err(); er4 != nil {
		return nil, er3
	}
	return tb, nil
}
func appendToArray(arr interface{}, item interface{}) interface{} {
	arrValue := reflect.ValueOf(arr)
	elemValue := reflect.Indirect(arrValue)

	itemValue := reflect.ValueOf(item)
	if itemValue.Kind() == reflect.Ptr {
		itemValue = reflect.Indirect(itemValue)
	}
	elemValue.Set(reflect.Append(elemValue, itemValue))
	return arr
}
func GetColumnIndexes(modelType reflect.Type) (map[string]int, error) {
	ma := make(map[string]int, 0)
	if modelType.Kind() != reflect.Struct {
		return ma, errors.New("bad type")
	}
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		column, ok := FindTag(ormTag, "column")
		column = strings.ToLower(column)
		if ok {
			ma[column] = i
		}
	}
	return ma, nil
}

func GetIndexesByTagJson(modelType reflect.Type) (map[string]int, error) {
	ma := make(map[string]int, 0)
	if modelType.Kind() != reflect.Struct {
		return ma, errors.New("bad type")
	}
	for i := 0; i < modelType.NumField(); i++ {
		field := modelType.Field(i)
		tagJson := field.Tag.Get("json")
		if len(tagJson) > 0 {
			ma[tagJson] = i
		}
	}
	return ma, nil
}

func FindTag(tag string, key string) (string, bool) {
	if has := strings.Contains(tag, key); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == key {
					return str2[j+1], true
				}
			}
		}
	}
	return "", false
}

func GetColumnsSelect(modelType reflect.Type) []string {
	numField := modelType.NumField()
	columnNameKeys := make([]string, 0)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		if has := strings.Contains(ormTag, "column"); has {
			str1 := strings.Split(ormTag, ";")
			num := len(str1)
			for i := 0; i < num; i++ {
				str2 := strings.Split(str1[i], ":")
				for j := 0; j < len(str2); j++ {
					if str2[j] == "column" {
						columnName := str2[j+1]
						columnNameKeys = append(columnNameKeys, columnName)
					}
				}
			}
		}
	}
	return columnNameKeys
}
func GetColumnNameForSearch(modelType reflect.Type, sortField string) string {
	sortField = strings.TrimSpace(sortField)
	i, _, column := GetFieldByJson(modelType, sortField)
	if i > -1 {
		return column
	}
	return sortField // injection
}
func GetSortType(sortType string) string {
	if sortType == "-" {
		return desc
	} else {
		return asc
	}
}
func GetColumns(cols []string, err error) ([]string, error) {
	if cols == nil || err != nil {
		return cols, err
	}
	c2 := make([]string, 0)
	for _, c := range cols {
		s := strings.ToLower(c)
		c2 = append(c2, s)
	}
	return c2, nil
}
func Scans(rows *sql.Rows, modelType reflect.Type, fieldsIndex map[string]int) (t []interface{}, err error) {
	columns, er0 := GetColumns(rows.Columns())
	if er0 != nil {
		return nil, er0
	}
	for rows.Next() {
		initModel := reflect.New(modelType).Interface()
		r, swapValues := StructScan(initModel, columns, fieldsIndex, -1)
		if err = rows.Scan(r...); err == nil {
			SwapValuesToBool(initModel, &swapValues)
			t = append(t, initModel)
		}
	}
	return
}

func ScansAndCount(rows *sql.Rows, modelType reflect.Type, fieldsIndex map[string]int) ([]interface{}, int64, error) {
	var t []interface{}
	columns, er0 := GetColumns(rows.Columns())
	if er0 != nil {
		return nil, 0, er0
	}
	var count int64
	for rows.Next() {
		initModel := reflect.New(modelType).Interface()
		var c []interface{}
		c = append(c, &count)
		r, swapValues := StructScan(initModel, columns, fieldsIndex, 0)
		c = append(c, r...)
		if err := rows.Scan(c...); err == nil {
			SwapValuesToBool(initModel, &swapValues)
			t = append(t, initModel)
		}
	}
	return t, count, nil
}

func ScanByModelType(rows *sql.Rows, modelType reflect.Type) (t []interface{}, err error) {
	for rows.Next() {
		gTb := reflect.New(modelType).Interface()
		r, swapValues := StructScan(gTb, nil, nil, -1)
		if err = rows.Scan(r...); err == nil {
			SwapValuesToBool(gTb, &swapValues)
			t = append(t, gTb)
		}
	}

	return
}

func Scan(rows *sql.Rows, structType reflect.Type, fieldsIndex map[string]int) (t interface{}, err error) {
	columns, er0 := GetColumns(rows.Columns())
	err = er0
	if err != nil {
		return
	}
	for rows.Next() {
		gTb := reflect.New(structType).Interface()
		r, swapValues := StructScan(gTb, columns, fieldsIndex, -1)
		if err = rows.Scan(r...); err == nil {
			SwapValuesToBool(gTb, &swapValues)
			t = gTb
			break
		}
	}
	return
}

//Row
func ScanRow(row *sql.Row, structType reflect.Type) (t interface{}, err error) {
	t = reflect.New(structType).Interface()
	r, swapValues := StructScan(t, nil, nil, -1)
	err = row.Scan(r...)
	SwapValuesToBool(t, &swapValues)
	return
}
