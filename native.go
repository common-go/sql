package sql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// raw query
func Save(ctx context.Context, db *sql.DB, table string, model interface{}) (int64, error) {
	queryString, value, err := BuildSql(db, table, model)
	if err != nil {
		return 0, err
	}
	res, err := db.ExecContext(ctx, queryString, value...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()

}

func SaveTx(ctx context.Context, db *sql.DB, tx *sql.Tx, table string, model interface{}) (int64, error) {
	query, values, err0 := BuildSql(db, table, model)
	if err0 != nil {
		return -1, err0
	}
	r, err1 := tx.ExecContext(ctx, query, values...)
	if err1 != nil {
		return -1, err1
	}
	return r.RowsAffected()
}

func BuildSql(db *sql.DB, table string, model interface{}, options...int) (string, []interface{}, error) {
	placeholders := make([]string, 0)
	exclude := make([]string, 0)
	modelType := reflect.Indirect(reflect.ValueOf(model)).Type()
	numField := modelType.NumField()
	for j := 0; j < numField; j++ {
		field := modelType.Field(j)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.TrimSpace(tag) == "-" {
				exclude = append(exclude, field.Name)
			}
		}
	}

	attrs, unique, err := ExtractMapValue(model, &exclude, false)
	if err != nil {
		return "0", nil, fmt.Errorf("cannot extract object's values: %w", err)
	}
	//mainScope := db.NewScope(model)
	//pkey := FindIdFields(modelType)
	size := len(attrs)
	dbColumns := make([]string, 0, size)
	variables := make([]string, 0, size)
	sorted := SortedKeys(attrs)

	// Also append variables to mainScope

	var setColumns []string
	dialect := GetDriver(db)
	i := 0
	switch dialect {
	case DriverPostgres:
		uniqueCols := make([]string, 0)
		value := make([]interface{}, 0, len(attrs)*2)
		for ; i < len(sorted); i++ {
			setColumns = append(setColumns, `"`+strings.Replace(sorted[i], `"`, `""`, -1)+`"`+" = excluded."+strings.Replace(sorted[i], `"`, `""`, -1))
			dbColumns = append(dbColumns, "`"+strings.Replace(sorted[i], "`", "``", -1)+"`")
			variables = append(variables, "$"+strconv.Itoa(i+1))
			value = append(value, attrs[sorted[i]])
		}
		for key, val := range unique {
			i++
			uniqueCols = append(uniqueCols, `"`+strings.Replace(key, `"`, `""`, -1)+`"`)
			dbColumns = append(dbColumns, "`"+strings.Replace(key, "`", "``", -1)+"`")
			variables = append(variables, "$"+strconv.Itoa(i+1))
			value = append(value, val)
		}
		queryString := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s",
			`"`+strings.Replace(table, `"`, `""`, -1)+`"`,
			strings.Join(dbColumns, ", "),
			strings.Join(variables, ", "),
			strings.Join(uniqueCols, ", "),
			strings.Join(setColumns, ", "),
		)
		return queryString, value, nil
	case DriverOracle:
		uniqueCols := make([]string, 0)
		inColumns := make([]string, 0)
		value := make([]interface{}, 0, len(attrs)*2)
		insertCols := make([]string, 0)
		for v, key := range sorted {
			value = append(value, attrs[key])
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
			value = append(value, val)
			insertCols = append(insertCols, tkey)
		}
		//for _, key := range sorted {
		//	value = append(value, attrs[key])
		//}
		queryString := fmt.Sprintf("MERGE INTO %s a USING (SELECT %s FROM dual) temp ON  (%s) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
			`"`+strings.Replace(table, `"`, `""`, -1)+`"`,
			strings.Join(variables, ", "),
			strings.Join(uniqueCols, " AND "),
			strings.Join(setColumns, ", "),
			strings.Join(insertCols, ", "),
			strings.Join(inColumns, ", "),
		)
		return queryString, value, nil
	case DriverSqlite3, DriverMysql:
		value := make([]interface{}, 0, len(attrs)*2)
		for _, key := range sorted {
			//mainScope.AddToVars(attrs[key])
			setColumns = append(setColumns, "`"+strings.Replace(key, "`", "``", -1)+"`"+" = ?")
			dbColumns = append(dbColumns, "`"+strings.Replace(key, "`", "``", -1)+"`")
			variables = append(variables, "?")
			value = append(value, attrs[key])
		}
		for key, val := range unique {
			//mainScope.AddToVars(attrs[key])
			dbColumns = append(dbColumns, "`"+strings.Replace(key, "`", "``", -1)+"`")
			variables = append(variables, "?")
			value = append(value, val)
		}
		valueQuery := "(" + strings.Join(variables, ", ") + ")"
		placeholders = append(placeholders, valueQuery)
		queryString := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			"`"+strings.Replace(table, "`", "``", -1)+"`",
			strings.Join(dbColumns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(setColumns, ", "),
		)
		for _, s := range sorted {
			value = append(value, attrs[s])
		}
		return queryString, value, nil
	case DriverMssql:
		uniqueCols := make([]string, 0)
		value := make([]interface{}, 0, len(attrs)*2)
		for _, key := range sorted {
			//mainScope.AddToVars(attrs[key])
			tkey := `"` + strings.Replace(key, `"`, `""`, -1) + `"`
			dbColumns = append(dbColumns, tkey)
			variables = append(variables, "?")
			value = append(value, attrs[key])
			setColumns = append(setColumns, tkey+" = temp."+tkey)
		}
		for i, val := range unique {
			tkey := `"` + strings.Replace(i, `"`, `""`, -1) + `"`
			dbColumns = append(dbColumns, `"`+strings.Replace(tkey, `"`, `""`, -1)+`"`)
			variables = append(variables, "?")
			value = append(value, val)
			onDupe := table + "." + tkey + " = " + "temp." + tkey
			uniqueCols = append(uniqueCols, onDupe)
		}
		queryString := fmt.Sprintf("MERGE INTO %s USING (VALUES %s) AS temp (%s) ON %s WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES %s;",
			`"`+strings.Replace(table, `"`, `""`, -1)+`"`,
			strings.Join(variables, ", "),
			strings.Join(dbColumns, ", "),
			strings.Join(uniqueCols, " AND "),
			strings.Join(setColumns, ", "),
			strings.Join(dbColumns, ", "),
			strings.Join(variables, ", "),
		)
		return queryString, value, nil
	default:
		return "", nil, fmt.Errorf("unsupported db vendor")
	}
}
