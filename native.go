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
	queryString, value, err := BuildSave(db, table, model)
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
	query, values, err0 := BuildSave(db, table, model)
	if err0 != nil {
		return -1, err0
	}
	r, err1 := tx.ExecContext(ctx, query, values...)
	if err1 != nil {
		return -1, err1
	}
	return r.RowsAffected()
}

func BuildSave(db *sql.DB, table string, model interface{}) (string, []interface{}, error) {
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

	attrs, unique, nAttrs, err := ExtractMapValue(model, &exclude, false)
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
	driver := GetDriver(db)
	i := 0
	switch driver {
	case DriverPostgres:
		uniqueCols := make([]string, 0)
		values := make([]interface{}, 0, len(attrs)*2)
		for ; i < len(sorted); i++ {
			setColumns = append(setColumns, `"`+strings.Replace(sorted[i], `"`, `""`, -1)+`"`+" = excluded."+strings.Replace(sorted[i], `"`, `""`, -1))
			dbColumns = append(dbColumns, "`"+strings.Replace(sorted[i], "`", "``", -1)+"`")
			variables = append(variables, "$"+strconv.Itoa(i+1))
			values = append(values, attrs[sorted[i]])
		}
		for key, val := range unique {
			i++
			uniqueCols = append(uniqueCols, `"`+strings.Replace(key, `"`, `""`, -1)+`"`)
			dbColumns = append(dbColumns, "`"+strings.Replace(key, "`", "``", -1)+"`")
			variables = append(variables, "$"+strconv.Itoa(i+1))
			values = append(values, val)
		}
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s",
			`"`+strings.Replace(table, `"`, `""`, -1)+`"`,
			strings.Join(dbColumns, ", "),
			strings.Join(variables, ", "),
			strings.Join(uniqueCols, ", "),
			strings.Join(setColumns, ", "),
		)
		return query, values, nil
	case DriverOracle:
		uniqueCols := make([]string, 0)
		inColumns := make([]string, 0)
		values := make([]interface{}, 0, len(attrs)*2)
		insertCols := make([]string, 0)
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
		//for _, key := range sorted {
		//	value = append(value, attrs[key])
		//}
		query := fmt.Sprintf("MERGE INTO %s a USING (SELECT %s FROM dual) temp ON  (%s) WHEN MATCHED THEN UPDATE SET %s WHEN NOT MATCHED THEN INSERT (%s) VALUES (%s)",
			`"`+strings.Replace(table, `"`, `""`, -1)+`"`,
			strings.Join(variables, ", "),
			strings.Join(uniqueCols, " AND "),
			strings.Join(setColumns, ", "),
			strings.Join(insertCols, ", "),
			strings.Join(inColumns, ", "),
		)
		return query, values, nil
	case DriverMysql:
		values := make([]interface{}, 0, len(attrs)*2)
		updates := make([]interface{}, 0)
		for key, val := range unique {
			_, notNil := nAttrs[key]
			//mainScope.AddToVars(attrs[key])
			if notNil {
				v, ok := GetDBValue(val)
				dbColumns = append(dbColumns, "`"+strings.Replace(key, "`", "``", -1)+"`")
				if ok {
					variables = append(variables, v)
				} else {
					variables = append(variables, "?")
					values = append(values, val)
				}
			}
		}
		for _, key := range sorted {
			//mainScope.AddToVars(attrs[key])
			val, notNil := nAttrs[key]
			if notNil {
				v, ok := GetDBValue(val)
				dbColumns = append(dbColumns, "`"+strings.Replace(key, "`", "``", -1)+"`")
				if ok {
					setColumns = append(setColumns, "`"+strings.Replace(key, "`", "``", -1)+"`"+" = " + v)
					variables = append(variables, v)
				} else {
					setColumns = append(setColumns, "`"+strings.Replace(key, "`", "``", -1)+"`"+" = ?")
					variables = append(variables, "?")
					values = append(values, val)
					updates = append(updates, val)
				}
			} else {
				setColumns = append(setColumns, "`"+strings.Replace(key, "`", "``", -1)+"`"+" = null")
			}
		}
		valueQuery := "(" + strings.Join(variables, ", ") + ")"
		placeholders = append(placeholders, valueQuery)
		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
			"`"+strings.Replace(table, "`", "``", -1)+"`",
			strings.Join(dbColumns, ", "),
			strings.Join(placeholders, ", "),
			strings.Join(setColumns, ", "),
		)
		for _, s := range updates {
			values = append(values, s)
		}
		return query, values, nil
	case DriverMssql:
		uniqueCols := make([]string, 0)
		values := make([]interface{}, 0, len(attrs)*2)
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
		return query, values, nil
	default:
		return "", nil, fmt.Errorf("unsupported db vendor")
	}
}
