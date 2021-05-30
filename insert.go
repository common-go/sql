package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

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

func makeSchema(modelType reflect.Type) ([]string, map[string]fieldDB) {
	numField := modelType.NumField()
	columns := make([]string, 0)
	schema := make(map[string]fieldDB, 0)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
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

							jTag, jOk := field.Tag.Lookup("json")
							if jOk {
								tagJsons := strings.Split(jTag, ",")
								json = tagJsons[0]
							}
							f := fieldDB{
								json: json,
								column: col,
								index: i,
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
	return columns, schema
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
	cols, schema := makeSchema(modelType)
	for j := 0; j < s.Len(); j++ {
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
}
