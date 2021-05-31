package sql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

func RemoveIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}

func RemoveItem(slice []string, val string) []string {
	for i, item := range slice {
		if item == val {
			return RemoveIndex(slice, i)
		}
	}
	return slice
}

func Find(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}

func BuildInsert(table string, model interface{}, i int, buildParam func(int) string) (string, []interface{}) {
	mapData, mapKey, columns, keys := BuildMapDataAndKeys(model, false)
	var cols []string
	var values []interface{}
	var params []string
	for _, columnName := range keys {
		if value, ok := mapKey[columnName]; ok {
			cols = append(cols, QuoteColumnName(columnName))
			v2b, ok2 := GetDBValue(value)
			if ok2 {
				params = append(params, v2b)
			} else {
				values = append(values, value)
				p := buildParam(i)
				params = append(params, p)
				i++
			}
		}
	}
	for _, columnName := range columns {
		if v1, ok := mapData[columnName]; ok {
			cols = append(cols, QuoteColumnName(columnName))
			v1b, ok1 := GetDBValue(v1)
			if ok1 {
				params = append(params, v1b)
			} else {
				values = append(values, v1)
				p := buildParam(i)
				params = append(params, p)
				i++
			}
		}
	}
	column := strings.Join(cols, ",")
	return fmt.Sprintf("insert into %v(%v)values(%v)", table, column, strings.Join(params, ",")), values
}

func BuildInsertWithVersion(table string, model interface{}, i int, versionIndex int, buildParam func(int) string) (string, []interface{}) {
	if versionIndex < 0 {
		panic("version index not found")
	}

	var versionValue int64 = 1
	_, err := setValue(model, versionIndex, &versionValue)
	if err != nil {
		panic(err)
	}
	mapData, mapKey, columns, keys := BuildMapDataAndKeys(model, false)
	var cols []string
	var values []interface{}
	var params []string
	for _, columnName := range keys {
		if value, ok := mapKey[columnName]; ok {
			cols = append(cols, QuoteColumnName(columnName))
			v2b, ok2 := GetDBValue(value)
			if ok2 {
				params = append(params, v2b)
			} else {
				values = append(values, value)
				p := buildParam(i)
				params = append(params, p)
				i++
			}
		}
	}
	for _, columnName := range columns {
		if v1, ok := mapData[columnName]; ok {
			cols = append(cols, QuoteColumnName(columnName))
			v1b, ok1 := GetDBValue(v1)
			if ok1 {
				params = append(params, v1b)
			} else {
				values = append(values, v1)
				p := buildParam(i)
				params = append(params, p)
				i++
			}
		}
	}
	column := strings.Join(cols, ",")
	return fmt.Sprintf("insert into %v(%v)values(%v)", table, column, strings.Join(params, ",")), values
}

func QuoteColumnName(str string) string {
	//if strings.Contains(str, ".") {
	//	var newStrs []string
	//	for _, str := range strings.Split(str, ".") {
	//		newStrs = append(newStrs, str)
	//	}
	//	return strings.Join(newStrs, ".")
	//}
	return str
}

func BuildMapDataAndKeys(model interface{}, update bool) (map[string]interface{}, map[string]interface{}, []string, []string) {
	var mapData = make(map[string]interface{})
	var mapKey = make(map[string]interface{})
	keys := make([]string, 0)
	columns := make([]string, 0)
	mv := reflect.Indirect(reflect.ValueOf(model))
	modelType := mv.Type()
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		if colName, isKey, exist := CheckByIndex(modelType, i, update); exist {
			f := mv.Field(i)
			fieldValue := f.Interface()
			isNil := false
			if f.Kind() == reflect.Ptr {
				if reflect.ValueOf(fieldValue).IsNil() {
					isNil = true
				} else {
					fieldValue = reflect.Indirect(reflect.ValueOf(fieldValue)).Interface()
				}
			}
			if isKey {
				columns = append(columns, colName)
				if !isNil {
					mapKey[colName] = fieldValue
				}
			} else {
				keys = append(keys, colName)
				if !isNil {
					if boolValue, ok := fieldValue.(bool); ok {
						valueS := modelType.Field(i).Tag.Get(strconv.FormatBool(boolValue))
						mapData[colName] = valueS
					} else {
						mapData[colName] = fieldValue
					}
				}
			}
		}
	}
	return mapData, mapKey, keys, columns
}
func CheckByIndex(modelType reflect.Type, index int, update bool) (col string, isKey bool, colExist bool) {
	field := modelType.Field(index)
	tag, _ := field.Tag.Lookup("gorm")
	if strings.Contains(tag, IgnoreReadWrite) {
		return "", false, false
	}
	if update {
		if strings.Contains(tag, "update:false") {
			return "", false, false
		}
	}
	if has := strings.Contains(tag, "column"); has {
		str1 := strings.Split(tag, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == "column" {
					isKey := strings.Contains(tag, "primary_key")
					return str2[j+1], isKey, true
				}
			}
		}
	}
	return "", false, false
}

func QuoteByDriver(key, driver string) string {
	switch driver {
	case DriverMysql:
		return fmt.Sprintf("`%s`", key)
	case DriverMssql:
		return fmt.Sprintf(`[%s]`, key)
	default:
		return fmt.Sprintf(`"%s"`, key)
	}
}

func BuildResult(result int64, err error) (int64, error) {
	if err != nil {
		return result, err
	} else {
		return result, nil
	}
}
