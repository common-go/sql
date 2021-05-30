package schema

import (
	"reflect"
	"strings"
)

const IgnoreReadWrite = "-"

var cache map[reflect.Type]Schema

type Schema struct {
	Type       reflect.Type
	Key        []string
	Columns    []string
	Insert     []string
	Update     []string
	ColumnMap  map[string]int
	UpdateMap  map[string]int
	KeyMap     map[string]int
	Map        map[string]string // key: json value: column
	BoolFields map[string]BoolStruct
}

type BoolStruct struct {
	Index int
	True  string
	False string
}

func GetSchema(modelType reflect.Type) Schema {
	if cache == nil {
		cache = make(map[reflect.Type]Schema)
	}
	s, ok := cache[modelType]
	if ok {
		return s
	}
	s0 := BuildSchema(modelType)
	cache[modelType] = s0
	return s0
}
func BuildSchema(modelType reflect.Type) Schema {
	var schema Schema
	keys := make([]string, 0)
	columns := make([]string, 0)
	insert := make([]string, 0)
	update := make([]string, 0)
	columnMap := make(map[string]int, 0)
	keyMap := make(map[string]int, 0)
	updateMap := make(map[string]int, 0)
	jsonMap := make(map[string]string, 0) // key: json value: column
	boolMap := make(map[string]BoolStruct, 0)
	schema.Type = modelType
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		tag, _ := field.Tag.Lookup("gorm")
		if !strings.Contains(tag, IgnoreReadWrite) {
			if has := strings.Contains(tag, "column"); has {
				json := field.Name
				col := json
				str1 := strings.Split(tag, ";")
				num := len(str1)
				for i := 0; i < num; i++ {
					str2 := strings.Split(str1[i], ":")
					for j := 0; j < len(str2); j++ {
						if str2[j] == "column" {
							col = str2[j+1]
							jTag, jOk := field.Tag.Lookup("json")
							if jOk {
								tagJsons := strings.Split(jTag, ",")
								json = tagJsons[0]
							}
							tTag, tOk := field.Tag.Lookup("true")
							if tOk {
								fTag, fOk := field.Tag.Lookup("false")
								bs := BoolStruct{Index: i, True: tTag}
								if fOk {
									bs.False = fTag
								}
								boolMap[col] = bs
							}
							isKey := strings.Contains(tag, "primary_key")
							isUpdate := !strings.Contains(tag, "update:false")
							columnMap[col] = i
							jsonMap[json] = col
							columns = append(columns, col)
							if isKey {
								keys = append(keys, col)
								keyMap[col] = i
							}
							if !isKey {
								insert = append(insert, col)
								if isUpdate {
									update = append(update, col)
									updateMap[col] = i
								}
							}
							schema.Key = keys
							schema.Columns = columns
							schema.Insert = insert
							schema.Update = update
							schema.ColumnMap = columnMap
							schema.UpdateMap = updateMap
							schema.KeyMap = keyMap
							schema.Map = jsonMap
							schema.BoolFields = boolMap
							break
						}
					}
				}
			}
		}
	}
	return schema
}

/*
func BuildMapDataAndKeys(model interface{}, update bool) (map[string]interface{}, map[string]interface{}, []string, []string) {
	var mapData = make(map[string]interface{})
	var mapKey = make(map[string]interface{})
	mv := reflect.Indirect(reflect.ValueOf(model))
	modelType := mv.Type()
	schema := GetSchema(modelType)
	columns := schema.Insert
	if update {
		columns = schema.Update
	}
	for _, colName := range columns {
		i := schema.ColumnMap[colName]
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
		if !isNil {
			if boolValue, ok := fieldValue.(bool); ok {
				bs := schema.BoolFields[colName]
				if boolValue {
					mapData[colName] = bs.True
				} else {
					mapData[colName] = bs.False
				}
			} else {
				mapData[colName] = fieldValue
			}
		}
	}
	for _, colName := range schema.Key {
		i := schema.ColumnMap[colName]
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
		if !isNil {
			mapKey[colName] = fieldValue
		}
	}
	return mapData, mapKey, columns, schema.Key
}
*/
