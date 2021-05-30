package sql

import (
	"reflect"
	"strings"
)

var cache map[reflect.Type]Schema

type Schema struct {
	Type       reflect.Type
	Key        []string
	Columns    []string
	Insert     []string
	Update     []string
	ColumnMap  map[string]int
	Map        map[string]string // key: json value: column
	BoolFields map[string]BoolStruct
}

type BoolStruct struct {
	Index int
	True  string
	False string
}

func GetSchema(modelType reflect.Type) Schema {
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
	var columnMap map[string]int
	var jsonMap map[string]string // key: json value: column
	var boolMap map[string]BoolStruct
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
							insert = append(insert, col)
							if isKey {
								keys = append(keys, col)
							}
							if !isKey && isUpdate {
								update = append(update, col)
							}
							schema.Map = jsonMap
							schema.Columns = columns
							schema.Key = keys
							schema.Insert = insert
							schema.Update = update
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
