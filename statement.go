package sql

import (
	"reflect"
	"strings"
)

type BatchStatement struct {
	Query         string
	Values        []interface{}
	Keys          []string
	Columns       []string
	Attributes    map[string]interface{}
	AttributeKeys map[string]interface{}
}

func newStatement(value interface{}, excludeColumns ...string) BatchStatement {
	attribute, attributeKey, _, _ := ExtractMapValue(value, &excludeColumns, false)
	attrSize := len(attribute)
	modelType := reflect.TypeOf(value)
	keys := FindDBColumNames(modelType)
	// Replace with database column name
	dbColumns := make([]string, 0, attrSize)
	for _, key := range SortedKeys(attribute) {
		dbColumns = append(dbColumns, QuoteColumnName(key))
	}
	// Scope to eventually run SQL
	statement := BatchStatement{Keys: keys, Columns: dbColumns, Attributes: attribute, AttributeKeys: attributeKey}
	return statement
}

func statement() BatchStatement {
	attributes := make(map[string]interface{})
	attributeKeys := make(map[string]interface{})
	return BatchStatement{Keys: []string{}, Columns: []string{}, Attributes: attributes, AttributeKeys: attributeKeys}
}

func FindDBColumNames(modelType reflect.Type) []string {
	numField := modelType.NumField()
	var idFields []string
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				k, ok := findTag(ormTag, "column")
				if ok {
					idFields = append(idFields, k)
				}
			}
		}
	}
	return idFields
}

func findTag(tag string, key string) (string, bool) {
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
