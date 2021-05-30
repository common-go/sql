package sql

import (
	"context"
	"database/sql"
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
	attribute, attributeKey, _ := ExtractMapValue(value, &excludeColumns, false)
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

// Field model field definition
type Field struct {
	Tags  map[string]string
	Value reflect.Value
	Type  reflect.Type
}

func GetMapField(object interface{}) []Field {
	modelType := reflect.TypeOf(object)
	value := reflect.Indirect(reflect.ValueOf(object))
	var result []Field
	numField := modelType.NumField()

	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		selectField := Field{Value: value.Field(i), Type: modelType}
		gormTag, ok := field.Tag.Lookup("gorm")
		tag := make(map[string]string)
		tag["fieldName"] = field.Name
		if ok {
			str1 := strings.Split(gormTag, ";")
			for k := 0; k < len(str1); k++ {
				str2 := strings.Split(str1[k], ":")
				if len(str2) == 1 {
					tag[str2[0]] = str2[0]
					selectField.Tags = tag
				} else {
					tag[str2[0]] = str2[1]
					selectField.Tags = tag
				}
			}
			result = append(result, selectField)
		}
	}
	return result
}

type Statement struct {
	Sql  string        `mapstructure:"sql" json:"sql,omitempty" gorm:"column:sql" bson:"sql,omitempty" dynamodbav:"sql,omitempty" firestore:"sql,omitempty"`
	Args []interface{} `mapstructure:"args" json:"args,omitempty" gorm:"column:args" bson:"args,omitempty" dynamodbav:"args,omitempty" firestore:"args,omitempty"`
}
type Statements interface {
	Exec(ctx context.Context, db *sql.DB) (int64, error)
	Add(sql string, args []interface{}) Statements
	Clear() Statements
}

func NewDefaultStatements(successFirst bool) *DefaultStatements {
	stms := make([]Statement, 0)
	s := &DefaultStatements{Statements: stms, SuccessFirst: successFirst}
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
	var stm = Statement{Sql: sql, Args: args}
	s.Statements = append(s.Statements, stm)
	return s
}
func (s *DefaultStatements) Clear() Statements {
	s.Statements = s.Statements[:0]
	return s
}
