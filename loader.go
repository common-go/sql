package sql

import (
	"context"
	"database/sql"
	"encoding/json"

	"fmt"
	_ "github.com/sirupsen/logrus"
	"reflect"
	"strings"
)

type Loader struct {
	Database          *sql.DB
	Map               func(ctx context.Context, model interface{}) (interface{}, error)
	modelType         reflect.Type
	modelsType        reflect.Type
	keys              []string
	mapJsonColumnKeys map[string]string
	fieldsIndex       map[string]int
	table             string
}

func NewLoader(db *sql.DB, tableName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *Loader {
	var mp func(ctx context.Context, model interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	_, idNames := FindNames(modelType)
	mapJsonColumnKeys := MapJsonColumn(modelType)
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	fieldsIndex, er0 := GetColumnIndexes(modelType)
	if er0 != nil {
		panic(er0)
	}
	return &Loader{db, mp, modelType, modelsType, idNames, mapJsonColumnKeys, fieldsIndex, tableName}
}

func (s *Loader) Keys() []string {
	return s.keys
}

func (s *Loader) All(ctx context.Context) (interface{}, error) {
	queryGetAll := BuildSelectAllQuery(s.table)
	result := reflect.New(s.modelsType).Interface()
	err := QueryWithType(s.Database, result, s.modelType, s.fieldsIndex, queryGetAll)
	if err == nil {
		if s.Map != nil {
			return MapModels(ctx, result, s.Map)
		}
		return result, err
	}
	return result, err
}

func (s *Loader) Load(ctx context.Context, ids interface{}) (interface{}, error) {
	queryFindById, values := BuildFindById(s.Database, s.table, ids, s.mapJsonColumnKeys, s.keys)
	r, err := QueryRow(s.Database, s.modelType, s.fieldsIndex, queryFindById, values...)
	if s.Map != nil {
		_, er2 := s.Map(ctx, &r)
		if er2 != nil {
			return r, er2
		}
		return r, er2
	}
	return r, err
}

func (s *Loader) Exist(ctx context.Context, id interface{}) (bool, error) {
	var count int32
	var where string
	buildParam := GetBuild(s.Database)
	var values []interface{}
	colNumber := 1
	if len(s.keys) == 1 {
		where = fmt.Sprintf("where %s = %s", s.mapJsonColumnKeys[s.keys[0]], buildParam(colNumber))
		values = append(values, id)
		colNumber++
	} else {
		queres := make([]string, 0)
		var ids = id.(map[string]interface{})
		for k, idk := range ids {
			columnName := s.mapJsonColumnKeys[k]
			queres = append(queres, fmt.Sprintf("%s = %s", columnName, buildParam(colNumber)))
			values = append(values, idk)
			colNumber++
		}
		where = "where " + strings.Join(queres, " and ")
	}
	row := s.Database.QueryRow(fmt.Sprintf("select count(*) from %s %s", s.table, where), values...)
	if err := row.Scan(&count); err != nil {
		return false, err
	} else {
		if count >= 1 {
			return true, nil
		}
		return false, nil
	}
}

func (s *Loader) LoadAndDecode(ctx context.Context, id interface{}, result interface{}) (bool, error) {
	var values []interface{}
	sql, values := BuildFindById(s.Database, s.table, id, s.mapJsonColumnKeys, s.keys)
	rowData, err1 := QueryRow(s.Database, s.modelType, s.fieldsIndex, sql, values...)
	if err1 != nil || rowData == nil {
		return false, err1
	}
	byteData, _ := json.Marshal(rowData)
	err := json.Unmarshal(byteData, &result)
	if err1 != nil {
		return false, err
	}
	//reflect.ValueOf(result).Elem().Set(reflect.ValueOf(rowData).Elem())
	if s.Map != nil {
		_, er3 := s.Map(ctx, result)
		if er3 != nil {
			return true, er3
		}
	}
	return true, nil
}

func MapJsonColumn(modelType reflect.Type) map[string]string {
	numField := modelType.NumField()
	columnNameKeys := make(map[string]string)
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
								tagj, ok1 := field.Tag.Lookup("json")
								t := strings.Split(tagj, ",")
								if ok1 && len(t) > 0 {
									json := t[0]
									columnNameKeys[json] = str2[j+1]
								}
							}
						}
					}
				}
			}
		}
	}
	return columnNameKeys
}
