package sql

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
)

type BatchPatcher struct {
	db          *sql.DB
	tableName   string
	idNames     []string
	idJsonName  []string
	buildParam  func(i int) string
	modelsType  reflect.Type
	modelsTypes reflect.Type
}

func NewBatchPatcher(db *sql.DB, tableName string, modelType reflect.Type, options...func(i int) string) *BatchPatcher {
	return NewBatchPatcherWithIds(db, tableName, modelType, nil, options...)
}
func NewBatchPatcherWithIds(db *sql.DB, tableName string, modelType reflect.Type, fieldName []string, options...func(i int) string) *BatchPatcher {
	modelsTypes := reflect.Zero(reflect.SliceOf(modelType)).Type()
	idJsonName := make([]string, 0)
	if fieldName == nil || len(fieldName) == 0 {
		fieldName, idJsonName = FindPrimaryKeys(modelType)
	}
	var buildParam func(i int) string
	if len(options) > 0 && options[0] != nil {
		buildParam = options[0]
	} else {
		buildParam = GetBuild(db)
	}
	return &BatchPatcher{db: db, tableName: tableName, idNames: fieldName, idJsonName: idJsonName, modelsType: modelType, modelsTypes: modelsTypes, buildParam: buildParam}
}

func (w *BatchPatcher) Write(ctx context.Context, models []map[string]interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)
	_, err := PatchInTransaction(ctx, w.db, w.tableName, models, w.idNames, w.idJsonName, w.buildParam)

	if err == nil {
		// Return full success
		successIndices = toArrayMapIndex(models, failIndices)
		return successIndices, failIndices, err
	} else {
		// Return full fail
		failIndices = toArrayMapIndex(models, failIndices)
	}
	return successIndices, failIndices, err
}

func toArrayMapIndex(models []map[string]interface{}, indices []int) []int {
	for i, _ := range models {
		indices = append(indices, i)
	}
	return indices
}

func FindPrimaryKeys(modelType reflect.Type) ([]string, []string) {
	numField := modelType.NumField()
	var idColumnFields []string
	var idJsons []string
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		tags := strings.Split(ormTag, ";")
		for _, tag := range tags {
			if strings.Compare(strings.TrimSpace(tag), "primary_key") == 0 {
				k, ok := findTag(ormTag, "column")
				if ok {
					idColumnFields = append(idColumnFields, k)
					tag1, ok1 := field.Tag.Lookup("json")
					tagJsons := strings.Split(tag1, ",")
					if ok1 && len(tagJsons) > 0 {
						idJsons = append(idJsons, tagJsons[0])
					}
				}
			}
		}
	}
	return idColumnFields, idJsons
}

func FindJsonName(modelType reflect.Type) map[string]string {
	numField := modelType.NumField()
	mapJsonColumn := make(map[string]string)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		column, ok := findTag(ormTag, "column")
		if ok {
			tag1, ok1 := field.Tag.Lookup("json")
			tagJsons := strings.Split(tag1, ",")
			if ok1 && len(tagJsons) > 0 {
				mapJsonColumn[tagJsons[0]] = column
			}
		}
	}
	return mapJsonColumn
}
