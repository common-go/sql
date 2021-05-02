package query

import (
	"database/sql"
	"fmt"
	s "github.com/core-go/search"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

const (
	driverPostgres   = "postgres"
	driverMysql      = "mysql"
	driverMssql      = "mssql"
	driverOracle     = "oracle"
	driverSqlite3    = "sqlite3"
	driverNotSupport = "no support"
	desc             = "desc"
	asc              = "asc"
)

type Builder struct {
	TableName  string
	ModelType  reflect.Type
	Driver     string
	BuildParam func(int) string
}

func NewBuilder(db *sql.DB, tableName string, modelType reflect.Type, options ...func(int) string) *Builder {
	driver := getDriver(db)
	var build func(int) string
	if len(options) > 0 {
		build = options[0]
	} else {
		build = getBuild(db)
	}
	return NewBuilderWithDriver(tableName, modelType, driver, build)
}
func NewBuilderWithDriver(tableName string, modelType reflect.Type, driver string, buildParam func(int) string) *Builder {
	return &Builder{TableName: tableName, ModelType: modelType, Driver: driver, BuildParam: buildParam}
}

const (
	exact            = "="
	like             = "like"
	greaterEqualThan = ">="
	greaterThan      = ">"
	lessEqualThan    = "<="
	lessThan         = "<"
	in               = "in"
)

func getStringFromTag(typeOfField reflect.StructField, tagName string, key string) *string {
	tag := typeOfField.Tag
	properties := strings.Split(tag.Get(tagName), ";")
	for _, property := range properties {
		if strings.HasPrefix(property, key) {
			column := property[len(key):]
			return &column
		}
	}
	return nil
}

func getJoinFromSqlBuilderTag(typeOfField reflect.StructField) *string {
	return getStringFromTag(typeOfField, "sql_builder", "join:")
}

func getColumnNameFromSqlBuilderTag(typeOfField reflect.StructField) *string {
	return getStringFromTag(typeOfField, "sql_builder", "column:")
	/*tag := typeOfField.Tag
	properties := strings.Split(tag.Get("sql_builder"), ";")
	for _, property := range properties {
		if strings.HasPrefix(property, "column:") {
			column := property[7:]
			return &column
		}
	}
	return nil*/
}
func (b *Builder) BuildQuery(sm interface{}) (string, []interface{}) {
	return Build(sm, b.TableName, b.ModelType, b.Driver, b.BuildParam)
}
func Build(sm interface{}, tableName string, modelType reflect.Type, driver string, buildParam func(int) string) (string, []interface{}) {
	s1 := ""
	rawConditions := make([]string, 0)
	queryValues := make([]interface{}, 0)
	rawJoin := make([]string, 0)
	sortString := ""
	fields := make([]string, 0)
	var keyword string
	var keywordFormat map[string]string
	keywordFormat = map[string]string{
		"prefix":  "?%",
		"contain": "%?%",
		"equal":   "?",
	}

	value := reflect.Indirect(reflect.ValueOf(sm))
	typeOfValue := value.Type()
	numField := value.NumField()
	marker := 0

	for i := 0; i < numField; i++ {
		field := value.Field(i)
		kind := field.Kind()
		x := field.Interface()
		typeOfField := value.Type().Field(i)
		param := buildParam(marker + 1)

		if v, ok := x.(*s.SearchModel); ok {
			if len(v.Fields) > 0 {
				for _, key := range v.Fields {
					i, _, columnName := getFieldByJson(modelType, key)
					if len(columnName) < 0 {
						fields = fields[len(fields):]
						break
					} else if i == -1 {
						columnName = strings.ToLower(key) // injection
					}
					fields = append(fields, columnName)
				}
			}
			if len(fields) > 0 {
				s1 = `select ` + strings.Join(fields, ",") + ` from ` + tableName
			} else {
				columns := getColumnsSelect(modelType)
				if len(columns) > 0 {
					s1 = `select  ` + strings.Join(columns, ",") + ` from ` + tableName
				} else {
					s1 = `select * from ` + tableName
				}
			}
			if len(v.Sort) > 0 {
				sortString = buildSort(v.Sort, modelType)
			}
		}

		columnName, existCol := getColumnName(value.Type(), typeOfField.Name)
		if !existCol {
			columnName, _ = getColumnName(modelType, typeOfField.Name)
		}

		columnNameFromSqlBuilderTag := getColumnNameFromSqlBuilderTag(typeOfField)
		if columnNameFromSqlBuilderTag != nil {
			columnName = *columnNameFromSqlBuilderTag
		}

		joinFromSqlBuilderTag := getJoinFromSqlBuilderTag(typeOfField)
		if joinFromSqlBuilderTag != nil {
			rawJoin = append(rawJoin, *joinFromSqlBuilderTag)
		}

		ps := false
		var value2 string
		if kind == reflect.Ptr {
			if field.IsNil() {
				continue
			}
			s0, ok0 := x.(*string)
			if ok0 {
				if s0 == nil || len(*s0) == 0 {
					continue
				}
				ps = true
				value2 = *s0
			}
			field = field.Elem()
			kind = field.Kind()
		}
		s0, ok0 := x.(string)
		if ok0 {
			if len(s0) == 0 {
				continue
			}
			value2 = s0
		}
		if v, ok := x.(*s.SearchModel); ok {
			if len(v.Excluding) > 0 {
				for key, val := range v.Excluding {
					index, _, columnName := getFieldByJson(value.Type(), key)
					if index == -1 || columnName == "" {
						log.Panic("column name not found")
					}
					if len(val) > 0 {
						format := fmt.Sprintf("(%s)", buildParametersFrom(marker, len(val), buildParam))
						marker += len(val) - 1
						rawConditions = append(rawConditions, fmt.Sprintf("%s NOT IN %s", columnName, format))
						queryValues = extractArray(queryValues, val)
					}
				}
			} else if len(v.Keyword) > 0 {
				keyword = strings.TrimSpace(v.Keyword)
			}
			continue
		} else if ps || kind == reflect.String {
			var searchValue bool
			if field.Len() > 0 {
				const defaultKey = "contain"
				if key, ok := typeOfValue.Field(i).Tag.Lookup("match"); ok {
					if format, exist := keywordFormat[key]; exist {
						searchValue = true
						//if sql == "mysql" {
						//	value2 = EscapeString(value2)
						//} else if sql == "postgres" || sql == "mssql" {
						//	value2 = EscapeStringForSelect(value2)
						//}
						value2 = func(format, s string) string {
							return strings.Replace(format, "?", s, -1)
						}(format, value2)
						//value2 = value2 + `%`
						//rawConditions = append(rawConditions, fmt.Sprintf("%s %s ?", columnName, like))
						queryValues = append(queryValues, value2)
					} else {
						log.Panicf("match not support \"%v\" format\n", key)
					}
				} else if format, exist := keywordFormat[defaultKey]; exist {
					searchValue = true
					//if sql == "mysql" {
					//	value2 = EscapeString(value2)
					//} else if sql == "postgres" || sql == "mssql" {
					//	value2 = EscapeStringForSelect(value2)
					//}
					//value2 = `%` + value2 + `%`
					value2 = func(format, s string) string {
						return strings.Replace(format, "?", s, -1)
					}(format, value2)
					//rawConditions = append(rawConditions, fmt.Sprintf("%s %s ?", columnName, like))
					queryValues = append(queryValues, value2)
				} else {
					searchValue = true
					value2, valid := x.(string)
					if !valid {
						log.Panicf("invalid data \"%v\" \n", x)
					}
					value2 = value2 + `%`
					//rawConditions = append(rawConditions, fmt.Sprintf("%s %s ?", columnName, like))
					queryValues = append(queryValues, value2)
				}
			} else if len(keyword) > 0 {
				if key, ok := typeOfValue.Field(i).Tag.Lookup("keyword"); ok {
					if format, exist := keywordFormat[key]; exist {
						//if sql == "mysql" {
						//	keyword = EscapeString(keyword)
						//} else if sql == "postgres" || sql == "mssql" {
						//	keyword = EscapeStringForSelect(keyword)
						//}
						if format == `?%` {
							keyword = keyword + `%`
						} else if format == `%?%` {
							keyword = `%` + keyword + `%`
						} else {
							log.Panicf("keyword not support \"%v\" format\n", key)
						}

						queryValues = append(queryValues, keyword)
					} else {
						log.Panicf("keyword not support \"%v\" format\n", key)
					}
				}
			}
			if searchValue {
				if driver == driverPostgres { // "postgres"
					rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, `ilike`, param))
				} else {
					rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, like, param))
				}
				marker++
			}
		} else if dateRange, ok := x.(s.DateRange); ok {
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, greaterEqualThan, param))
			queryValues = append(queryValues, dateRange.StartDate)
			var eDate = dateRange.EndDate.Add(time.Hour * 24)
			dateRange.EndDate = &eDate
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, lessThan, param))
			queryValues = append(queryValues, dateRange.EndDate)
			marker += 2
		} else if dateRange, ok := x.(*s.DateRange); ok && dateRange != nil {
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, greaterEqualThan, param))
			queryValues = append(queryValues, dateRange.StartDate)
			var eDate = dateRange.EndDate.Add(time.Hour * 24)
			dateRange.EndDate = &eDate
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, lessThan, param))
			queryValues = append(queryValues, dateRange.EndDate)
			marker += 2
		} else if dateTime, ok := x.(s.TimeRange); ok {
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, greaterEqualThan, param))
			queryValues = append(queryValues, dateTime.StartTime)
			var eDate = dateTime.EndTime.Add(time.Hour * 24)
			dateTime.EndTime = &eDate
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, lessThan, param))
			queryValues = append(queryValues, dateTime.EndTime)
			marker += 2
		} else if dateTime, ok := x.(*s.TimeRange); ok && dateTime != nil {
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, greaterEqualThan, param))
			queryValues = append(queryValues, dateTime.StartTime)
			var eDate = dateTime.EndTime.Add(time.Hour * 24)
			dateTime.EndTime = &eDate
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, lessThan, param))
			queryValues = append(queryValues, dateTime.EndTime)
			marker += 2
		} else if numberRange, ok := x.(s.NumberRange); ok {
			if numberRange.Min != nil {
				rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, greaterEqualThan, param))
				queryValues = append(queryValues, numberRange.Min)
				marker++
			} else if numberRange.Lower != nil {
				rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, greaterThan, param))
				queryValues = append(queryValues, numberRange.Lower)
				marker++
			}
			if numberRange.Max != nil {
				rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, lessEqualThan, param))
				queryValues = append(queryValues, numberRange.Max)
				marker++
			} else if numberRange.Upper != nil {
				rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, lessThan, param))
				queryValues = append(queryValues, numberRange.Upper)
				marker++
			}
		} else if numberRange, ok := x.(*s.NumberRange); ok && numberRange != nil {
			if numberRange.Min != nil {
				rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, greaterEqualThan, param))
				queryValues = append(queryValues, numberRange.Min)
				marker++
			} else if numberRange.Lower != nil {
				rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, greaterThan, param))
				queryValues = append(queryValues, numberRange.Lower)
				marker++
			}
			if numberRange.Max != nil {
				rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, lessEqualThan, param))
				queryValues = append(queryValues, numberRange.Max)
				marker++
			} else if numberRange.Upper != nil {
				rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, lessThan, param))
				queryValues = append(queryValues, numberRange.Upper)
				marker++
			}
		} else if kind == reflect.Slice {
			if field.Len() > 0 {
				format := fmt.Sprintf("(%s)", buildParametersFrom(marker, field.Len(), buildParam))
				rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, in, format))
				queryValues = extractArray(queryValues, x)
				marker += field.Len()
			}
		} else {
			rawConditions = append(rawConditions, fmt.Sprintf("%s %s %s", columnName, exact, param))
			queryValues = append(queryValues, x)
		}
	}

	if len(rawJoin) > 0 {
		s1 = s1 + " " + strings.Join(rawJoin, " ")
	}
	if len(rawConditions) > 0 {
		s2 := s1 + ` where ` + strings.Join(rawConditions, " AND ") + sortString
		return s2, queryValues
	}
	s3 := s1 + sortString
	return s3, queryValues
}

func extractArray(values []interface{}, field interface{}) []interface{} {
	s := reflect.Indirect(reflect.ValueOf(field))
	for i := 0; i < s.Len(); i++ {
		values = append(values, s.Index(i).Interface())
	}
	return values
}
func getFieldByJson(modelType reflect.Type, jsonName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		tag1, ok1 := field.Tag.Lookup("json")
		if ok1 && strings.Split(tag1, ",")[0] == jsonName {
			if tag2, ok2 := field.Tag.Lookup("gorm"); ok2 {
				if has := strings.Contains(tag2, "column"); has {
					str1 := strings.Split(tag2, ";")
					num := len(str1)
					for k := 0; k < num; k++ {
						str2 := strings.Split(str1[k], ":")
						for j := 0; j < len(str2); j++ {
							if str2[j] == "column" {
								return i, field.Name, str2[j+1]
							}
						}
					}
				}
			}
			return i, field.Name, ""
		}
	}
	return -1, jsonName, jsonName
}
func getColumnName(modelType reflect.Type, fieldName string) (col string, colExist bool) {
	field, ok := modelType.FieldByName(fieldName)
	if !ok {
		return fieldName, false
	}
	tag2, ok2 := field.Tag.Lookup("gorm")
	if !ok2 {
		return "", true
	}

	if has := strings.Contains(tag2, "column"); has {
		str1 := strings.Split(tag2, ";")
		num := len(str1)
		for i := 0; i < num; i++ {
			str2 := strings.Split(str1[i], ":")
			for j := 0; j < len(str2); j++ {
				if str2[j] == "column" {
					return str2[j+1], true
				}
			}
		}
	}
	//return gorm.ToColumnName(fieldName), false
	return fieldName, false
}
func getColumnsSelect(modelType reflect.Type) []string {
	numField := modelType.NumField()
	columnNameKeys := make([]string, 0)
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		ormTag := field.Tag.Get("gorm")
		if has := strings.Contains(ormTag, "column"); has {
			str1 := strings.Split(ormTag, ";")
			num := len(str1)
			for i := 0; i < num; i++ {
				str2 := strings.Split(str1[i], ":")
				for j := 0; j < len(str2); j++ {
					if str2[j] == "column" {
						columnName := str2[j+1]
						columnNameTag := getColumnNameFromSqlBuilderTag(field)
						if columnNameTag != nil {
							columnName = *columnNameTag
						}
						columnNameKeys = append(columnNameKeys, columnName)
					}
				}
			}
		}
	}
	return columnNameKeys
}
func buildSort(sortString string, modelType reflect.Type) string {
	var sort = make([]string, 0)
	sorts := strings.Split(sortString, ",")
	for i := 0; i < len(sorts); i++ {
		sortField := strings.TrimSpace(sorts[i])
		fieldName := sortField
		c := sortField[0:1]
		if c == "-" || c == "+" {
			fieldName = sortField[1:]
		}
		columnName := getColumnNameForSearch(modelType, fieldName)
		sortType := getSortType(c)
		sort = append(sort, columnName+" "+sortType)
	}
	return ` order by ` + strings.Join(sort, ",")
}
func getColumnNameForSearch(modelType reflect.Type, sortField string) string {
	sortField = strings.TrimSpace(sortField)
	i, _, column := getFieldByJson(modelType, sortField)
	if i > -1 {
		return column
	}
	return sortField // injection
}
func getSortType(sortType string) string {
	if sortType == "-" {
		return desc
	} else {
		return asc
	}
}

func getDriver(db *sql.DB) string {
	if db == nil {
		return driverNotSupport
	}
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return driverPostgres
	case "*godror.drv":
		return driverOracle
	case "*mysql.MySQLDriver":
		return driverMysql
	case "*mssql.Driver":
		return driverMssql
	case "*sqlite3.SQLiteDriver":
		return driverSqlite3
	default:
		return driverNotSupport
	}
}
func buildParam(i int) string {
	return "?"
}
func buildOracleParam(i int) string {
	return ":val" + strconv.Itoa(i)
}
func buildMsSqlParam(i int) string {
	return "@p" + strconv.Itoa(i)
}
func buildDollarParam(i int) string {
	return "$" + strconv.Itoa(i)
}
func getBuild(db *sql.DB) func(i int) string {
	driver := reflect.TypeOf(db.Driver()).String()
	switch driver {
	case "*pq.Driver":
		return buildDollarParam
	case "*godror.drv":
		return buildOracleParam
	case "*mssql.Driver":
		return buildMsSqlParam
	default:
		return buildParam
	}
}
func buildParametersFrom(i int, numCol int, buildParam func(i int) string) string {
	var arrValue []string
	for j := 0; j < numCol; j++ {
		arrValue = append(arrValue, buildParam(i+j+1))
	}
	return strings.Join(arrValue, ",")
}
