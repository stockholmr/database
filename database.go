package database

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"time"
)

func Insert(db *sqlx.DB, table string, fields []*FieldValuePair) (*sql.NullInt64, error) {
	cmd, values := buildInsertCommand(table, fields)

	stmt, err := db.Preparex(cmd)
	if err != nil {
		return nil, err
	}

	result, err := stmt.Exec(values...)
	if err != nil {
		return nil, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	err = stmt.Close()
	if err != nil {
		return nil, err
	}

	return &sql.NullInt64{Int64: id, Valid: true}, nil
}
func Update(db *sqlx.DB, table string, fields []*FieldValuePair, where *FieldValuePair) error {
	cmd, values := buildUpdateCommand(table, fields, where)

	stmt, err := db.Preparex(cmd)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(values...)
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	return nil
}
func Delete(db *sqlx.DB, table string, where *FieldValuePair) error {
	cmd, values := buildDeleteCommand(table, where)

	stmt, err := db.Preparex(cmd)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(values...)
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	return nil
}
func Select(db *sqlx.DB, query string, values []interface{}) ([]*DataRow, error) {
	stmt, err := db.Preparex(query)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Queryx(values...)
	if err != nil {
		return nil, err
	}

	items := make([]*DataRow, 0)
	for rows.Next() {
		row := NewDataRow()
		err = rows.MapScan(row.Data)
		if err != nil {
			return nil, err
		}
		items = append(items, row)
	}

	err = rows.Close()
	if err != nil {
		return nil, err
	}

	err = stmt.Close()
	if err != nil {
		return nil, err
	}

	return items, nil
}
func SelectRow(db *sqlx.DB, query string, values []interface{}) (*DataRow, error) {
	stmt, err := db.Preparex(query)
	if err != nil {
		return nil, err
	}

	result := stmt.QueryRowx(values...)

	row := NewDataRow()
	err = result.MapScan(row.Data)

	if err != nil {
		return nil, err
	}

	err = stmt.Close()
	if err != nil {
		return nil, err
	}

	return row, nil
}

func buildInsertCommand(table string, fields []*FieldValuePair) (string, []interface{}) {
	var orderedValues = make([]interface{}, 0)
	cmd := fmt.Sprintf("INSERT INTO `%s`(", table)
	placeholders := ""

	for i, o := range fields {
		cmd += fmt.Sprintf("`%s`", o.Key)
		placeholders += "?"
		if i < len(fields)-1 {
			cmd += ", "
			placeholders += ", "
		}
		orderedValues = append(orderedValues, o.Value)
	}
	cmd += ") VALUES (" + placeholders + ");"
	return cmd, orderedValues
}
func buildUpdateCommand(table string, fields []*FieldValuePair, where *FieldValuePair) (string, []interface{}) {
	var orderedValues = make([]interface{}, 0)
	cmd := fmt.Sprintf("UPDATE `%s` SET ", table)

	for i, o := range fields {
		cmd += fmt.Sprintf("`%s`=?", o.Key)
		if i < len(fields)-1 {
			cmd += ", "
		}
		orderedValues = append(orderedValues, o.Value)
	}

	cmd += fmt.Sprintf(" WHERE `%s`=?", where.Key)
	orderedValues = append(orderedValues, where.Value)
	return cmd, orderedValues
}
func buildDeleteCommand(table string, where *FieldValuePair) (string, []interface{}) {
	var orderedValues = make([]interface{}, 0)
	cmd := fmt.Sprintf("DELETE FROM `%s` WHERE `%s`=?", table, where.Key)
	orderedValues = append(orderedValues, where.Value)
	return cmd, orderedValues
}

// ============================================================
//  Row
// ============================================================

type DataRow struct {
	Data map[string]interface{}
}

func NewDataRow() *DataRow {
	return &DataRow{
		Data: make(map[string]interface{}),
	}
}
func (r *DataRow) getItem(key string) (interface{}, error) {
	if v, ok := r.Data[key]; ok {
		return v, nil
	}
	return nil, errors.New("invalid key")
}
func (r *DataRow) Int(key string) *sql.NullInt64 {
	value, err := r.getItem(key)
	if err != nil {
		return &sql.NullInt64{}
	}
	return _int(value)
}
func (r *DataRow) String(key string) *sql.NullString {
	value, err := r.getItem(key)
	if err != nil {
		return &sql.NullString{}
	}
	return _string(value)
}
func (r *DataRow) Float(key string) *sql.NullFloat64 {
	value, err := r.getItem(key)
	if err != nil {
		return &sql.NullFloat64{}
	}
	return _float(value)
}
func (r *DataRow) Bool(key string) *sql.NullBool {
	value, err := r.getItem(key)
	if err != nil {
		return &sql.NullBool{}
	}
	return _bool(value)
}
func (r *DataRow) BoolFromInt(key string) *sql.NullBool {
	value := r.Int(key)
	if !value.Valid {
		return &sql.NullBool{}
	}
	return _boolFromInt(value)
}
func (r *DataRow) TimeFromUnix(key string) *sql.NullTime {
	intValue := r.Int(key)
	if !intValue.Valid {
		return &sql.NullTime{}
	}
	return _timeFromUnix(intValue)
}
func (r *DataRow) TimeFromRFC3339String(key string) *sql.NullTime {
	strValue := r.String(key)
	if !strValue.Valid {
		return &sql.NullTime{}
	}
	return _timeFromRFC3339String(strValue)
}

// ============================================================
//  FieldValuePair
// ============================================================

type FieldValuePair struct {
	Key   string
	Value interface{}
}

func NewFieldValuePairCollection(items ...*FieldValuePair) []*FieldValuePair {
	return items
}
func NewFieldValuePair(key string, value interface{}) *FieldValuePair {
	return &FieldValuePair{
		Key:   key,
		Value: value,
	}
}
func (f *FieldValuePair) Int() *sql.NullInt64 {
	return _int(f.Value)
}
func (f *FieldValuePair) String() *sql.NullString {
	return _string(f.Value)
}
func (f *FieldValuePair) Float() *sql.NullFloat64 {
	return _float(f.Value)
}
func (f *FieldValuePair) Bool() *sql.NullBool {
	return _bool(f.Value)
}
func (f *FieldValuePair) BoolFromInt() *sql.NullBool {
	value := f.Int()
	if !value.Valid {
		return &sql.NullBool{}
	}
	return _boolFromInt(value)
}
func (f *FieldValuePair) Time() *sql.NullTime {
	return _time(f.Value)
}
func (f *FieldValuePair) TimeFromUnix() *sql.NullTime {
	intValue := f.Int()
	if !intValue.Valid {
		return &sql.NullTime{}
	}

	return _timeFromUnix(intValue)
}
func (f *FieldValuePair) TimeFromRFC3339String() *sql.NullTime {
	strValue := f.String()
	if !strValue.Valid {
		return &sql.NullTime{}
	}
	return _timeFromRFC3339String(strValue)
}

func _int(value interface{}) *sql.NullInt64 {
	switch v := value.(type) {
	case int:
		return &sql.NullInt64{Int64: int64(v), Valid: true}
	case int64:
		return &sql.NullInt64{Int64: v, Valid: true}
	case int32:
		return &sql.NullInt64{Int64: int64(v), Valid: true}
	default:
		return &sql.NullInt64{}
	}
}
func _string(value interface{}) *sql.NullString {
	if v, ok := value.(string); ok {
		return &sql.NullString{String: v, Valid: true}
	}
	return &sql.NullString{}
}
func _float(value interface{}) *sql.NullFloat64 {
	switch v := value.(type) {
	case float32:
		return &sql.NullFloat64{Float64: float64(v), Valid: true}
	case float64:
		return &sql.NullFloat64{Float64: v, Valid: true}
	default:
		return &sql.NullFloat64{}
	}
}
func _bool(value interface{}) *sql.NullBool {
	if v, ok := value.(bool); ok {
		return &sql.NullBool{Bool: v, Valid: true}
	}
	return &sql.NullBool{}
}
func _boolFromInt(value *sql.NullInt64) *sql.NullBool {
	if value.Int64 == 1 {
		return &sql.NullBool{Bool: true, Valid: true}
	}
	return &sql.NullBool{Bool: false, Valid: true}
}
func _time(value interface{}) *sql.NullTime {
	if v, ok := value.(time.Time); ok {
		return &sql.NullTime{Time: v, Valid: true}
	}
	return &sql.NullTime{}
}
func _timeFromUnix(value *sql.NullInt64) *sql.NullTime {
	unixTime := time.Unix(value.Int64, 0)
	return &sql.NullTime{Time: unixTime, Valid: true}
}
func _timeFromRFC3339String(value *sql.NullString) *sql.NullTime {
	dt, err := time.Parse(time.RFC3339, value.String)
	if err != nil {
		return &sql.NullTime{}
	}
	return &sql.NullTime{Time: dt, Valid: true}
}
