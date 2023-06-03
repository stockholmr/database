package database

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"testing"
	"time"
)

var db *sqlx.DB

func setup() error {
	db = sqlx.MustConnect("sqlite3", ":memory:")

	cmd := `CREATE TABLE "testing" (
		"id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
		"name" TEXT NOT NULL,
		"value" TEXT NOT NULL,
		"status" INTEGER NOT NULL DEFAULT "0");
INSERT INTO testing(name, value, status) VALUES ('John Doe', 'sdsdgsd9u2rolj2r9wuyf', 1);
INSERT INTO testing(name, value, status) VALUES ('John Doe2', 'sdsdgsd9u2rolj2r9wuyf', 0);
INSERT INTO testing(name, value, status) VALUES ('John Doe3', 'sdsdgsd9u2rolj2r9wuyf', 55);`

	_, err := db.Exec(cmd)
	if err != nil {
		return err
	}
	return nil
}

func TestInsert(t *testing.T) {
	err := setup()
	if err != nil {
		t.Error(err)
	}

	id, err := Insert(
		db,
		"testing",
		NewFieldValuePairCollection(
			NewFieldValuePair("name", "Jane Doe"),
			NewFieldValuePair("value", "sdgihsighsdighsidhgushgf"),
			NewFieldValuePair("status", "1"),
		),
	)

	if err != nil {
		t.Error(err)
	}

	if id.Int64 != 4 {
		t.FailNow()
	}

	err = db.Close()
	if err != nil {
		t.Error(err)
	}
}
func TestUpdate(t *testing.T) {
	err := setup()
	if err != nil {
		t.Error(err)
	}

	err = Update(
		db,
		"testing",
		NewFieldValuePairCollection(
			NewFieldValuePair("name", "New Value Test"),
		),
		NewFieldValuePair("id", 1),
	)

	if err != nil {
		t.Error(err)
	}

	err = db.Close()
	if err != nil {
		t.Error(err)
	}
}
func TestSelect(t *testing.T) {
	err := setup()
	if err != nil {
		t.Error(err)
	}

	rows, err := Select(
		db,
		"SELECT * FROM testing",
		nil,
	)

	if err != nil {
		t.Error(err)
	}

	if len(rows) != 3 {
		t.FailNow()
	}

	err = db.Close()
	if err != nil {
		t.Error(err)
	}
}
func TestSelectRow(t *testing.T) {
	err := setup()
	if err != nil {
		t.Error(err)
	}

	row, err := SelectRow(
		db,
		"SELECT * FROM testing",
		nil,
	)

	if err != nil {
		t.Error(err)
	}

	if row.Int("id").Int64 != 1 {
		t.FailNow()
	}

	err = db.Close()
	if err != nil {
		t.Error(err)
	}
}
func TestDelete(t *testing.T) {
	err := setup()
	if err != nil {
		t.Error(err)
	}

	err = Delete(
		db,
		"testing",
		NewFieldValuePair("id", 1),
	)

	if err != nil {
		t.Error(err)
	}

	_, err = SelectRow(
		db,
		"SELECT * FROM testing WHERE id=?",
		[]interface{}{1},
	)

	if err != nil {
		if err != sql.ErrNoRows {
			t.Error(err)
		}
	}
}
func TestFieldValuePair_String(t *testing.T) {
	td := FieldValuePair{
		Key:   "name",
		Value: "John Doe",
	}

	result := td.String()
	if !result.Valid {
		t.FailNow()
	}
}
func TestFieldValuePair_Int(t *testing.T) {
	td := FieldValuePair{
		Key:   "value",
		Value: 24357834,
	}

	result := td.Int()
	if !result.Valid {
		t.FailNow()
	}
}
func TestFieldValuePair_Bool(t *testing.T) {
	td := FieldValuePair{
		Key:   "value",
		Value: true,
	}

	result := td.Bool()
	if !result.Valid {
		t.FailNow()
	}
}
func TestFieldValuePair_BoolFromInt(t *testing.T) {
	td := FieldValuePair{
		Key:   "value",
		Value: 1,
	}

	result := td.BoolFromInt()
	if !result.Valid && result.Bool {
		t.FailNow()
	}
}
func TestFieldValuePair_Float(t *testing.T) {
	td := FieldValuePair{
		Key:   "value",
		Value: 555.555,
	}

	result := td.Float()
	if !result.Valid {
		t.FailNow()
	}
}
func TestFieldValuePair_Time(t *testing.T) {
	td := FieldValuePair{
		Key:   "value",
		Value: time.Now(),
	}

	result := td.Time()
	if !result.Valid {
		t.FailNow()
	}
}
func TestFieldValuePair_TimeFromUnix(t *testing.T) {
	td := FieldValuePair{
		Key:   "value",
		Value: time.Now().Unix(),
	}

	result := td.TimeFromUnix()
	if !result.Valid {
		t.FailNow()
	}
}
func TestFieldValuePair_TimeFromRFC3339String(t *testing.T) {
	td := FieldValuePair{
		Key:   "value",
		Value: time.Now().Format(time.RFC3339),
	}

	result := td.TimeFromRFC3339String()
	if !result.Valid {
		t.FailNow()
	}
}
func TestRow_String(t *testing.T) {
	r := NewDataRow()
	r.Data["bool"] = true
	r.Data["string"] = "string"
	r.Data["float"] = 444.55
	r.Data["int"] = 3345353
	r.Data["time"] = time.Now().Unix()

	result := r.String("string")
	if !result.Valid {
		t.FailNow()
	}
}
func TestRow_Int(t *testing.T) {
	r := NewDataRow()
	r.Data["bool"] = true
	r.Data["string"] = "string"
	r.Data["float"] = 444.55
	r.Data["int"] = 3345353
	r.Data["time"] = time.Now().Unix()

	result := r.Int("int")
	if !result.Valid {
		t.FailNow()
	}
}
func TestRow_Float(t *testing.T) {
	r := NewDataRow()
	r.Data["bool"] = true
	r.Data["string"] = "string"
	r.Data["float"] = 444.55
	r.Data["int"] = 3345353
	r.Data["time"] = time.Now().Unix()

	result := r.Float("float")
	if !result.Valid {
		t.FailNow()
	}
}
func TestRow_Bool(t *testing.T) {
	r := NewDataRow()
	r.Data["bool"] = true
	r.Data["string"] = "string"
	r.Data["float"] = 444.55
	r.Data["int"] = 3345353
	r.Data["time"] = time.Now().Unix()

	result := r.Bool("bool")
	if !result.Valid {
		t.FailNow()
	}
}
func TestRow_BoolFromInt(t *testing.T) {
	r := NewDataRow()
	r.Data["bool"] = 1
	r.Data["bool2"] = 0
	r.Data["string"] = "string"
	r.Data["float"] = 444.55
	r.Data["int"] = 3345353
	r.Data["time"] = time.Now().Unix()

	result := r.BoolFromInt("bool")
	if !result.Valid && result.Bool {
		t.FailNow()
	}

	result2 := r.BoolFromInt("bool2")
	if !result2.Valid && !result2.Bool {
		t.FailNow()
	}
}
func TestRow_TimeFromUnix(t *testing.T) {
	r := NewDataRow()
	r.Data["bool"] = true
	r.Data["string"] = "string"
	r.Data["float"] = 444.55
	r.Data["int"] = 3345353
	r.Data["time"] = time.Now().Unix()

	result := r.TimeFromUnix("time")
	if !result.Valid {
		t.FailNow()
	}
}
func TestRow_TimeFromRFC3339String(t *testing.T) {
	r := NewDataRow()
	r.Data["bool"] = true
	r.Data["string"] = "string"
	r.Data["float"] = 444.55
	r.Data["int"] = 3345353
	r.Data["time"] = time.Now().Unix()
	r.Data["time2"] = time.Now().Format(time.RFC3339)

	result := r.TimeFromRFC3339String("time2")
	if !result.Valid {
		t.FailNow()
	}
}
