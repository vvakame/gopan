package gopan

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/MakeNowJust/heredoc"
)

type Sample1 struct {
	ID int64 `gopan:"id"`
}

func TestExtruder_Table(t *testing.T) {
	v := reflect.ValueOf(&Sample1{}).Elem()
	ex, err := getExtruder(v.Type())
	if err != nil {
		t.Fatal(err)
	}

	if v := ex.Table(); v != "Sample1" {
		t.Error("unexpected", v)
	}
}

func TestExtruder_KeyError(t *testing.T) {
	v := reflect.ValueOf(&Sample1{ID: 7}).Elem()
	ex, err := getExtruder(v.Type())
	if err != nil {
		t.Fatal(err)
	}

	key, err := ex.KeyError(v)
	if err != nil {
		t.Fatal(err)
	}

	if v := key.Table; v != ex.Table() {
		t.Error("unexpected", v)
	}
	if v := reflect.DeepEqual(key.Key, spanner.Key{int64(7)}); !v {
		t.Error("unexpected", v)
	}
}

type Outer struct {
	ID   int64 `gopan:"id"`
	Name string
	Embed
}

type Embed struct {
	UpdatedAt time.Time
	CreatedAt time.Time
}

func TestExtruderField_Value(t *testing.T) {
	obj := &Outer{}
	v := reflect.ValueOf(obj).Elem()
	ex, err := getExtruder(v.Type())
	if err != nil {
		t.Fatal(err)
	}

	if v := len(ex.Fields); v != 4 {
		t.Fatal("unexpected", v)
	}

	id := ex.Fields[0]
	idV := id.Value(v)
	idV.SetInt(10)

	name := ex.Fields[1]
	nameV := name.Value(v)
	nameV.SetString("Test")

	updatedAt := ex.Fields[2]
	updatedAtV := updatedAt.Value(v)
	now := time.Now()
	updatedAtV.Set(reflect.ValueOf(now))

	createdAt := ex.Fields[3]
	createdAtV := createdAt.Value(v)
	createdAtV.Set(reflect.ValueOf(now.Add(10 * time.Minute)))

	if v := obj.ID; v != 10 {
		t.Fatal("unexpected", v)
	}
	if v := obj.Name; v != "Test" {
		t.Fatal("unexpected", v)
	}
	if v := obj.UpdatedAt; !v.Equal(now) {
		t.Fatal("unexpected", v)
	}
	if v := obj.CreatedAt; !v.Equal(now.Add(10 * time.Minute)) {
		t.Fatal("unexpected", v)
	}
}

func TestExtruder_Columns(t *testing.T) {
	v := reflect.ValueOf(&Sample1{}).Elem()
	ex, err := getExtruder(v.Type())
	if err != nil {
		t.Fatal(err)
	}

	columns := ex.Columns()
	if v := len(columns); v != 1 {
		t.Fatal("unexpected", v)
	}
	if v := columns[0]; v != "ID" {
		t.Fatal("unexpected", v)
	}
}

func TestExtruder_DDLCreateTable(t *testing.T) {
	v := reflect.ValueOf(&Sample1{}).Elem()
	ex, err := getExtruder(v.Type())
	if err != nil {
		t.Fatal(err)
	}

	ddl := ex.DDLCreateTable()
	expected := heredoc.Doc(`
		CREATE TABLE Sample1 (
			ID	INT64	NOT NULL,
		) PRIMARY KEY (ID)
	`)
	if v := strings.TrimSpace(ddl); v != strings.TrimSpace(expected) {
		t.Fatal("unexpected", v)
	}
}
