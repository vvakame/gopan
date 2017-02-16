package gopan

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
)

var typeMap = make(map[reflect.Type]*extruder)
var mu sync.Mutex

type extruder struct {
	Type     reflect.Type
	Fields   []*extruderField
	PKFields []*extruderField
}

type extruderField struct {
	ColumnIndex    int
	ReflectIndexes []int
	Name           string
	ColumnType     string
	PK             bool
	Length         int
	NotNull        bool
}

func getExtruder(t reflect.Type) (*extruder, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, errors.New("src is not struct")
	}

	mu.Lock()
	defer mu.Unlock()

	ex, ok := typeMap[t]
	if ok {
		return ex, nil
	}

	ex = &extruder{Type: t}
	typeMap[t] = ex

	var f func(t reflect.Type, refIndexes []int) error
	columnIdx := 0
	f = func(t reflect.Type, refIndexes []int) error {
		for i := 0; i < t.NumField(); i++ {
			tf := t.Field(i)
			exported := tf.PkgPath == ""
			if !exported && !tf.Anonymous {
				continue
			}

			tag := tf.Tag.Get("spanner")
			if tag == "-" {
				continue
			}

			if tf.Anonymous {
				nextT := tf.Type
				if nextT.Kind() == reflect.Ptr {
					nextT = nextT.Elem()
				}
				nextRefIndexes := make([]int, 0, len(refIndexes)+1)
				nextRefIndexes = append(nextRefIndexes, refIndexes...)
				nextRefIndexes = append(nextRefIndexes, i)
				err := f(nextT, nextRefIndexes)
				if err != nil {
					return err
				}
				continue
			}

			exF := &extruderField{ColumnIndex: columnIdx}
			ex.Fields = append(ex.Fields, exF)
			nextRefIndexes := make([]int, 0, len(refIndexes)+1)
			nextRefIndexes = append(nextRefIndexes, refIndexes...)
			nextRefIndexes = append(nextRefIndexes, i)
			exF.ReflectIndexes = nextRefIndexes
			columnIdx++

			exF.Name = tf.Name

			tag = tf.Tag.Get("gopan")
			if tag == "id" {
				exF.PK = true
			}

			switch tf.Type.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				exF.ColumnType = "INT64"
			case reflect.String:
				exF.ColumnType = "STRING"
			default:
				switch tf.Type {
				case reflect.TypeOf(time.Time{}):
					exF.ColumnType = "TIMESTAMP"
				default:
					return fmt.Errorf("unsupported type: %s", tf.Type)
				}
			}

			// TODO Length, NotNull
		}

		return nil
	}
	err := f(t, nil)
	if err != nil {
		return nil, err
	}

	for _, f := range ex.Fields {
		if !f.PK {
			continue
		}
		ex.PKFields = append(ex.PKFields, f)
	}

	return ex, nil
}

func (ex *extruder) Table() string {
	return ex.Type.Name()
}

func (ex *extruder) KeyError(v reflect.Value) (*Key, error) {
	var key spanner.Key
	for _, f := range ex.PKFields {
		key = append(key, f.Value(v).Interface())
	}

	return &Key{Table: ex.Table(), Key: key}, nil
}

func (ex *extruder) Columns() []string {
	columns := make([]string, 0, len(ex.Fields))
	for _, f := range ex.Fields {
		columns = append(columns, f.Name)
	}

	return columns
}

func (f *extruderField) Value(st reflect.Value) reflect.Value {
	if st.Kind() == reflect.Ptr {
		st = st.Elem()
	}
	v := st
	for _, idx := range f.ReflectIndexes {
		v = v.Field(idx)
	}
	return v
}

func (ex *extruder) DDLCreateTable() string {
	buf := bytes.NewBufferString("CREATE TABLE ")
	buf.WriteString(ex.Table())
	buf.WriteString(" (\n")
	for _, f := range ex.Fields {
		buf.WriteString("\t")
		buf.WriteString(f.DDLColumn())
		buf.WriteString(",\n")
	}
	buf.WriteString(") PRIMARY KEY (")
	var pkNames []string
	for _, f := range ex.PKFields {
		pkNames = append(pkNames, f.Name)
	}
	buf.WriteString(strings.Join(pkNames, ", "))
	buf.WriteString(")")

	return buf.String()
}

func (f *extruderField) DDLColumn() string {
	buf := bytes.NewBufferString("")
	buf.WriteString(f.Name)
	buf.WriteString("\t")
	buf.WriteString(f.ColumnType)
	if f.Length != 0 {
		buf.WriteString("\t")
		buf.WriteString("(")
		buf.WriteString(fmt.Sprintf("%d", f.Length))
		buf.WriteString(")")
	} else {
		switch f.ColumnType {
		case "STRING":
			buf.WriteString("\t")
			buf.WriteString("(MAX)")
		}
	}
	if f.NotNull {
		buf.WriteString("\t")
		buf.WriteString("NOT NULL")
	}

	return buf.String()
}
