package gopan

import (
	"errors"
	"fmt"
	"reflect"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

type Gopan struct {
	Context     context.Context
	AdminClient *database.DatabaseAdminClient
	Client      *spanner.Client

	ProjectID string
	Instance  string
	DBName    string
}

func FromContext(c context.Context) *Gopan {
	// TODO その他の値の設定
	return &Gopan{
		Context: c,
	}
}

func (g *Gopan) CreateDB(srcs ...interface{}) error {
	var tableDDLs []string
	for _, src := range srcs {
		v := reflect.Indirect(reflect.ValueOf(src))
		extruder, err := getExtruder(v.Type())
		if err != nil {
			return err
		}

		tableDDLs = append(tableDDLs, extruder.DDLCreateTable())
	}

	op, err := g.AdminClient.CreateDatabase(g.Context, &adminpb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", g.ProjectID, g.Instance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", g.DBName),
		ExtraStatements: tableDDLs,
	})
	if err != nil {
		return err
	}

	_, err = op.Wait(g.Context)
	if err != nil {
		return err
	}
	return nil
}

// TODO DBの設定(Namespace的な)が必要 context.Context経由

func (g *Gopan) Key(src interface{}) *Key {
	key, err := g.KeyError(src)
	if err == nil {
		return key
	}
	return nil
}

// Deprecated: use Table instead.
func (g *Gopan) Kind(src interface{}) string {
	return g.Table(src)
}

func (g *Gopan) Table(src interface{}) string {
	return reflect.Indirect(reflect.ValueOf(src)).Type().Name()
}

func (g *Gopan) KeyError(src interface{}) (*Key, error) {
	v := reflect.Indirect(reflect.ValueOf(src))
	ex, err := getExtruder(v.Type())
	if err != nil {
		return nil, err
	}
	return ex.KeyError(v)
}

// TODO func (g *Gopan) RunInTransaction(

// Deprecated: use Insert or Update or InsertOrUpdate instead.
func (g *Gopan) Put(src interface{}) (*Key, error) {
	keys, err := g.PutMulti([]interface{}{src})
	if err != nil {
		// TODO
		return nil, err
	}
	return keys[0], err
}

// Deprecated: use InsertMulti or UpdateMulti or InsertOrUpdateMulti instead.
func (g *Gopan) PutMulti(src interface{}) ([]*Key, error) {
	return g.InsertMulti(src)
}

func (g *Gopan) Insert(src interface{}) (*Key, error) {
	keys, err := g.InsertMulti([]interface{}{src})
	if err != nil {
		// TODO
		return nil, err
	}
	return keys[0], err
}

func (g *Gopan) InsertMulti(src interface{}) ([]*Key, error) {
	return g.execMutationMulti(src, spanner.InsertStruct)
}

func (g *Gopan) Update(src interface{}) (*Key, error) {
	keys, err := g.UpdateMulti([]interface{}{src})
	if err != nil {
		// TODO
		return nil, err
	}
	return keys[0], err
}

func (g *Gopan) UpdateMulti(src interface{}) ([]*Key, error) {
	return g.execMutationMulti(src, spanner.UpdateStruct)
}

func (g *Gopan) InsertOrUpdate(src interface{}) (*Key, error) {
	keys, err := g.UpdateMulti([]interface{}{src})
	if err != nil {
		// TODO
		return nil, err
	}
	return keys[0], err
}

func (g *Gopan) InsertOrUpdateMulti(src interface{}) ([]*Key, error) {
	return g.execMutationMulti(src, spanner.InsertOrUpdateStruct)
}

func (g *Gopan) execMutationMulti(src interface{}, f func(table string, in interface{}) (*spanner.Mutation, error)) ([]*Key, error) {
	vs := reflect.Indirect(reflect.ValueOf(src))
	if k := vs.Type().Kind(); k != reflect.Slice && k != reflect.Array {
		return nil, fmt.Errorf("unsupported type: %s", k.String())
	}
	muts := make([]*spanner.Mutation, 0, vs.Len())
	for i := 0; i < vs.Len(); i++ {
		v := vs.Index(i)
		mut, err := f(g.Table(v.Interface()), v.Interface())
		if err != nil {
			return nil, err
		}
		muts = append(muts, mut)
	}

	_, err := g.Client.Apply(g.Context, muts)
	if err != nil {
		return nil, err
	}

	// TODO
	keys := make([]*Key, vs.Len())
	return keys, nil
}

func (g *Gopan) Get(dst interface{}) error {
	return g.GetMulti([]interface{}{dst})
}

func (g *Gopan) GetMulti(dst interface{}) error {
	vs := reflect.Indirect(reflect.ValueOf(dst))
	if k := vs.Type().Kind(); k != reflect.Slice && k != reflect.Array {
		return fmt.Errorf("unsupported type: %s", k.String())
	}

	var ex *extruder
	var keys []spanner.Key
	for i := 0; i < vs.Len(); i++ {
		v := reflect.ValueOf(vs.Index(i).Interface()) // remove Kind=Interface
		extruder, err := getExtruder(v.Type())
		if err != nil {
			return err
		}
		if i == 0 {
			ex = extruder
		} else if ex.Table() != extruder.Table() {
			return errors.New("") // TODO
		}

		key, err := extruder.KeyError(v)
		if err != nil {
			return err
		}
		keys = append(keys, key.Key)
	}
	keySet := spanner.Keys(keys...)
	iter := g.Client.Single().Read(g.Context, ex.Table(), keySet, ex.Columns())
	defer iter.Stop()
	for i := 0; ; i++ {
		row, err := iter.Next()
		if err == iterator.Done {
			return nil
		} else if err != nil {
			return err
		}
		err = row.ToStruct(vs.Index(i).Interface())
		if err != nil {
			return err
		}
	}
}

func (g *Gopan) Delete(key *Key) error {
	return g.DeleteMulti([]*Key{key})
}

func (g *Gopan) DeleteMulti(keys []*Key) error {
	muts := make([]*spanner.Mutation, 0, len(keys))
	for _, key := range keys {
		mut := spanner.Delete(key.Table, key.Key)
		muts = append(muts, mut)
	}

	_, err := g.Client.Apply(g.Context, muts)
	if err != nil {
		return err
	}

	return nil
}
