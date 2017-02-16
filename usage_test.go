package gopan

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
)

type Article struct {
	ID        int64 `gopan:"id"`
	Title     string
	Body      string
	Authors   []spanner.NullString
	CreatedAt time.Time
	UpdatedAt time.Time
}

func TestGopan_CreateDB(t *testing.T) {
	g, err := makeDefaultGopan(context.Background())
	if err != nil {
		t.Fatal(err.Error())
	}

	err = g.CreateDB(&Article{})
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestGopan_InsertMulti(t *testing.T) {
	// depends on result of TestGopan_CreateDB

	g, err := makeDefaultGopan(context.Background())
	if err != nil {
		t.Fatal(err.Error())
	}

	var arts []*Article
	for i := 1; i <= 100000; i++ {
		art := &Article{
			ID:        int64(i),
			Title:     fmt.Sprintf("Title %d", i),
			Body:      fmt.Sprintf("Body %d", i),
			Authors:   []spanner.NullString{String("vvakame"), String("cat")},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		arts = append(arts, art)
		if len(arts) == 2000 {
			_, err = g.InsertMulti(arts)
			if err != nil {
				t.Fatal(err.Error())
			}
			arts = nil
		}
	}
	if len(arts) != 0 {
		_, err = g.InsertMulti(arts)
		if err != nil {
			t.Fatal(err.Error())
		}
	}
}

func TestGopan_Get(t *testing.T) {
	g, err := makeDefaultGopan(context.Background())
	if err != nil {
		t.Fatal(err.Error())
	}

	{ // Before
		key := g.Key(&Article{ID: 300})
		err := g.Delete(key)
		if err != nil {
			t.Log(err)
		}
	}

	art := &Article{
		ID:        300,
		Title:     "Title TestGopan_GetMulti",
		Body:      "Body TestGopan_GetMulti",
		Authors:   []spanner.NullString{String("vvakame"), String("maya")},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	_, err = g.Insert(art)
	if err != nil {
		t.Fatal(err)
	}

	{
		ex, err := getExtruder(reflect.TypeOf(Article{}))
		if err != nil {
			t.Fatal(err)
		}
		keySet := spanner.Keys(g.Key(art).Key)
		iter := g.client.Single().Read(g.Context, ex.Table(), keySet, ex.Columns())
		defer iter.Stop()
		for i := 0; ; i++ {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			} else if err != nil {
				if err != nil {
					t.Fatal(err)
				}
			}
			err = row.ToStruct(&Article{})
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	newArt := &Article{ID: 300}
	err = g.Get(newArt)
	if err != nil {
		t.Fatal(err)
	}

	if v := newArt.Title; v != art.Title {
		t.Error("unexpected", v)
	}
	if v := newArt.Body; v != art.Body {
		t.Error("unexpected", v)
	}
}

func TestSpanner_AutoInc(t *testing.T) {
	t.SkipNow() // Result: Spanner does not have AUTOINCREMENT like feature.

	g, err := makeDefaultGopan(context.Background())
	if err != nil {
		t.Fatal(err.Error())
	}

	columns := []string{"Title", "Body", "CreatedAt", "UpdatedAt"}
	m := []*spanner.Mutation{
		spanner.Insert("Article", columns, []interface{}{"Title", "Body", time.Now(), time.Now()}),
	}

	g.client.Apply(g.Context, m)
}

func makeDefaultGopan(c context.Context) (*Gopan, error) {
	g := FromContext(context.Background())
	g.ProjectID = os.Getenv("GCLOUD_PROJECT_ID")
	g.Instance = os.Getenv("GCLOUD_SPANNER_INSTANCE")
	g.DBName = os.Getenv("GCLOUD_SPANNER_DBNAME")

	adminClient, err := database.NewDatabaseAdminClient(context.Background())
	if err != nil {
		return nil, err
	}
	g.adminClient = adminClient

	client, err := spanner.NewClient(c, fmt.Sprintf("projects/%s/instances/%s/databases/%s", g.ProjectID, g.Instance, g.DBName))
	if err != nil {
		return nil, err
	}
	g.client = client

	return g, nil
}
