package gopan

import (
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
)

func String(v string) spanner.NullString {
	return spanner.NullString{Valid: true, StringVal: v}
}

func Time(v time.Time) spanner.NullTime {
	if v.IsZero() {
		return spanner.NullTime{}
	}
	return spanner.NullTime{Valid: true, Time: v}
}

func Float64(v float64) spanner.NullFloat64 {
	return spanner.NullFloat64{Valid: true, Float64: v}
}

func Int64(v int64) spanner.NullInt64 {
	return spanner.NullInt64{Valid: true, Int64: v}
}

func Date(v civil.Date) spanner.NullDate {
	return spanner.NullDate{Valid: true, Date: v}
}

func Bool(v bool) spanner.NullBool {
	return spanner.NullBool{Valid: true, Bool: v}
}
