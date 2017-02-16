package gopan

import "cloud.google.com/go/spanner"

func String(str string) spanner.NullString {
	return spanner.NullString{Valid: true, StringVal: str}
}
