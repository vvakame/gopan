package gopan

import "cloud.google.com/go/spanner"

// NOTE
// spanner.Key does not have Table information.

type Key struct {
	Table string
	spanner.Key
}
