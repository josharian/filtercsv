package filtercsv

import (
	"encoding/csv"
	"fmt"
)

// A Reader reads a CSV that has a header row,
// yielding rows that can be queried by header title.
// It doesn't really semantically belong in something called "filtercsv". Oh well.
type Reader struct {
	r          *csv.Reader
	Header     []string // populated during first call to Read
	fieldIdx   map[string]int
	seenheader bool
}

// NewReader returns a *Reader that wraps r.
func NewReader(r *csv.Reader) *Reader {
	return &Reader{r: r, fieldIdx: make(map[string]int)}
}

// Read returns a single row from the CSV.
// It does not return a separate header row.
// If you want to do something with the header row,
// after the first call to read, use r.Header.
func (r *Reader) Read() (*Row, error) {
	e, err := r.r.Read()
	if err != nil {
		return nil, err
	}
	if !r.seenheader {
		r.seenheader = true
		// header line: parse field names
		for idx, name := range e {
			r.fieldIdx[name] = idx
		}
		if len(r.fieldIdx) != len(e) {
			return nil, fmt.Errorf("duplicate column names")
		}
		r.Header = append([]string(nil), e...)
		// cheap hack: recurse once to get next (non-header) row
		return r.Read()
	}
	return &Row{fieldIdx: r.fieldIdx, fields: e}, nil
}
