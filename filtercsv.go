package filtercsv

import (
	"encoding/csv"
	"errors"
	"io"
)

type fieldError string

func (f fieldError) Error() string {
	return "no column named " + string(f)
}

type Row struct {
	fieldIdx map[string]int
	fields   []string
	copied   bool
}

func (r *Row) Field(name string) string {
	idx, ok := r.fieldIdx[name]
	if !ok {
		panic(fieldError(name))
	}
	return r.fields[idx]
}

func (r *Row) SetField(name, value string) {
	if !r.copied {
		// Make a copy to avoid overwriting reused slices.
		r.fields = append([]string(nil), r.fields...)
		r.copied = true
	}
	idx, ok := r.fieldIdx[name]
	if !ok {
		panic(fieldError(name))
	}
	r.fields[idx] = value
}

type Config struct {
	KeepCol   func(name string) bool
	KeepRow   func(r *Row) bool
	ModifyRow func(r *Row)
}

func Process(r *csv.Reader, w *csv.Writer, cfg *Config) (err error) {
	if cfg.KeepCol == nil {
		cfg.KeepCol = func(string) bool { return true }
	}
	if cfg.KeepRow == nil {
		cfg.KeepRow = func(*Row) bool { return true }
	}
	if cfg.ModifyRow == nil {
		cfg.ModifyRow = func(*Row) {}
	}

	defer func() {
		r := recover()
		switch r := r.(type) {
		case fieldError:
			err = r
		case nil:
		default:
			panic(r)
		}
	}()

	rr := NewReader(r)
	var keepIdx []bool
	for first := true; ; first = false {
		row, err := rr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if first {
			// one-time init
			keepIdx = make([]bool, len(row.fields))
			for idx, name := range rr.Header {
				keepIdx[idx] = cfg.KeepCol(name)
			}
			if err := w.Write(trim(rr.Header, keepIdx)); err != nil {
				return err
			}
		}
		if !cfg.KeepRow(row) {
			continue
		}
		cfg.ModifyRow(row)
		if err := w.Write(trim(row.fields, keepIdx)); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

func trim(s []string, keep []bool) []string {
	var out []string
	for i, v := range s {
		if keep[i] {
			out = append(out, v)
		}
	}
	return out
}
