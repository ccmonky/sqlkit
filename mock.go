// The three clause BSD license (http://en.wikipedia.org/wiki/BSD_licenses)
//
// Copyright (c) 2013-2019, DATA-DOG team
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// * Redistributions of source code must retain the above copyright notice, this
//   list of conditions and the following disclaimer.
//
// * Redistributions in binary form must reproduce the above copyright notice,
//   this list of conditions and the following disclaimer in the documentation
//   and/or other materials provided with the distribution.
//
// * The name DataDog.lt may not be used to endorse or promote products
//   derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL MICHAEL BOSTOCK BE LIABLE FOR ANY DIRECT,
// INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
// OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
// EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
package sqlkit

import (
	"context"
	"database/sql/driver"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/pkg/errors"
)

type Mock struct {
	Name         string
	Playback     bool
	ExecReturns  *SyncMap[string, *Return[driver.Result]]
	QueryReturns *SyncMap[string, *Return[driver.Rows]]
}

func NewMock(opts ...MockOption) *Mock {
	mock := Mock{
		ExecReturns:  NewSyncMap[string, *Return[driver.Result]](),
		QueryReturns: NewSyncMap[string, *Return[driver.Rows]](),
	}
	for _, opt := range opts {
		opt(&mock)
	}
	return &mock
}

type MockOption func(*Mock)

func WithMockName(name string) MockOption {
	return func(mock *Mock) {
		mock.Name = name
	}
}

func WithMockPlayback(pb bool) MockOption {
	return func(mock *Mock) {
		mock.Playback = pb
	}
}

func WithMockExecReturns(m map[string]*Return[driver.Result]) MockOption {
	return func(mock *Mock) {
		for k, v := range m {
			mock.ExecReturns.Store(k, v)
		}
	}
}

func WithMockQueryReturns(m map[string]*Return[driver.Rows]) MockOption {
	return func(mock *Mock) {
		for k, v := range m {
			mock.QueryReturns.Store(k, v)
		}
	}
}

// Load load data from fixture
func (m *Mock) Load() error {
	if m.Name == "" {
		return nil
	}
	if m.Playback {
		// TODO: 从fixture.name.go加载结果
	}
	return nil
}

// Dump cache data to fixture
func (m *Mock) Dump() error {
	if m.Name == "" {
		return errors.New("can not dump for empty mock name")
	}
	if m.Playback {
		// TODO: fixture.name.go, 写入exec和query结果
	}
	return nil
}

func (m *Mock) AddExec(query string, ret *Return[driver.Result]) {
	m.ExecReturns.Store(query, ret)
}

func (m *Mock) AddQuery(query string, ret *Return[driver.Rows]) {
	m.QueryReturns.Store(query, ret)
}

func (m *Mock) ExecContext(next ExecContext) ExecContext {
	return func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
		if ret, ok := m.ExecReturns.Load(query); ok {
			return ret.Value, ret.Err
		}
		results, err := next(ctx, query, args)
		m.ExecReturns.Store(query, NewReturn(results, err))
		return results, err
	}
}

func (m *Mock) QueryContext(next QueryContext) QueryContext {
	return func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		if ret, ok := m.QueryReturns.Load(query); ok {
			return ret.Value, ret.Err
		}
		rows, err := next(ctx, query, args)
		m.QueryReturns.Store(query, NewReturn(rows, err))
		return rows, err
	}
}

func NewReturn[T any](value T, err error) *Return[T] {
	return &Return[T]{
		Value: value,
		Err:   err,
	}
}

type Return[T any] struct {
	Value T
	Err   error
}

func NewRows(columns []string) *Rows {
	return &Rows{
		Cols:      columns,
		NextErr:   make(map[int]error),
		Converter: driver.DefaultParameterConverter,
	}
}

// Rows mainly copy from `sqlmock`, and used with `sqlkit.Mock`
// Experimental!!!
type Rows struct {
	Converter driver.ValueConverter
	Cols      []string
	Rows      [][]driver.Value
	CloseErr  error
	Pos       int
	NextErr   map[int]error
}

func (r *Rows) Columns() []string {
	return r.Cols
}

func (r *Rows) Close() error {
	return r.CloseErr
}

// advances to next row
func (r *Rows) Next(dest []driver.Value) error {
	r.Pos++
	if r.Pos > len(r.Rows) {
		return io.EOF // per interface spec
	}
	for i, col := range r.Rows[r.Pos-1] {
		if b, ok := rawBytes(col); ok {
			dest[i] = b
			continue
		}
		dest[i] = col
	}
	return r.NextErr[r.Pos-1]
}

func rawBytes(col driver.Value) (_ []byte, ok bool) {
	val, ok := col.([]byte)
	if !ok || len(val) == 0 {
		return nil, false
	}
	// Copy the bytes from the mocked row into a shared raw buffer, which we'll replace the content of later
	// This allows scanning into sql.RawBytes to correctly become invalid on subsequent calls to Next(), Scan() or Close()
	b := make([]byte, len(val))
	copy(b, val)
	return b, true
}

// CloseError allows to set an error
// which will be returned by rows.Close
// function.
//
// The close error will be triggered only in cases
// when rows.Next() EOF was not yet reached, that is
// a default sql library behavior
func (r *Rows) CloseError(err error) *Rows {
	r.CloseErr = err
	return r
}

// RowError allows to set an error
// which will be returned when a given
// row number is read
func (r *Rows) RowError(row int, err error) *Rows {
	r.NextErr[row] = err
	return r
}

// AddRow composed from database driver.Value slice
// return the same instance to perform subsequent actions.
// Note that the number of values must match the number
// of columns
func (r *Rows) AddRow(values ...driver.Value) *Rows {
	if len(values) != len(r.Cols) {
		panic("Expected number of values to match number of columns")
	}

	row := make([]driver.Value, len(r.Cols))
	for i, v := range values {
		// Convert user-friendly values (such as int or driver.Valuer)
		// to database/sql native value (driver.Value such as int64)
		var err error
		v, err = r.Converter.ConvertValue(v)
		if err != nil {
			panic(fmt.Errorf(
				"row #%d, column #%d (%q) type %T: %s",
				len(r.Rows)+1, i, r.Cols[i], values[i], err,
			))
		}

		row[i] = v
	}

	r.Rows = append(r.Rows, row)
	return r
}

// FromCSVString build rows from csv string.
// return the same instance to perform subsequent actions.
// Note that the number of values must match the number
// of columns
func (r *Rows) FromCSVString(s string) *Rows {
	res := strings.NewReader(strings.TrimSpace(s))
	csvReader := csv.NewReader(res)

	for {
		res, err := csvReader.Read()
		if err != nil || res == nil {
			break
		}

		row := make([]driver.Value, len(r.Cols))
		for i, v := range res {
			row[i] = CSVColumnParser(strings.TrimSpace(v))
		}
		r.Rows = append(r.Rows, row)
	}
	return r
}

// CSVColumnParser is a function which converts trimmed csv
// column string to a []byte representation. Currently
// transforms NULL to nil
var CSVColumnParser = func(s string) []byte {
	switch {
	case strings.ToLower(s) == "null":
		return nil
	}
	return []byte(s)
}

type EmptyRows struct{}

func (rs *EmptyRows) Columns() []string              { return nil }
func (rs *EmptyRows) Close() error                   { return nil }
func (rs *EmptyRows) Next(dest []driver.Value) error { return io.EOF }

var (
	_ Middleware = (*Mock)(nil)
)
