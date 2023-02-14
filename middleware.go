// The MIT License (MIT)
//
// Copyright (c) 2016 Gustavo Chaín
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package sqlkit

import (
	"context"
	"database/sql/driver"
	"errors"
)

// Middleware is a middleware which wrap a driver to another
// This work is based on `sqlhooks`, why define a new `Hook`? For `sqlhooks.Hooks` can not operate the arguments & returns
// but only ctx, which is not applicable for some scenarios, e.g. mock, cache, tranform, ...
// Experimental!!!
type Middleware interface {
	ExecContext(ExecContext) ExecContext
	QueryContext(QueryContext) QueryContext
}

type ExecContext func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error)

type QueryContext func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)

// Wrap is used to create a new instrumented driver, it takes a vendor specific driver, and a Hooks instance to produce a new driver instance.
// It's usually used inside a sql.Register() statement
func Wrap(driver driver.Driver, wrapper Middleware) driver.Driver {
	return &Driver{driver, wrapper}
}

// Driver implements a database/sql/driver.Driver
type Driver struct {
	driver.Driver
	wrapper Middleware
}

// Open opens a connection
func (drv *Driver) Open(name string) (driver.Conn, error) {
	conn, err := drv.Driver.Open(name)
	if err != nil {
		return conn, err
	}

	// Drivers that don't implement driver.ConnBeginTx are not supported.
	if _, ok := conn.(driver.ConnBeginTx); !ok {
		return nil, errors.New("driver must implement driver.ConnBeginTx")
	}

	wrapped := &Conn{conn, drv.wrapper}
	if isExecer(conn) && isQueryer(conn) && isSessionResetter(conn) {
		return &ExecerQueryerContextWithSessionResetter{wrapped,
			&ExecerContext{wrapped}, &QueryerContext{wrapped},
			&SessionResetter{wrapped}}, nil
	} else if isExecer(conn) && isQueryer(conn) {
		return &ExecerQueryerContext{wrapped, &ExecerContext{wrapped},
			&QueryerContext{wrapped}}, nil
	} else if isExecer(conn) {
		// If conn implements an Execer interface, return a driver.Conn which
		// also implements Execer
		return &ExecerContext{wrapped}, nil
	} else if isQueryer(conn) {
		// If conn implements an Queryer interface, return a driver.Conn which
		// also implements Queryer
		return &QueryerContext{wrapped}, nil
	}
	return wrapped, nil
}

// Conn implements a database/sql.driver.Conn
type Conn struct {
	Conn    driver.Conn
	wrapper Middleware
}

func (conn *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var (
		stmt driver.Stmt
		err  error
	)

	if c, ok := conn.Conn.(driver.ConnPrepareContext); ok {
		stmt, err = c.PrepareContext(ctx, query)
	} else {
		stmt, err = conn.Prepare(query)
	}

	if err != nil {
		return stmt, err
	}

	return &Stmt{
		Stmt:    stmt,
		query:   query,
		wrapper: conn.wrapper}, nil
}

func (conn *Conn) Prepare(query string) (driver.Stmt, error) { return conn.Conn.Prepare(query) }
func (conn *Conn) Close() error                              { return conn.Conn.Close() }
func (conn *Conn) Begin() (driver.Tx, error)                 { return conn.Conn.Begin() }
func (conn *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return conn.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

// ExecerContext implements a database/sql.driver.ExecerContext
type ExecerContext struct {
	*Conn
}

func isExecer(conn driver.Conn) bool {
	switch conn.(type) {
	case driver.ExecerContext:
		return true
	case driver.Execer:
		return true
	default:
		return false
	}
}

func (conn *ExecerContext) execContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	switch c := conn.Conn.Conn.(type) {
	case driver.ExecerContext:
		return c.ExecContext(ctx, query, args)
	case driver.Execer:
		dargs, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		return c.Exec(query, dargs)
	default:
		// This should not happen
		return nil, errors.New("ExecerContext created for a non Execer driver.Conn")
	}
}

func (conn *ExecerContext) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return conn.wrapper.ExecContext(conn.execContext)(ctx, query, args)
}

func (conn *ExecerContext) Exec(query string, args []driver.Value) (driver.Result, error) {
	// We have to implement Exec since it is required in the current version of
	// Go for it to run ExecContext. From Go 10 it will be optional. However,
	// this code should never run since database/sql always prefers to run
	// ExecContext.
	return nil, errors.New("Exec was called when ExecContext was implemented")
}

// QueryerContext implements a database/sql.driver.QueryerContext
type QueryerContext struct {
	*Conn
}

func isQueryer(conn driver.Conn) bool {
	switch conn.(type) {
	case driver.QueryerContext:
		return true
	case driver.Queryer:
		return true
	default:
		return false
	}
}

func (conn *QueryerContext) queryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	switch c := conn.Conn.Conn.(type) {
	case driver.QueryerContext:
		return c.QueryContext(ctx, query, args)
	case driver.Queryer:
		dargs, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		return c.Query(query, dargs)
	default:
		// This should not happen
		return nil, errors.New("QueryerContext created for a non Queryer driver.Conn")
	}
}

func (conn *QueryerContext) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	return conn.wrapper.QueryContext(conn.queryContext)(ctx, query, args)
}

// ExecerQueryerContext implements database/sql.driver.ExecerContext and
// database/sql.driver.QueryerContext
type ExecerQueryerContext struct {
	*Conn
	*ExecerContext
	*QueryerContext
}

// ExecerQueryerContext implements database/sql.driver.ExecerContext and
// database/sql.driver.QueryerContext
type ExecerQueryerContextWithSessionResetter struct {
	*Conn
	*ExecerContext
	*QueryerContext
	*SessionResetter
}

type SessionResetter struct {
	*Conn
}

// Stmt implements a database/sql/driver.Stmt
type Stmt struct {
	Stmt    driver.Stmt
	query   string
	wrapper Middleware
}

func (stmt *Stmt) execContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s, ok := stmt.Stmt.(driver.StmtExecContext); ok {
		return s.ExecContext(ctx, args)
	}

	values := make([]driver.Value, len(args))
	for _, arg := range args {
		values[arg.Ordinal-1] = arg.Value
	}

	return stmt.Exec(values)
}

func (stmt *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return stmt.wrapper.ExecContext(func(c context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
		return stmt.execContext(c, a)
	})(ctx, stmt.query, args)
}

func (stmt *Stmt) queryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s, ok := stmt.Stmt.(driver.StmtQueryContext); ok {
		return s.QueryContext(ctx, args)
	}

	values := make([]driver.Value, len(args))
	for _, arg := range args {
		values[arg.Ordinal-1] = arg.Value
	}
	return stmt.Query(values)
}

func (stmt *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return stmt.wrapper.QueryContext(func(c context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
		return stmt.queryContext(c, a)
	})(ctx, stmt.query, args)
}

func (stmt *Stmt) Close() error                                    { return stmt.Stmt.Close() }
func (stmt *Stmt) NumInput() int                                   { return stmt.Stmt.NumInput() }
func (stmt *Stmt) Exec(args []driver.Value) (driver.Result, error) { return stmt.Stmt.Exec(args) }
func (stmt *Stmt) Query(args []driver.Value) (driver.Rows, error)  { return stmt.Stmt.Query(args) }

func namedToInterface(args []driver.NamedValue) []interface{} {
	list := make([]interface{}, len(args))
	for i, a := range args {
		list[i] = a.Value
	}
	return list
}

// namedValueToValue copied from database/sql
func namedValueToValue(named []driver.NamedValue) ([]driver.Value, error) {
	dargs := make([]driver.Value, len(named))
	for n, param := range named {
		if len(param.Name) > 0 {
			return nil, errors.New("sql: driver does not support the use of Named Parameters")
		}
		dargs[n] = param.Value
	}
	return dargs, nil
}

func isSessionResetter(conn driver.Conn) bool {
	_, ok := conn.(driver.SessionResetter)
	return ok
}

func (s *SessionResetter) ResetSession(ctx context.Context) error {
	c := s.Conn.Conn.(driver.SessionResetter)
	return c.ResetSession(ctx)
}
