package sqlkit_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"

	"github.com/ccmonky/sqlkit"
)

func TestMock(t *testing.T) {
	ctx := context.Background()
	mock := sqlkit.NewMock()
	query := "select id, app_name, name, version from data;"
	sql.Register("mock", sqlkit.Wrap(&mysql.MySQLDriver{}, mock))
	db, err := sql.Open("mock", dsn)
	assert.Nilf(t, err, "open mock db err")

	rows, err := db.QueryContext(ctx, query)
	assert.Nilf(t, err, "query err")
	ds := datas(rows)
	assert.Equalf(t, 3, len(ds), "rows length")
	for _, d := range ds {
		t.Log(d)
	}

	columns := []string{"id", "app_name", "name", "version"}
	mockRrows := sqlkit.NewRows(columns).
		AddRow(1, "auto-edd-dmap", "name1", "version1").
		AddRow(2, "auto-edd-apistore", "name2", "version2")
	mock.AddQuery(query, sqlkit.NewReturn[driver.Rows](mockRrows, nil))

	rows, err = db.QueryContext(ctx, query)
	assert.Nilf(t, err, "query err")
	ds = datas(rows)
	assert.Equalf(t, 2, len(ds), "rows length")
	for _, d := range ds {
		t.Log(d)
	}
}
