package sqlkit_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/qustavo/sqlhooks/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ccmonky/errors"
	"github.com/ccmonky/sqlkit"
	skmysql "github.com/ccmonky/sqlkit/mysql"
)

func prepareAuditDB(
	driverName string,
	config []byte,
	registerDriver func(string, *sqlkit.Audit)) (*sqlkit.Audit, *sql.DB, error) {
	var audit = sqlkit.Audit{}
	err := json.Unmarshal(config, &audit)
	if err != nil {
		return nil, nil, err
	}
	ctx := context.TODO()
	err = audit.Provision(ctx)
	if err != nil {
		return nil, nil, err
	}
	registerDriver(driverName, &audit)
	adb, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, nil, err
	}
	audit.SetDB(adb)

	if err := audit.Validate(); err != nil {
		return nil, nil, err
	}
	return &audit, adb, nil
}

func TestAudit(t *testing.T) {
	config := []byte(`{
		"database_name": "sqlkitdemo",
		"alarm_threshold": 0,
		"banned_threshold": 2,
		"seen_sql_log_level": 0
	}`)
	ctx := context.Background()
	audit, adb, err := prepareAuditDB(
		sqlkit.MysqlAuditDriverName,
		config,
		func(driverName string, au *sqlkit.Audit) {
			sql.Register(driverName, sqlhooks.Wrap(&mysql.MySQLDriver{}, au))
		})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		adb.Close()
	}()

	assert.Equalf(t, "sqlkitdemo", audit.DatabaseName, "DatabaseName")
	assert.Equalf(t, int64(0), *audit.AlarmThreshold, "AlarmThreshold")
	assert.Equalf(t, int64(2), audit.BannedThreshold, "BannedThreshold")
	assert.Equalf(t, int32(0), audit.SeenSqlLogLevel.Load(), "SeenSqlLogLevel")
	tables, err := audit.Tables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(tables)
	if err != nil {
		t.Fatal(err)
	}
	assert.JSONEq(t, `{
		"data": {
			"name": "data",
			"count": 3
		},
		"scripts": {
			"name": "scripts",
			"count": 0
		},
		"tests": {
			"name": "tests",
			"count": 0
		}
	}`, string(data), "tables")

	_, _ = adb.QueryContext(ctx, "select * from data;")
	time.Sleep(3 * time.Second)
	_, err = adb.QueryContext(ctx, "select * from data;")
	assert.Truef(t, errors.Is(err, sqlkit.ErrBanned), "should banned error")
	assert.Truef(t, errors.Is(sqlkit.ErrBanned, errors.InvalidArgument), "should parameters error")
	_, err = adb.QueryContext(ctx, "select * from data where app_name=?;", "xxx-")
	assert.Nilf(t, err, "should not banned")
	_, err = adb.QueryContext(ctx, "select * from tests;")
	assert.Nilf(t, err, "should not banned")
	time.Sleep(time.Second)

	sqls := audit.Sqls()
	data, err = json.Marshal(sqls)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
	assert.JSONEq(t, `{
		"select * from data where app_name=?;": {
			"query": "select * from data where app_name=?;",
			"args": [
				"xxx-"
			],
			"explain": [
				{
					"id": 1,
					"select_type": "SIMPLE",
					"table": "data",
					"partitions": null,
					"type": "ref",
					"possible_keys": "data_app_name_name_version",
					"key": "data_app_name_name_version",
					"key_len": 1022,
					"ref": "const",
					"rows": 1,
					"filtered": 100,
					"extra": null
				}
			],
			"alarm_type": "normal",
			"reason": "",
			"created_at": "2023-02-01T15:00:00+08:00"
		},
		"select * from data;": {
			"query": "select * from data;",
			"args": [],
			"explain": [
				{
					"id": 1,
					"select_type": "SIMPLE",
					"table": "data",
					"partitions": null,
					"type": "ALL",
					"possible_keys": null,
					"key": null,
					"key_len": null,
					"ref": null,
					"rows": 3,
					"filtered": 100,
					"extra": null
				}
			],
			"alarm_type": "banned",
			"reason": "explain:type:ALL",
			"created_at": "2023-02-01T15:00:00+08:00"
		},
		"select * from tests;": {
			"query": "select * from tests;",
			"args": [],
			"explain": [
				{
					"id": 1,
					"select_type": "SIMPLE",
					"table": "tests",
					"partitions": null,
					"type": "ALL",
					"possible_keys": null,
					"key": null,
					"key_len": null,
					"ref": null,
					"rows": 1,
					"filtered": 100,
					"extra": null
				}
			],
			"alarm_type": "alarm",
			"reason": "explain:type:ALL",
			"created_at": "2023-02-01T15:00:00+08:00"
		}
	}`, string(data), "sqls")
}

func TestSql(t *testing.T) {
	createdAt := now
	s := sqlkit.Sql{
		Query: "select * from data where app_name=?;",
		Args: []interface{}{
			"xxx-demo",
		},
		Explain: []skmysql.ExplainRow{
			skmysql.ExplainRow{
				ID:         1,
				SelectType: "SIMPLE",
				Table:      ptr("t"),
				Type:       ptr("ALL"),
				Rows:       ptr(1),
				Filtered:   ptr[float32](100),
				Extra:      ptr("Using where"),
			},
			skmysql.ExplainRow{
				ID:           1,
				SelectType:   "SIMPLE",
				Table:        ptr("d"),
				Partitions:   nil,
				Type:         ptr("eq_ref"),
				PossibleKeys: ptr("PRIMARY"),
				Key:          ptr("PRIMARY"),
				KeyLen:       ptr(8),
				Ref:          ptr("sqlkitdemo.t.data_id"),
				Rows:         ptr(1),
				Filtered:     ptr[float32](100),
				Extra:        nil,
			},
		},
		Reason:    "explain:type:ALL",
		CreatedAt: createdAt,
	}
	if s.AlarmType != sqlkit.Normal {
		t.Fatal("should ==")
	}
	s.AlarmType = sqlkit.Alarm
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(data))
	assert.JSONEq(t, `{
		"query": "select * from data where app_name=?;",
		"args": [
			"xxx-demo"
		],
		"explain": [
			{
				"id": 1,
				"select_type": "SIMPLE",
				"table": "t",
				"partitions": null,
				"type": "ALL",
				"possible_keys": null,
				"key": null,
				"key_len": null,
				"ref": null,
				"rows": 1,
				"filtered": 100,
				"extra": "Using where"
			},
			{
				"id": 1,
				"select_type": "SIMPLE",
				"table": "d",
				"partitions": null,
				"type": "eq_ref",
				"possible_keys": "PRIMARY",
				"key": "PRIMARY",
				"key_len": 8,
				"ref": "sqlkitdemo.t.data_id",
				"rows": 1,
				"filtered": 100,
				"extra": null
			}
		],
		"alarm_type": "alarm",
		"reason": "explain:type:ALL",
		"created_at": "2023-02-01T15:00:00+08:00"
	}`, string(data), "marshal sql")
	var s2 sqlkit.Sql
	err = json.Unmarshal(data, &s2)
	assert.Nilf(t, err, "unmarshal sql")
	assert.Equalf(t, "select * from data where app_name=?;", s2.Query, "query")
	assert.Equalf(t, "xxx-demo", s2.Args[0], "args")
	assert.Equalf(t, sqlkit.Alarm, s2.AlarmType, "alarm type")
	assert.Equalf(t, createdAt, s2.CreatedAt, "created_at")

	assert.Equalf(t, 1, s2.Explain[0].ID, "explain[0].ID")
	assert.Equalf(t, "SIMPLE", s2.Explain[0].SelectType, "explain[0].Simple")
	assert.Equalf(t, "t", *s2.Explain[0].Table, "explain[0].Table")
	assert.Nilf(t, s2.Explain[0].Partitions, "explain[0].Partitions")
	assert.Equalf(t, "ALL", *s2.Explain[0].Type, "explain[0].Type")
	assert.Nilf(t, s2.Explain[0].PossibleKeys, "explain[0].PossibleKeys")
	assert.Nilf(t, s2.Explain[0].Key, "explain[0].Key")
	assert.Nilf(t, s2.Explain[0].KeyLen, "explain[1].KeyLen")
	assert.Nilf(t, s2.Explain[0].Ref, "explain[0].Ref")
	assert.Equalf(t, 1, *s2.Explain[0].Rows, "explain[0].Rows")
	assert.Equalf(t, float32(100), *s2.Explain[0].Filtered, "explain[0].Filtered")
	assert.Equalf(t, "Using where", *s2.Explain[0].Extra, "explain[0].Extra")

	assert.Equalf(t, 1, s2.Explain[1].ID, "explain[1].ID")
	assert.Equalf(t, "SIMPLE", s2.Explain[1].SelectType, "explain[1].Simple")
	assert.Equalf(t, "d", *s2.Explain[1].Table, "explain[1].Table")
	assert.Nilf(t, s2.Explain[1].Partitions, "explain[1].Partitions")
	assert.Equalf(t, "eq_ref", *s2.Explain[1].Type, "explain[1].Type")
	assert.Equalf(t, "PRIMARY", *s2.Explain[1].PossibleKeys, "explain[1].PossibleKeys")
	assert.Equalf(t, "PRIMARY", *s2.Explain[1].Key, "explain[1].Key")
	assert.Equalf(t, 8, *s2.Explain[1].KeyLen, "explain[1].KeyLen")
	assert.Equalf(t, "sqlkitdemo.t.data_id", *s2.Explain[1].Ref, "explain[1].Ref")
	assert.Equalf(t, 1, *s2.Explain[1].Rows, "explain[1].Rows")
	assert.Equalf(t, float32(100), *s2.Explain[1].Filtered, "explain[1].Filtered")
	assert.Nilf(t, s2.Explain[1].Extra, "explain[1].Extra")
}

func ptr[T any](s T) *T {
	return &s
}

func TestAPI(t *testing.T) {
	config := []byte(`{
		"database_name": "sqlkitdemo",
		"alarm_threshold": 0,
		"banned_threshold": 2
	}`)
	audit, auditDB, err := prepareAuditDB(
		"audit:mysql+testapi",
		config,
		func(dn string, au *sqlkit.Audit) {
			sql.Register(dn, sqlhooks.Wrap(&mysql.MySQLDriver{}, au))
		})
	if err != nil {
		t.Fatal(err)
	}
	testMockDB(t, auditDB, true)
	testAudit(t, audit)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method + ":" + r.URL.Path {
		case "GET:/config":
			audit.ConfigAPI(w, r)
			return
		case "GET:/tables":
			audit.TablesAPI(w, r)
			return
		case "GET:/sqls":
			audit.SqlsAPI(w, r)
			return
		case "GET:/metrics":
			audit.MetricsAPI(w, r)
			return
		case "POST:/config/seen_sql_log_level":
			audit.SetSeenSqlLogLevelAPI(w, r)
			return
		case "POST:/config/whitelist":
			audit.WhitelistAPI(w, r)
			return
		case "POST:/config/blacklist":
			audit.BlacklistAPI(w, r)
			return
		default:
			io.WriteString(w, fmt.Sprintf("unknown route: %s:%s", r.Method, r.URL.Path))
		}
	}))
	defer ts.Close()

	testAPI(t, "GET", ts.URL+"/tables?_renderx=rest", nil, 200, ptr(`{
		"data": {
			"app": "",
			"database": "sqlkitdemo",
			"tables": {
				"data": {
					"name": "data",
					"count": 3
				},
				"scripts": {
					"name": "scripts",
					"count": 0
				},
				"tests": {
					"name": "tests",
					"count": 0
				}
			}
		},
		"errcode": "1",
		"errdetail": "",
		"errmsg": "Successful.",
		"result": true
	}`))
	testAPI(t, "GET", ts.URL+"/config?_renderx=rest", nil, 200, ptr(`{
		"data": {
			"alarm_threshold": 0,
			"app": "",
			"banned_threshold": 2,
			"database": "sqlkitdemo",
			"explain_extra_alarm_substrs": {
				"Block Nested Loop": {},
				"filesort": {},
				"temporary": {}
			},
			"seen_sql_log_level": 1,
			"sql_cache_duration": null,
			"whitelist": [
				"SELECT table_name, table_rows FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?;"
			]
		},
		"errcode": "1",
		"errdetail": "",
		"errmsg": "Successful.",
		"result": true
	}`))
	testAPI(t, "GET", ts.URL+"/sqls?_renderx=rest", nil, 200, ptr(`{
		"data": {
			"app": "",
			"database": "sqlkitdemo",
			"sqls": {
				"select id, app_name, name, version from data where app_name='xxx-demo';": {
					"query": "select id, app_name, name, version from data where app_name='xxx-demo';",
					"args": [],
					"explain": [
						{
							"id": 1,
							"select_type": "SIMPLE",
							"table": "data",
							"partitions": null,
							"type": "ref",
							"possible_keys": "data_app_name_name_version",
							"key": "data_app_name_name_version",
							"key_len": 1022,
							"ref": "const",
							"rows": 3,
							"filtered": 100,
							"extra": null
						}
					],
					"alarm_type": "normal",
					"reason": "",
					"created_at": "2023-02-01T15:00:00+08:00"
				},
				"select id, app_name, name, version from data;": {
					"query": "select id, app_name, name, version from data;",
					"args": [],
					"explain": [
						{
							"id": 1,
							"select_type": "SIMPLE",
							"table": "data",
							"partitions": null,
							"type": "ALL",
							"possible_keys": null,
							"key": null,
							"key_len": null,
							"ref": null,
							"rows": 3,
							"filtered": 100,
							"extra": null
						}
					],
					"alarm_type": "banned",
					"reason": "explain:type:ALL",
					"created_at": "2023-02-01T15:00:00+08:00"
				}
			}
		},
		"errcode": "1",
		"errdetail": "",
		"errmsg": "Successful.",
		"result": true
	}`))
	testAPI(t, "GET", ts.URL+"/metrics?_renderx=rest", nil, 200, nil)
	testAPI(t, "POST", ts.URL+"/config/seen_sql_log_level?seen_sql_log_level=0&_renderx=rest", nil, 200, ptr(`{
		"data": {
			"seen_sql_log_level": 0
		},
		"errcode": "1",
		"errdetail": "",
		"errmsg": "Successful.",
		"result": true
	}`))
	audit.DelWhitelistQuery(skmysql.TablesQuery)
	testAPI(t, "POST", ts.URL+"/config/whitelist?action=add&_renderx=rest", bytes.NewBufferString(`{"query": "select * from scripts;"}`), 200, ptr(`{
		"data": {
			"whitelist": [
				"select * from scripts;"
			]
		},
		"errcode": "1",
		"errdetail": "",
		"errmsg": "Successful.",
		"result": true
	}`))
	testAPI(t, "POST", ts.URL+"/config/whitelist?action=delete&_renderx=rest", bytes.NewBufferString(`{"query": "select * from scripts;"}`), 200, ptr(`{
		"data": {
			"whitelist": null
		},
		"errcode": "1",
		"errdetail": "",
		"errmsg": "Successful.",
		"result": true
	}`))
	testAPI(t,
		"POST",
		ts.URL+"/config/blacklist?action=add&_renderx=rest",
		bytes.NewBufferString(`{
			"query": "select * from tests;",
			"alarm_type": "banned",
			"reason": "test"
		}`),
		200,
		ptr(`{
			"data": {
				"select * from tests;": {
					"query": "select * from tests;",
					"args": null,
					"explain": null,
					"alarm_type": "banned",
					"reason": "test",
					"created_at": "2023-02-01T15:00:00+08:00"
				}
			},
			"errcode": "1",
			"errdetail": "",
			"errmsg": "Successful.",
			"result": true
		}`),
	)
}

func testAPI(t *testing.T, method, url string, rqBody io.Reader, status int, jsonBody *string) {
	var rp *http.Response
	var err error
	switch method {
	case "POST":
		rp, err = http.Post(url, "application/json", rqBody)
	default:
		rp, err = http.Get(url)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer rp.Body.Close()
	assert.Equalf(t, status, rp.StatusCode, "status code")
	body, err := io.ReadAll(rp.Body)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(string(body))
	if jsonBody != nil {
		assert.JSONEqf(t, *jsonBody, string(body), "json body")
	}
}

func benchmark(b *testing.B, db *sql.DB) {
	for i := 0; i < b.N; i++ {
		rows, err := db.QueryContext(context.Background(), query)
		require.NoError(b, err)
		require.NoError(b, rows.Close())
	}
}

func BenchmarkAudit(b *testing.B) {
	mockDB, err := prepareMockDB()
	if err != nil {
		b.Fatal(err)
	}
	testMockDB(b, mockDB, false)
	// configure other options, like `"seen_sql_log_level": 0`
	config := []byte(`{
		"database_name": "sqlkitdemo",
		"alarm_threshold": 0,
		"banned_threshold": 2
	}`)
	audit, auditMockDB, err := prepareAuditDB(
		"audit:mock:mysql",
		config,
		func(dn string, au *sqlkit.Audit) {
			//sql.Register(auditMockDriverName, sqlhooks.Wrap(&mysql.MySQLDriver{}, audit))
			sql.Register(dn, sqlhooks.Wrap(sqlkit.Wrap(&mysql.MySQLDriver{}, sqlkit.NewMock()), au))
			//sql.Register(auditMockDriverName, sqlkit.Wrap(sqlkit.Wrap(&mysql.MySQLDriver{}, sqlkit.NewMock()), &audit))
		})
	if err != nil {
		b.Fatal(err)
	}
	testMockDB(b, auditMockDB, true)
	testAudit(b, audit)
	b.ResetTimer()
	b.Run("With Hooks", func(b *testing.B) {
		benchmark(b, auditMockDB)
	})
	b.Run("Without Hooks", func(b *testing.B) {
		benchmark(b, mockDB)
	})
	fmt.Println("before_duration_seconds: ", sqlkit.MarshalMetric("before_duration_seconds"))
	fmt.Println("after_duration_seconds: ", sqlkit.MarshalMetric("after_duration_seconds"))
}

func prepareMockDB() (*sql.DB, error) {
	mockDriverName := "mock:mysql"
	mockDriver := sqlkit.Wrap(&mysql.MySQLDriver{}, sqlkit.NewMock())
	sql.Register(mockDriverName, mockDriver)
	mysqlDB, err := sql.Open(mockDriverName, dsn)
	if err != nil {
		return nil, err
	}
	return mysqlDB, nil
}

func testMockDB(b assert.TestingT, db *sql.DB, isAudit bool) {
	ctx := context.Background()

	// FIXME: why got ErrSkip for mock?
	// m, err := sqlkit.NewMySQL(db).GetTables(ctx, "sqlkitdemo")
	// assert.Nilf(b, err, "get tables error")
	// b.Log(m)

	rows, err := db.QueryContext(ctx, query)
	assert.Nilf(b, err, "%s err", query)
	assert.Lenf(b, datas(rows), 3, "query got 3 rows")

	if isAudit {
		_, _ = db.QueryContext(ctx, queryBanned)
		//assert.Truef(b, errors.Is(err, sqlkit.ErrBanned), "err is banned") // not true if async
		time.Sleep(3 * time.Second)
		rows, err := db.QueryContext(ctx, queryBanned)
		assert.Truef(b, errors.Is(err, sqlkit.ErrBanned), "err is banned")
		assert.Nilf(b, rows, "query banned rows")
	} else {
		rows, err := db.QueryContext(ctx, queryBanned)
		assert.Nilf(b, err, "%s err", query)
		assert.Lenf(b, datas(rows), 3, "query banend on mock got 3 rows")
	}
}

func testAudit(b assert.TestingT, audit *sqlkit.Audit) {
	s := audit.GetSql(query)
	assert.Truef(b, s.AlarmType == sqlkit.Normal, "alarm normal")

	s = audit.GetSql(queryBanned)
	assert.Truef(b, s.AlarmType == sqlkit.Banned, "alarm banned")

	ers, err := audit.Explain(context.TODO(), "select * from data;")
	assert.Nilf(b, err, "explain err")
	assert.Lenf(b, ers, 1, "explain rows length")
}

func init() {
	sqlkit.Now = func() time.Time {
		return now
	}
}

type Data struct {
	ID      int
	AppName string
	Name    string
	Version string
}

func (d Data) String() string {
	return fmt.Sprintf("%d:%s:%s:%s", d.ID, d.AppName, d.Name, d.Version)
}

func datas(rows *sql.Rows) []Data {
	defer rows.Close()
	var ds []Data
	for rows.Next() {
		var (
			id      int
			appName string
			name    string
			version string
		)
		err := rows.Scan(&id, &appName, &name, &version)
		if err != nil {
			return nil
		}
		d := Data{
			ID:      id,
			AppName: appName,
			Name:    name,
			Version: version,
		}
		ds = append(ds, d)
	}
	return ds
}

var (
	dsn         = "root@tcp(localhost:3306)/sqlkitdemo?charset=utf8&parseTime=True&loc=Local"
	query       = "select id, app_name, name, version from data where app_name='xxx-demo';"
	queryBanned = "select id, app_name, name, version from data;"
	now         = time.Date(2023, 2, 1, 15, 0, 0, 0, time.Local)
)
