package sqlkit_test

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/ccmonky/pkg/utils"
	"github.com/ccmonky/sqlkit"
	"github.com/mattn/go-sqlite3"
	"github.com/qustavo/sqlhooks/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestLogHooks(t *testing.T) {
	b := &bytes.Buffer{}
	c := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zap.CombineWriteSyncers(os.Stderr, zapcore.AddSync(b)),
		zapcore.DebugLevel,
	)
	logger := zap.New(c)

	// First, register the wrapper
	sql.Register("sqlite3WithLogHooks", sqlhooks.Wrap(&sqlite3.SQLiteDriver{}, &sqlkit.LogHooks{
		Logger:    logger,
		Level:     zapcore.InfoLevel,
		FieldSize: 5,
	}))
	// Connect to the registered wrapped driver
	db, err := sql.Open("sqlite3WithLogHooks", ":memory:")
	assert.Nil(t, err)
	defer db.Close()
	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE t1 (id INTEGER, text VARCHAR(16))")
	assert.Nil(t, err)
	ctx = context.WithValue(ctx, utils.RequestIDKey, "foo")
	_, err = db.ExecContext(ctx, "INSERT into t1 (text) VALUES(?), (?)", "foo", "bar")
	assert.Nil(t, err)
	ctx = context.WithValue(ctx, utils.RequestIDKey, "bar")
	rows, err := db.QueryContext(ctx, "SELECT id FROM t1")
	assert.Nil(t, err)
	rows.Close()
	ctx = context.WithValue(ctx, utils.RequestIDKey, "baz")
	rows, err = db.QueryContext(ctx, "SELECT * FROM t1 where text=?", "123456789")
	assert.Nil(t, err)
	rows.Close()
	rows, err = db.QueryContext(ctx, "SELECT id, text FROM t1")
	assert.Nil(t, err)
	rows.Close()
	_, err = db.QueryContext(ctx, "SELECT id2, text FROM t1")
	assert.NotNil(t, err)

	// {"level":"info","ts":1687946069.724422,"msg":"sql log","query":"CREATE TABLE t1 (id INTEGER, text VARCHAR(16))","rt":0,"gsid":"-"}
	// {"level":"info","ts":1687946069.724513,"msg":"sql log","query":"INSERT into t1 (text) VALUES(?), (?)","rt":0,"gsid":"foo","arg0":"foo","arg1":"bar"}
	// {"level":"info","ts":1687946069.724535,"msg":"sql log","query":"SELECT id FROM t1","rt":0,"gsid":"bar"}
	// {"level":"info","ts":1687946069.724555,"msg":"sql log","query":"SELECT * FROM t1 where text=?","rt":0,"gsid":"baz","arg0":"12345..."}
	// {"level":"info","ts":1687946069.724567,"msg":"sql log","query":"SELECT id, text FROM t1","rt":0,"gsid":"baz"}
	length := len(strings.Split(b.String(), "\n"))
	if length < 6 {
		t.Fatalf("should >=6, got %d", length)
	}
}
