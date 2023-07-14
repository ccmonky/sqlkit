package sqlkit

import (
	"context"
	"strconv"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/ccmonky/pkg/logkit"
)

// LogHooks log sqls with rt and traceid
type LogHooks struct {
	Logger    *zap.Logger
	Level     zapcore.Level `json:"level,omitempty"`
	FieldSize int           `json:"field_size,omitempty"`
}

// Before hook will print the query with it's args and return the context with the timestamp
// TODO: before log + after log?
func (h *LogHooks) Before(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	return context.WithValue(ctx, ctxKeyStartTime, time.Now()), nil
}

// After hook will get the timestamp registered on the Before hook and print the elapsed time
func (h *LogHooks) After(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	start := ctx.Value(ctxKeyStartTime).(time.Time)
	rt := time.Since(start).Nanoseconds() / 1e6 // unit: Ms
	var fields = make([]zap.Field, 0, len(args)+3)
	fields = append(fields, zap.String("query", query))
	fields = append(fields, zap.Int64("rt", rt))
	fields = append(fields, zap.String("gsid", logkit.GetReqID(ctx)))
	for i, arg := range args {
		argi := "arg" + strconv.Itoa(i)
		if h.FieldSize <= 0 {
			fields = append(fields, zap.Any(argi, arg))
		} else {
			fields = append(fields, logkit.ZapAnyN(argi, arg, h.FieldSize))
		}
	}
	h.Logger.Log(h.Level, "sql log", fields...)
	return ctx, nil
}

var ctxKeyStartTime = struct{}{}