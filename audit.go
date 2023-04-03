package sqlkit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ccmonky/errors"
	"github.com/ccmonky/pkg/utils"
	"github.com/ccmonky/render"
	"github.com/ccmonky/sqlkit/mysql"
	"github.com/golang/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"
	"github.com/qustavo/sqlhooks/v2"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	// MysqlAuditDriverName msyql+audit database driver name
	MysqlAuditDriverName = "audit:mysql"
)

var (
	// App app name
	App = ""

	// DefaultAlarmThreshold default alarm threshold(scan rows)
	DefaultAlarmThreshold int64 = 500

	// DefaultBannedThreshold default banned threshold(scan rows)
	DefaultBannedThreshold int64 = 100000

	// Now is time.Now
	Now = time.Now // used for test
)

// AlarmType alarm type
type AlarmType int

const (
	// Normal normal, means not alarm
	Normal AlarmType = iota

	// Alarm warning, means index missing but the number of scan lines is not large, still let the sql go through
	Alarm

	// Banned banned, means index missing and the number of scan lines is not large, still let the sql will be banned
	Banned
)

func (at AlarmType) String() string {
	switch at {
	case Normal:
		return "normal"
	case Alarm:
		return "alarm"
	case Banned:
		return "banned"
	default:
		return fmt.Sprintf("unknown:(%d)", int(at))
	}
}

func (at AlarmType) MarshalJSON() ([]byte, error) {
	return json.Marshal(at.String())
}

func (at *AlarmType) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case `"normal"`:
		*at = 0
	case `"alarm"`:
		*at = 1
	case `"banned"`:
		*at = 2
	default:
		*at = -1
	}
	return nil
}

// DefaultSeenSqlLogLevel default log level for seen sql
var DefaultSeenSqlLogLevel = int(Alarm)

var (
	// ErrBanned sql banned error, use errors.Is(err, ErrBanned) to assert
	ErrBanned = errors.WithError(errors.New("sql is banned"), errors.InvalidArgument)

	// ErrAlarm sql warning, use errors.Is(err, ErrAlarm) to assert
	ErrAlarm = errors.New("sql is alarmed")
)

// Audit audit sqls and alarm or ban according to some conditions
//
// Usage:
//
//     import (
//         sql "database/sql"
//
//         "github.com/qustavo/sqlhooks/v2"
//         "github.com/go-sql-driver/mysql"
//
//         "gitlab.alibaba-inc.com/t3/pkg/sqlkit"
//     )
//
//     audit := &sqlkit.Audit{
//         DatabaseName: "xxx",
//     }
//     err := audit.Provision(ctx)
//     sql.Register(sql.DriverName, sqlhooks.Wrap(&mysql.MySQLDriver{}, audit))
//     db, err := sql.Open(sqlkit.DriverName, ...)
//     err = audit.SetDB(db) // NOTE: reuse the same pool
//     err = audit.Validate()
//     // if err == nil, then you can use the db ...
//
type Audit struct {
	// DatabaseName database name
	DatabaseName string `json:"database_name"`

	// AlarmThreshold sql will be alarmed if scan rows great than this threshold, default to 500
	AlarmThreshold *int64 `json:"alarm_threshold"`

	// BannedThreshold sql will be banned if scan rows great than this threshold, default to 10w
	BannedThreshold int64 `json:"banned_threshold"`

	// SeenSqlLogLevel log level for seen sqls(not new found), if SeenSqlLogLevel <= AlarmType will be logged
	SeenSqlLogLevel *atomic.Int32 `json:"seen_sql_log_level,omitempty"`

	// Whitelist contains sqls which will not be audited
	Whitelist []string `json:"whitelist,omitempty"`

	// SqlCacheDuration sql explain result cache duration, default is forever
	SqlCacheDuration *utils.Duration `json:"sql_cache_duration,omitempty"`

	// ExplainExtraAlarmSubstrs alarm when explain extra contains the sub-string in this list
	ExplainExtraAlarmSubstrs []string `json:"explain_extra_alarm_substrs,omitempty"`

	// ShouldAuditFunc used to determine if a sql should be audited, default behavior is detect if startss with `select|insert|update|delete`
	// NOTE: it does contains the whitelist
	ShouldAuditFunc func(query string) bool `json:"-"`

	// ContextLogFields extract zap fileds list for logging, e.g. traceid...
	ContextLogFields func(context.Context) []zap.Field `json:"-"`

	logger                   *zap.Logger
	db                       *sql.DB
	sqls                     sync.Map // map[query]*Sql
	whitelist                sync.Map // map[query]struct{}
	explainExtraAlarmSubstrs map[string]struct{}
	labels                   prometheus.Labels
	//rewrites                 sync.Map // map[query]*Rewrite // FIXME: use Middleware?
}

func (audit *Audit) SetDB(db *sql.DB) error {
	if db == nil {
		return errors.New("nil db")
	}
	audit.db = db
	return nil
}

func (audit *Audit) SetLogger(logger *zap.Logger) error {
	if logger == nil {
		return errors.New("nil logger")
	}
	audit.logger = logger
	return nil
}

// SetSeenSqlLogLevel 用于动态调整日志级别, 如用于trace分析问题
func (audit *Audit) SetSeenSqlLogLevel(level int32) {
	if audit.SeenSqlLogLevel == nil {
		audit.SeenSqlLogLevel = atomic.NewInt32(level)
	} else {
		audit.SeenSqlLogLevel.Store(level)
	}
}

// AddBlacklistQuery 用于动态设定黑名单查询, 用于止血
// 注意：未持久化
func (audit *Audit) AddBlacklistQuery(query string, alarmType AlarmType, reason string) {
	s := Sql{
		Query:     query,
		AlarmType: alarmType,
		Reason:    reason,
		CreatedAt: Now(),
	}
	audit.sqls.Store(query, &s)
}

// SetWhitelistQuery 用于动态设定白名单查询, 如出现误判场景
// NOTE: no persistence!
func (audit *Audit) AddWhitelistQuery(query string) {
	audit.whitelist.Store(query, struct{}{})
}

func (audit *Audit) DelWhitelistQuery(query string) {
	audit.whitelist.Delete(query)
}

// Whitelists 返回所有白名单查询, 包括静态配置和动态添加
func (audit *Audit) Whitelists() []string {
	var w []string
	audit.whitelist.Range(func(k, v interface{}) bool {
		w = append(w, k.(string))
		return true
	})
	return w
}

func (audit *Audit) Provision(ctx context.Context) error {
	if audit.logger == nil {
		audit.logger = zap.NewNop()
	}
	if audit.AlarmThreshold == nil {
		audit.AlarmThreshold = &DefaultAlarmThreshold
	}
	if audit.BannedThreshold <= 0 {
		audit.BannedThreshold = DefaultBannedThreshold
	}
	if audit.SeenSqlLogLevel == nil {
		audit.SeenSqlLogLevel = atomic.NewInt32(int32(DefaultSeenSqlLogLevel))
	}

	if audit.ShouldAuditFunc == nil {
		audit.ShouldAuditFunc = DefaultShouldAudit
	}
	if audit.ContextLogFields == nil {
		audit.ContextLogFields = func(context.Context) []zap.Field { return nil }
	}
	if audit.explainExtraAlarmSubstrs == nil {
		audit.explainExtraAlarmSubstrs = map[string]struct{}{
			"Block Nested Loop": struct{}{},
			"temporary":         struct{}{},
			"filesort":          struct{}{},
		}
	}
	for _, ss := range audit.ExplainExtraAlarmSubstrs {
		audit.explainExtraAlarmSubstrs[ss] = struct{}{}
	}
	audit.whitelist.Store(mysql.TablesQuery, struct{}{})
	for _, query := range audit.Whitelist {
		audit.whitelist.Store(query, struct{}{})
	}
	auditMetrics.init.Do(func() {
		initAuditMetrics()
	})
	if audit.labels == nil {
		audit.labels = prometheus.Labels{
			"app":      App,
			"database": audit.DatabaseName,
		}
	}
	return nil
}

func (audit *Audit) Validate() error {
	if audit.db == nil {
		return errors.New("nil db")
	}
	return nil
}

// Tables return all tables cached for representation
func (audit *Audit) Tables(ctx context.Context) (map[string]*mysql.Table, error) {
	return mysql.NewMySQL(audit.db).GetTables(ctx, audit.DatabaseName)
}

// Sqls return all sqls cached for representation
func (audit *Audit) Sqls() map[string]*Sql {
	var sqls = make(map[string]*Sql)
	audit.sqls.Range(func(k, v interface{}) bool {
		sqls[k.(string)] = v.(*Sql)
		return true
	})
	return sqls
}

// Sql sql statement
type Sql struct {
	Query     string             `json:"query"`
	Args      []interface{}      `json:"args"`
	Explain   []mysql.ExplainRow `json:"explain"`
	AlarmType AlarmType          `json:"alarm_type"`
	Reason    string             `json:"reason"`
	CreatedAt time.Time          `json:"created_at"`
}

func (audit *Audit) ShouldAudit(query string) bool {
	if _, ok := audit.whitelist.Load(query); ok {
		return false
	}
	return audit.ShouldAuditFunc(query)
}

// DetectAlarmType 根据Explain结果判断AlarmType
func (audit *Audit) DetectAlarmType(ers []mysql.ExplainRow) (alarmType AlarmType, reason string) {
	alarmType = Normal
	for i := range ers {
		at, cause := audit.detectAlarmType(&ers[i])
		if at > alarmType {
			alarmType = at
			reason = cause
		}
	}
	return
}

func (audit *Audit) detectAlarmType(er *mysql.ExplainRow) (alarmType AlarmType, reason string) {
	alarmType = Normal
	if er.Table == nil || er.Type == nil {
		return
	}
	var rows int
	if er.Rows != nil {
		rows = *er.Rows
	}
	if er.Type != nil {
		if *er.Type == "ALL" || *er.Type == "index" {
			reason = "explain:type:" + *er.Type
			if rows > int(audit.BannedThreshold) {
				alarmType = Banned
			} else if rows > int(*audit.AlarmThreshold) {
				alarmType = Alarm
			}
		}
	}
	if alarmType == Normal && er.Extra != nil {
		for ss := range audit.explainExtraAlarmSubstrs {
			if strings.Contains(*er.Extra, ss) {
				reason = "explain:extra:" + ss
				if rows > int(audit.BannedThreshold) {
					alarmType = Banned
				} else if rows > int(*audit.AlarmThreshold) {
					alarmType = Alarm
				}
			}
		}
	}
	return
}

// Explain do mysql explain
func (audit *Audit) Explain(ctx context.Context, query string, args ...interface{}) ([]mysql.ExplainRow, error) {
	return mysql.NewMySQL(audit.db).Explain(ctx, query, args...)
}

// GetSql get sql
func (audit *Audit) GetSql(query string) *Sql {
	if v, ok := audit.sqls.Load(query); ok {
		return v.(*Sql)
	}
	return nil
}

// SetSql set sql used to set blacklist(note: no persistence)
func (audit *Audit) SetSql(s *Sql) error {
	if s == nil {
		return errors.New("set nil sql")
	}
	if s.Query == "" {
		return errors.New("set sql with empty query")
	}
	audit.sqls.Store(s.Query, s)
	return nil
}

// DeteleSql delete specified sql in cache
func (audit *Audit) DeleteSql(query string) error {
	audit.sqls.Delete(query)
	return nil
}

// ClearSqls clear cached sqls
func (audit *Audit) ClearSqls() error {
	audit.sqls.Range(func(key interface{}, value interface{}) bool {
		audit.sqls.Delete(key)
		return true
	})
	return nil
}

func (audit *Audit) Before(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	return audit.before(ctx, query, args...)
}

// Before hook will print the query with it's args and return the context with the timestamp
func (audit *Audit) before(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	start := time.Now()
	if !audit.ShouldAudit(query) {
		return ctx, nil
	}
	defer func() {
		dur := time.Since(start).Seconds()
		auditMetrics.beforeDuration.Observe(dur)
	}()

	//auditMetrics.queryCount.With(audit.labels).Inc()
	//auditMetrics.queryInFlight.With(audit.labels).Inc()

	v, ok := audit.sqls.Load(query)
	if ok {
		s := v.(*Sql)
		if audit.SqlCacheDuration != nil && time.Since(s.CreatedAt) > audit.SqlCacheDuration.Duration+jitter(30) { // NOTE: jitter avoid invalidate too many at once!
			audit.sqls.Delete(query)
		} else {
			switch s.AlarmType {
			case Banned:
				//auditMetrics.bannedCount.With(audit.labels).Inc()
				if audit.SeenSqlLogLevel.Load() <= int32(Banned) {
					fields := append([]zap.Field{zap.String("query", query), zap.Error(ErrBanned), zap.Bool(alarmFieldName, true)}, audit.ContextLogFields(ctx)...)
					audit.logger.Error("seen banned query", fields...)
				}
				return ctx, errors.WithMessage(ErrBanned, query)
			case Alarm:
				//auditMetrics.alarmCount.With(audit.labels).Inc()
				if audit.SeenSqlLogLevel.Load() <= int32(Alarm) {
					fields := append([]zap.Field{zap.String("query", query), zap.Error(ErrAlarm), zap.Bool(alarmFieldName, true)}, audit.ContextLogFields(ctx)...)
					audit.logger.Error("seen alarm query", fields...)
				}
				return context.WithValue(ctx, startCtxKey{}, Now()), nil
			default:
				if audit.SeenSqlLogLevel.Load() <= int32(Normal) {
					fields := append([]zap.Field{zap.String("query", query)}, audit.ContextLogFields(ctx)...)
					audit.logger.Info("seen normal query", fields...)
					return context.WithValue(ctx, startCtxKey{}, Now()), nil
				}
				return ctx, nil
			}
		}
	}
	_, loaded := audit.sqls.LoadOrStore(query, &Sql{ // TODO: 定期(如10s)巡检mysql负载状态, 定义可放行阈值？此处目前先放行处理。
		Query:     query,
		Args:      args,
		Reason:    temporaryReason,
		CreatedAt: Now(),
	})
	if !loaded {
		audit.auditAsync(ctx, query, args...)
	}
	return ctx, nil
}

func (audit *Audit) auditAsync(ctx context.Context, query string, args ...interface{}) {
	go func() {
		defer func() {
			if p := recover(); p != nil {
				audit.sqls.Delete(query)
				err := fmt.Errorf("panic: %v;\nstack trace: %s", p, debug.Stack())
				audit.logger.Error("audit async paniced", zap.String("query", query), zap.Error(err))
				return
			}
		}()
		explainCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		ers, err := audit.Explain(explainCtx, query, args...)
		if err != nil {
			audit.sqls.Delete(query)
			audit.logger.Error("async explain failed", zap.Error(err), zap.String("query", query), zap.Bool(alarmFieldName, true))
			return
		}
		alarmType, reason := audit.DetectAlarmType(ers)
		audit.sqls.Store(query, &Sql{
			Query:     query,
			Args:      args,
			CreatedAt: Now(),
			AlarmType: alarmType,
			Reason:    reason,
			Explain:   ers,
		})
		fields := []zap.Field{
			zap.String("query", query),
		}
		if audit.ContextLogFields != nil {
			fields = append(fields, audit.ContextLogFields(ctx)...)
		}
		switch alarmType {
		case Banned:
			//auditMetrics.bannedCount.With(audit.labels).Inc()
			audit.logger.Error("new found banned query", append(fields, zap.Error(ErrBanned), zap.Bool(alarmFieldName, true))...)
			return
		case Alarm:
			//auditMetrics.alarmCount.With(audit.labels).Inc()
			audit.logger.Error("new found alarm query", append(fields, zap.Error(ErrAlarm), zap.Bool(alarmFieldName, true))...)
			return
		default:
			audit.logger.Info("new found normal query", fields...)
			return
		}
	}()
}

func jitter(n int) time.Duration {
	return time.Duration(rand.Intn(n)) * time.Second
}

func (audit *Audit) After(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	return audit.after(ctx, query, args...)
}

// After hook will get the timestamp registered on the Before hook and print the elapsed time
func (audit *Audit) after(ctx context.Context, query string, args ...interface{}) (context.Context, error) {
	defer func(start time.Time) {
		auditMetrics.afterDuration.Observe(time.Since(start).Seconds())
	}(time.Now())
	v := ctx.Value(startCtxKey{})
	if start, ok := v.(time.Time); ok {
		fields := []zap.Field{zap.String("query", query), zap.Duration("rt", time.Since(start))}
		audit.logger.Info("query rt", append(fields, audit.ContextLogFields(ctx)...)...)
	}
	//auditMetrics.queryInFlight.With(audit.labels).Dec()
	return ctx, nil
}

func (audit *Audit) ExecContext(next ExecContext) ExecContext {
	return func(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
		ifaceArgs := namedToInterface(args)
		ctx, err := audit.before(ctx, query, ifaceArgs...)
		if err != nil {
			return nil, err
		}
		results, err := next(ctx, query, args)
		if err != nil {
			return results, err
		}
		_, err = audit.after(ctx, query, ifaceArgs...)
		if err != nil {
			return nil, err
		}
		return results, err
	}
}

func (audit *Audit) QueryContext(next QueryContext) QueryContext {
	return func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
		ifaceArgs := namedToInterface(args)
		ctx, err := audit.before(ctx, query, ifaceArgs...)
		if err != nil {
			return nil, err
		}
		rows, err := next(ctx, query, args)
		if err != nil {
			return rows, err
		}
		_, err = audit.after(ctx, query, ifaceArgs...)
		if err != nil {
			return nil, err
		}
		return rows, err
	}
}

// DefaultShouldAudit sql是否审计的默认实现
func DefaultShouldAudit(query string) bool {
	query = strings.TrimSpace(query)
	if len(query) < 6 {
		return false
	}
	p := query[0:6]
	ef := strings.EqualFold
	return ef(p, "select") || ef(p, "insert") || ef(p, "update") || ef(p, "delete")
}

// current only used for internal
var auditMetrics = struct {
	init sync.Once
	// queryInFlight  *prometheus.GaugeVec
	// queryCount     *prometheus.CounterVec
	// alarmCount     *prometheus.CounterVec
	// bannedCount    *prometheus.CounterVec
	beforeDuration prometheus.Histogram
	afterDuration  prometheus.Histogram
}{
	init: sync.Once{},
}

func initAuditMetrics() {
	const ns, sub = "sqlkit", "audit"
	//labels := []string{"app", "database", "query", "alarmtype"}
	// auditMetrics.queryInFlight = promauto.NewGaugeVec(prometheus.GaugeOpts{
	// 	Namespace: ns,
	// 	Subsystem: sub,
	// 	Name:      "querys_in_flight",
	// 	Help:      "Number of querys currently beijing audited.",
	// }, labels)
	// auditMetrics.queryCount = promauto.NewCounterVec(prometheus.CounterOpts{
	// 	Namespace: ns,
	// 	Subsystem: sub,
	// 	Name:      "querys_total",
	// 	Help:      "Counter of querys audited.",
	// }, labels)
	// auditMetrics.alarmCount = promauto.NewCounterVec(prometheus.CounterOpts{
	// 	Namespace: ns,
	// 	Subsystem: sub,
	// 	Name:      "alarms_total",
	// 	Help:      "Counter of querys alarmed.",
	// }, labels)
	// auditMetrics.bannedCount = promauto.NewCounterVec(prometheus.CounterOpts{
	// 	Namespace: ns,
	// 	Subsystem: sub,
	// 	Name:      "banned_total",
	// 	Help:      "Counter of querys banned.",
	// }, labels)

	auditMetrics.beforeDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns,
		Subsystem: sub,
		Name:      "before_duration_seconds",
		Help:      "Histogram of query before phase durations.",
		Buckets:   []float64{1e-7, 2e-7, 5e-7, 1e-6, 2e-6, 5e-6, 1e-5, 1e-4, 1e-3},
	})
	auditMetrics.afterDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: ns,
		Subsystem: sub,
		Name:      "after_duration_seconds",
		Help:      "Histogram of query after phase durations.",
		Buckets:   []float64{1e-8, 2e-8, 5e-8, 1e-7, 2e-7, 5e-7, 1e-6, 1e-5, 1e-4, 1e-3},
	})
}

func MarshalMetric(name string) string {
	metric := &dto.Metric{}
	switch name {
	case "before_duration_seconds":
		auditMetrics.beforeDuration.Write(metric)
	case "after_duration_seconds":
		auditMetrics.afterDuration.Write(metric)
	}
	return proto.MarshalTextString(metric)
}

// ConfigAPI list config
func (audit *Audit) ConfigAPI(w http.ResponseWriter, r *http.Request) {
	render.R(renderName).OK(w, r, map[string]interface{}{
		"data": map[string]interface{}{
			"app":                         App,
			"database":                    audit.DatabaseName,
			"alarm_threshold":             audit.AlarmThreshold,
			"banned_threshold":            audit.BannedThreshold,
			"seen_sql_log_level":          audit.SeenSqlLogLevel.Load(),
			"whitelist":                   audit.Whitelists(),
			"sql_cache_duration":          audit.SqlCacheDuration,
			"explain_extra_alarm_substrs": audit.explainExtraAlarmSubstrs,
		},
	})
}

// TablesAPI list tables
func (audit *Audit) TablesAPI(w http.ResponseWriter, r *http.Request) {
	tables, err := audit.Tables(r.Context())
	if err != nil {
		render.R(renderName).Err(w, r, err)
		return
	}
	render.R(renderName).OK(w, r, map[string]interface{}{
		"data": map[string]interface{}{
			"app":      App,
			"database": audit.DatabaseName,
			"tables":   tables,
		},
	})
}

// SqlsAPI list sqls
func (audit *Audit) SqlsAPI(w http.ResponseWriter, r *http.Request) {
	render.R(renderName).OK(w, r, map[string]interface{}{
		"data": map[string]interface{}{
			"app":      App,
			"database": audit.DatabaseName,
			"sqls":     audit.Sqls(),
		},
	})
}

// MetricsAPI list metrics
func (audit *Audit) MetricsAPI(w http.ResponseWriter, r *http.Request) {
	render.R(renderName).OK(w, r, map[string]interface{}{
		"data": map[string]interface{}{
			"app":      App,
			"database": audit.DatabaseName,
			"metrics": map[string]string{
				"before_duration_seconds": MarshalMetric("before_duration_seconds"),
				"after_duration_seconds":  MarshalMetric("after_duration_seconds"),
			},
		},
	})
}

// SetSeenSqlLogLevelAPI set seen_sql_log_level
func (audit *Audit) SetSeenSqlLogLevelAPI(w http.ResponseWriter, r *http.Request) {
	levelStr := r.FormValue("seen_sql_log_level")
	level, err := strconv.ParseInt(levelStr, 10, 64)
	if err != nil {
		render.R(renderName).Err(w, r, errors.Adapt(err, errors.InvalidArgument))
		return
	}
	audit.SeenSqlLogLevel.Store(int32(level))
	render.R(renderName).OK(w, r, map[string]interface{}{
		"data": map[string]interface{}{
			"seen_sql_log_level": audit.SeenSqlLogLevel.Load(),
		},
	})
}

// WhitelistQueryAPI
func (audit *Audit) WhitelistAPI(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		render.R(renderName).Err(w, r, errors.Adapt(err, errors.Unknown))
		return
	}
	defer r.Body.Close()
	m := map[string]string{}
	err = json.Unmarshal(body, &m)
	if err != nil {
		render.R(renderName).Err(w, r, errors.Adapt(err, errors.Unknown))
		return
	}
	action := r.FormValue("action")
	query := m["query"]
	if DefaultShouldAudit(query) {
		switch action {
		case "add":
			audit.AddWhitelistQuery(query)
		case "delete":
			audit.DelWhitelistQuery(query)
		}
	} else {
		err := errors.Errorf("query: %s will not be audited", query)
		render.R(renderName).Err(w, r, errors.Adapt(err, errors.InvalidArgument))
		return
	}
	render.R(renderName).OK(w, r, map[string]interface{}{
		"data": map[string]interface{}{
			"whitelist": audit.Whitelists(),
		},
	})
}

// BlacklistQueryAPI
func (audit *Audit) BlacklistAPI(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		render.R(renderName).Err(w, r, errors.Adapt(err, errors.Unknown))
		return
	}
	defer r.Body.Close()
	m := BlacklistRequest{}
	err = json.Unmarshal(body, &m)
	if err != nil {
		render.R(renderName).Err(w, r, errors.Adapt(err, errors.Unknown))
		return
	}
	action := r.FormValue("action")
	query := m.Query
	if DefaultShouldAudit(query) {
		switch action {
		case "add":
			audit.AddBlacklistQuery(query, m.AlarmType, m.Reason)
		}
	} else {
		err := errors.Errorf("query: %s will not be audited", query)
		render.R(renderName).Err(w, r, errors.Adapt(err, errors.InvalidArgument))
		return
	}
	render.R(renderName).OK(w, r, map[string]interface{}{
		"data": map[string]interface{}{
			query: audit.GetSql(query),
		},
	})
}

type BlacklistRequest struct {
	Query     string    `json:"query"`
	AlarmType AlarmType `json:"alarm_type"`
	Reason    string    `json:"reason"`
}

func (br BlacklistRequest) String() string {
	return fmt.Sprintf("query:%s;alarm_type:%s;reason:%s", br.Query, br.AlarmType, br.Reason)
}

type startCtxKey struct{}

var (
	alarmFieldName = "alarm"
	renderName     = "json"
)

const temporaryReason = "__temporary"

var (
	_ Middleware     = (*Audit)(nil)
	_ sqlhooks.Hooks = (*Audit)(nil)
)
