package sqlkit

import (
	"context"
	"strings"
	"sync"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Rewrite used to rewrite sql and args
type Rewrite struct {
	GlobalRewriter  *Rewriter            `json:"global_rewriter,omitempty"`
	CustomRewriters map[string]*Rewriter `json:"custom_rewriters,omitempty"`
}

func (r Rewrite) Rewrite(sql string, args []any) (string, []any, error) {
	var err error
	if r.GlobalRewriter != nil {
		sql, args, err = r.GlobalRewriter.Rewrite(sql, args)
		if err != nil {
			return "", nil, errors.WithMessagef(err, "global rewrite failed: %s: %s", r.GlobalRewriter.Name(), sql)
		}
	}
	if r.CustomRewriters != nil {
		if cr, ok := r.CustomRewriters[sql]; ok {
			sql, args, err = cr.Rewrite(sql, args)
			if err != nil {
				return "", nil, errors.WithMessagef(err, "custom rewrite failed: %s: %s", cr.Name(), sql)
			}
		}
	}
	return sql, args, nil
}

type Rewriter struct {
	SqlRewriters  []SqlRewriter  `json:"sql_rewriters,omitempty"`
	ArgsRewriters []ArgsRewriter `json:"args_rewriters,omitempty"`
}

func (rr Rewriter) Name() string {
	var srName, arName string
	for i, sr := range rr.SqlRewriters {
		if i == len(rr.SqlRewriters)-1 {
			srName += sr.Name()
		} else {
			srName += sr.Name() + "->"
		}
	}

	for i, ar := range rr.ArgsRewriters {
		if i == len(rr.ArgsRewriters)-1 {
			arName += ar.Name()
		} else {
			arName += ar.Name() + "->"
		}
	}
	return "[sql]%s[args]%s"
}

func (rr Rewriter) Rewrite(sql string, args []any) (string, []any, error) {
	var err error
	for _, sr := range rr.SqlRewriters {
		sql, err = sr.RewriteSql(sql)
		if err != nil {
			return sql, args, errors.WithMessagef(err, "rewrite sql failed: %s: %s", sr.Name(), sql)
		}
	}
	for _, ar := range rr.ArgsRewriters {
		args, err = ar.RewriteArgs(args)
		if err != nil {
			return sql, args, errors.WithMessagef(err, "rewrite args failed: %s: %v", ar.Name(), args)
		}
	}
	return sql, args, nil
}

type SqlRewriter interface {
	Name() string
	RewriteSql(sql string) (string, error)
}

type ArgsRewriter interface {
	Name() string
	RewriteArgs(args []any) ([]any, error)
}

type ShadowTable struct {
	Prefix string `json:"prefix,omitempty"`
	Suffix string `json:"suffix,omitempty"`
	Logger *zap.Logger

	parser *parser.Parser
	cache  sync.Map
}

func (st *ShadowTable) Name() string {
	return "shadow_table"
}

func (st *ShadowTable) Provision(ctx context.Context) error {
	if st.Prefix == "" && st.Suffix == "" {
		return errors.New("shadow table with empty prefix and suffix")
	}
	st.parser = parser.New()
	return nil
}

func (st *ShadowTable) Enter(in ast.Node) (ast.Node, bool) {
	switch n := in.(type) {
	case *ast.TableName:
		n.Name = model.NewCIStr(st.Prefix + n.Name.String() + st.Suffix)
	case *ast.TableSource:
		if n.AsName.String() != "" {
			n.AsName = model.NewCIStr(st.Prefix + n.AsName.String() + st.Suffix)
		}
	case *ast.ColumnName:
		if n.Table.String() != "" {
			n.Table = model.NewCIStr(st.Prefix + n.Table.String() + st.Suffix)
		}
	}
	return in, false
}

func (st *ShadowTable) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func (st *ShadowTable) RewriteSql(sql string) (string, error) {
	if result, ok := st.cache.Load(sql); ok {
		return result.(string), nil
	}
	stmtNodes, warns, err := st.parser.Parse(sql, "", "")
	if err != nil {
		return "", errors.WithMessagef(err, "parser sql faield: %s", sql)
	}
	if len(warns) > 0 && st.Logger != nil {
		st.Logger.Debug("shadow table warnings", zap.Any("warns", warns), zap.String("sql", sql))
	}
	stmtNode := stmtNodes[0]
	switch stmtNode.(type) {
	case *ast.SelectStmt, *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt:
	default:
		return sql, nil // FIXME
	}
	node, accept := stmtNode.Accept(st)
	if !accept {
		return "", errors.WithMessagef(err, "accept failed for sql: %s", sql)
	}
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.RestoreKeyWordUppercase, &sb)
	err = node.Restore(ctx)
	if err != nil {
		return "", errors.WithMessagef(err, "restore failed for sql: %s; ctx: %v", sql, ctx)
	}
	st.cache.Store(sql, sb.String())
	return sb.String(), nil
}

func (st *ShadowTable) Sqls() map[string]string {
	snapshot := make(map[string]string)
	st.cache.Range(func(k, v any) bool {
		snapshot[k.(string)] = v.(string)
		return true
	})
	return snapshot
}

var (
	_ SqlRewriter = (*ShadowTable)(nil)
)
