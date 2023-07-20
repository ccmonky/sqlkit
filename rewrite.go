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

type ShadowTable struct {
	Prefix string `json:"prefix,omitempty"`
	Suffix string `json:"suffix,omitempty"`
	Logger *zap.Logger

	parser *parser.Parser
	cache  sync.Map
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

func (st *ShadowTable) Rewrite(sql string) (string, error) {
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

// Rewrite used to rewrite sql
// TODO ...
type Rewrite struct {
	Query  string `json:"query"`
	ArgOps map[uint]ArgOp
}

func (r Rewrite) Args(args ...interface{}) ([]interface{}, error) {
	if r.ArgOps == nil {
		return args, nil
	}
	var result []interface{}
	for i := range args {
		if op, ok := r.ArgOps[uint(i)]; ok {
			if op == DelOp {
				continue
			}
		}
		result = append(result, args[i])
	}
	return result, nil
}

type ArgOp int

const (
	NoOp ArgOp = iota
	DelOp
)
