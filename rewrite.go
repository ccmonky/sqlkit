package sqlkit

import (
	"context"
	"strings"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pkg/errors"
)

type ShadowTable struct {
	Prefix string `json:"prefix,omitempty"`
	Suffix string `json:"suffix,omitempty"`
	//Logger *zap.Logger

	parser *parser.Parser
}

func (st *ShadowTable) Provision(ctx context.Context) error {
	if st.Prefix == "" && st.Suffix == "" {
		return errors.New("shadow table with empty prefix and suffix")
	}
	st.parser = parser.New()
	return nil
}

func (st *ShadowTable) Enter(in ast.Node) (ast.Node, bool) {
	if tn, ok := in.(*ast.TableName); ok {
		tn.Name = model.NewCIStr(st.Prefix + tn.Name.String() + st.Suffix)
	}
	return in, false
}

func (st *ShadowTable) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func (st *ShadowTable) Rewrite(sql string) (string, error) {
	stmtNodes, warns, err := st.parser.Parse(sql, "", "")
	if err != nil {
		return "", errors.WithMessagef(err, "parser sql faield: %s", sql)
	}
	if len(warns) > 0 {
		//st.Logger.Debug("shadow table warnings", zap.Any("warns", warns), zap.String("sql", sql))
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
	return sb.String(), nil
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
