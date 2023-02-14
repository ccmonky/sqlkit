package sqlkit_test

import (
	"testing"

	"github.com/ccmonky/sqlkit"
	"github.com/stretchr/testify/assert"
)

func TestRewrite(t *testing.T) {
	r := sqlkit.Rewrite{
		Query: "select * from data where app_name='xxx' and version=?",
		ArgOps: map[uint]sqlkit.ArgOp{
			1: sqlkit.DelOp,
		},
	}

	args, err := r.Args("0.1.0")
	assert.Nilf(t, err, "args err")
	assert.Equalf(t, 1, len(args), "args result len")
	assert.Equalf(t, "0.1.0", args[0], "args result len")

	args, err = r.Args("0.1.0", "to-be-deleted")
	assert.Nilf(t, err, "args err")
	assert.Equalf(t, 1, len(args), "args result len")
	assert.Equalf(t, "0.1.0", args[0], "args result len")

	r.ArgOps = map[uint]sqlkit.ArgOp{
		1: sqlkit.DelOp,
		3: sqlkit.DelOp,
	}
	args, err = r.Args("0.1.0", "to-be-deleted", 1, true, "abc", 3.4)
	assert.Nilf(t, err, "args err")
	assert.Equalf(t, 4, len(args), "args result len")
	assert.Equalf(t, "0.1.0", args[0], "args result len")
	assert.Equalf(t, 1, args[1], "args result len")
	assert.Equalf(t, "abc", args[2], "args result len")
	assert.Equalf(t, 3.4, args[3], "args result len")
}