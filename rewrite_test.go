package sqlkit_test

import (
	"context"
	"testing"

	"github.com/ccmonky/sqlkit"
	"github.com/stretchr/testify/assert"
)

func TestShadowTable(t *testing.T) {
	st := sqlkit.ShadowTable{
		Suffix: "_shadow",
	}
	err := st.Provision(context.Background())
	assert.Nil(t, err)
	var cases = []struct {
		sql    string
		shadow string
	}{
		{
			sql:    "select * from t where a = ?",
			shadow: "SELECT * FROM t_shadow WHERE a=?",
		},
		{
			sql:    "SELECT DISTINCT `classes`.`id`, `classes`.`create_time`, `classes`.`update_time`, `classes`.`name`, `classes`.`version`, `classes`.`level`, `classes`.`num`, `classes`.`class_type`, `classes`.`support_tid`, `classes`.`reverse_search_status`, `classes`.`lng`, `classes`.`lat`, `classes`.`scale`, `classes`.`pitch_angle`, `classes`.`uid`, `classes`.`bind_card`, `classes`.`unbinding`, `classes`.`has_sub`, `classes`.`class_library_id`, `classes`.`style_category_id`, `classes`.`parent_id`, `classes`.`bind_card_unbind` FROM `classes` WHERE `classes`.`id` = ?",
			shadow: "SELECT DISTINCT classes_shadow.id,classes_shadow.create_time,classes_shadow.update_time,classes_shadow.name,classes_shadow.version,classes_shadow.level,classes_shadow.num,classes_shadow.class_type,classes_shadow.support_tid,classes_shadow.reverse_search_status,classes_shadow.lng,classes_shadow.lat,classes_shadow.scale,classes_shadow.pitch_angle,classes_shadow.uid,classes_shadow.bind_card,classes_shadow.unbinding,classes_shadow.has_sub,classes_shadow.class_library_id,classes_shadow.style_category_id,classes_shadow.parent_id,classes_shadow.bind_card_unbind FROM classes_shadow WHERE classes_shadow.id=?",
		},
	}
	for _, tc := range cases {
		s, err := st.Rewrite(tc.sql)
		assert.Nil(t, err)
		assert.Equal(t, tc.shadow, s)
	}
}

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
