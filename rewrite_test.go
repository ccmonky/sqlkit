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
		{
			sql:    "SELECT DISTINCT `templates`.`id`, `templates`.`create_time`, `templates`.`update_time`, `templates`.`version`, `templates`.`name`, `templates`.`description`, `templates`.`user_name`, `templates`.`last_user_name`, `templates`.`uid`, `templates`.`last_uid`, `templates`.`state`, `templates`.`hardware`, `templates`.`fixed_desc`, `templates`.`engine_file_suffix`, `templates`.`scale`, `templates`.`category_id`, `templates`.`level_id`, `templates`.`file_name_library_id`, `templates`.`source_library_id`, `templates`.`class_library_id`, `templates`.`maps_category_id`, `templates`.`source_link` FROM `templates` JOIN (SELECT `template_id` FROM `maps` WHERE `id` = ?) AS `t1` ON `templates`.`id` = `t1`.`template_id`",
			shadow: "SELECT DISTINCT templates_shadow.id,templates_shadow.create_time,templates_shadow.update_time,templates_shadow.version,templates_shadow.name,templates_shadow.description,templates_shadow.user_name,templates_shadow.last_user_name,templates_shadow.uid,templates_shadow.last_uid,templates_shadow.state,templates_shadow.hardware,templates_shadow.fixed_desc,templates_shadow.engine_file_suffix,templates_shadow.scale,templates_shadow.category_id,templates_shadow.level_id,templates_shadow.file_name_library_id,templates_shadow.source_library_id,templates_shadow.class_library_id,templates_shadow.maps_category_id,templates_shadow.source_link FROM templates_shadow JOIN (SELECT template_id FROM maps_shadow WHERE id=?) AS t1_shadow ON templates_shadow.id=t1_shadow.template_id",
		},
	}
	for _, tc := range cases {
		s, err := st.Rewrite(tc.sql)
		assert.Nil(t, err)
		assert.Equal(t, tc.shadow, s)
	}
	assert.Len(t, st.Sqls(), 3)
}

func BenchmarkRegisterTypeMultiple(b *testing.B) {
	st := sqlkit.ShadowTable{
		Suffix: "_s",
	}
	err := st.Provision(context.Background())
	assert.Nil(b, err)
	sql := "SELECT DISTINCT `templates`.`id`, `templates`.`create_time`, `templates`.`update_time`, `templates`.`version`, `templates`.`name`, `templates`.`description`, `templates`.`user_name`, `templates`.`last_user_name`, `templates`.`uid`, `templates`.`last_uid`, `templates`.`state`, `templates`.`hardware`, `templates`.`fixed_desc`, `templates`.`engine_file_suffix`, `templates`.`scale`, `templates`.`category_id`, `templates`.`level_id`, `templates`.`file_name_library_id`, `templates`.`source_library_id`, `templates`.`class_library_id`, `templates`.`maps_category_id`, `templates`.`source_link` FROM `templates` JOIN (SELECT `template_id` FROM `maps` WHERE `id` = ?) AS `t1` ON `templates`.`id` = `t1`.`template_id`"
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, _ = st.Rewrite(sql)
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
