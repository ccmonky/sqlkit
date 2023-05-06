package mysql_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/ccmonky/sqlkit/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func TestMySQL(t *testing.T) {
	db, err := sql.Open("mysql", "root@tcp(localhost:3306)/demo?parseTime=True&loc=Local") // MySQL 5.5.3+
	if err != nil {
		t.Fatal(err)
	}
	my := &mysql.MySQL{
		DB: db,
	}
	testGetCharacterSetVars(t, my)
}

// Since version 1.5 Go-MySQL-Driver automatically uses the collation  utf8mb4_general_ci by default.
// If you specify charset=utf8 in dsn, then character_set_xxx will be utf8mb3 and collation will be utf8mb3_general_ci,
// thus when you use chinese characters will got error like this:
//
//     `Error 3988: Conversion from collation utf8_general_ci into utf8mb4_general_ci impossible for parameter`
//
func testGetCharacterSetVars(t *testing.T, my *mysql.MySQL) {
	vars, err := my.GetCharacterSetVars(context.Background())
	assert.Nil(t, err)
	assert.Equalf(t, "utf8mb4", vars["character_set_client"].Value, "character_set_client")
	assert.Equalf(t, "utf8mb4", vars["character_set_connection"].Value, "character_set_connection")
	assert.Equalf(t, "utf8mb4", vars["character_set_results"].Value, "character_set_results")
	assert.Equalf(t, "utf8mb4_general_ci", vars["collation_connection"].Value, "collation_connection")
}
