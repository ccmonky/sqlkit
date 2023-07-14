package mysql_test

import (
	"testing"

	my "github.com/ccmonky/sqlkit/mysql"
	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
)

func TestIsMySQLDuplicateError(t *testing.T) {
	err := &mysql.MySQLError{
		Number: uint16(1062),
	}
	assert.Truef(t, my.IsMySQLDuplicateError(err), "got %v", my.IsMySQLDuplicateError(err))
	err.Number = uint16(1061)
	assert.False(t, my.IsMySQLDuplicateError(err))
	assert.False(t, my.IsMySQLDuplicateError(nil))
}
