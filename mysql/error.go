package mysql

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

var (
	IsMySQLDuplicateError = IsMySQLError(uint16(1062))
)

func IsMySQLError(code uint16) func(error) bool {
	return func(err error) bool {
		var myErr *mysql.MySQLError
		if errors.As(err, &myErr) {
			return myErr.Number == code
		}
		return false
	}
}
