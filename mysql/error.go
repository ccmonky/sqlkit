package mysql

import (
	"github.com/go-sql-driver/mysql"
)

var (
	IsMySQLDuplicateError = IsMySQLError(uint16(1062))
)

func IsMySQLError(code uint16) func(error) bool {
	return func(err error) bool {
		entErr, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		myErr, ok := entErr.Unwrap().(*mysql.MySQLError)
		if !ok {
			return false
		}
		if myErr.Number == code { // Duplicate entry
			return true
		}
		return false
	}
}
