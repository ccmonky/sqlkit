package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func NewMySQL(db *sql.DB) MySQL {
	return MySQL{db}
}

type MySQL struct {
	*sql.DB
}

// GetTables get table from mysql `INFORMATION_SCHEMA.TABLES`
func (mysql MySQL) GetTables(ctx context.Context, databaseName string) (map[string]*Table, error) {
	var tables = make(map[string]*Table)
	rows, err := mysql.DB.QueryContext(ctx, TablesQuery, databaseName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			tableName string
			count     int64
		)
		if err := rows.Scan(&tableName, &count); err != nil {
			return nil, err
		}
		tables[tableName] = &Table{
			Name:  tableName,
			Count: count,
		}
	}
	return tables, rows.Err()
}

// Table database table
type Table struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

// Explain execute `explain query`
func (mysql MySQL) Explain(ctx context.Context, query string, args ...interface{}) ([]ExplainRow, error) {
	rows, err := mysql.DB.QueryContext(ctx, "explain "+query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ers []ExplainRow
	for rows.Next() {
		var (
			id           int
			selectType   string
			table        *string
			partitions   *string
			typ          *string
			possibleKeys *string
			key          *string
			keyLen       *int
			ref          *string
			nrows        *int
			filtered     *float32
			extra        *string
		)
		err := rows.Scan(&id, &selectType, &table, &partitions, &typ, &possibleKeys, &key, &keyLen, &ref, &nrows, &filtered, &extra)
		if err != nil {
			return nil, err
		}
		er := ExplainRow{
			ID:           id,
			SelectType:   selectType,
			Table:        table,
			Partitions:   partitions,
			Type:         typ,
			PossibleKeys: possibleKeys,
			Key:          key,
			KeyLen:       keyLen,
			Ref:          ref,
			Rows:         nrows,
			Filtered:     filtered,
			Extra:        extra,
		}
		ers = append(ers, er)
	}
	return ers, nil
}

type ExplainRow struct {
	ID           int      `json:"id"`
	SelectType   string   `json:"select_type"`
	Table        *string  `json:"table"`
	Partitions   *string  `json:"partitions"`
	Type         *string  `json:"type"`
	PossibleKeys *string  `json:"possible_keys"`
	Key          *string  `json:"key"`
	KeyLen       *int     `json:"key_len"`
	Ref          *string  `json:"ref"`
	Rows         *int     `json:"rows"`
	Filtered     *float32 `json:"filtered"`
	Extra        *string  `json:"extra"`
}

func (er ExplainRow) String() string {
	b := strings.Builder{}
	null := fmt.Sprintln("NULL")
	b.WriteString("           id: " + fmt.Sprintln(er.ID))
	b.WriteString("  select_type: " + fmt.Sprintln(er.SelectType))
	if er.Table == nil {
		b.WriteString("        table: " + null)
	} else {
		b.WriteString("        table: " + fmt.Sprintln(*er.Table))
	}
	if er.Partitions == nil {
		b.WriteString("   partitions: " + null)
	} else {
		b.WriteString("   partitions: " + fmt.Sprintln(*er.Partitions))
	}
	if er.Type == nil {
		b.WriteString("         type: " + null)
	} else {
		b.WriteString("         type: " + fmt.Sprintln(*er.Type))
	}
	if er.PossibleKeys == nil {
		b.WriteString("possible_keys: " + null)
	} else {
		b.WriteString("possible_keys: " + fmt.Sprintln(*er.PossibleKeys))
	}
	if er.Key == nil {
		b.WriteString("          key: " + null)
	} else {
		b.WriteString("          key: " + fmt.Sprintln(*er.Key))
	}
	if er.KeyLen == nil {
		b.WriteString("      key_len: " + null)
	} else {
		b.WriteString("      key_len: " + fmt.Sprintln(*er.KeyLen))
	}
	if er.Ref == nil {
		b.WriteString("          ref: " + null)
	} else {
		b.WriteString("          ref: " + fmt.Sprintln(*er.Ref))
	}
	if er.Rows == nil {
		b.WriteString("         rows: " + null)
	} else {
		b.WriteString("         rows: " + fmt.Sprintln(*er.Rows))
	}
	if er.Filtered == nil {
		b.WriteString("     filtered: " + null)
	} else {
		b.WriteString("     filtered: " + fmt.Sprintln(*er.Filtered))
	}
	if er.Extra == nil {
		b.WriteString("        Extra: " + null)
	} else {
		b.WriteString("        Extra: " + fmt.Sprintln(*er.Extra))
	}
	return b.String()
}

func (mysql MySQL) Trace(ctx context.Context, query string, args ...interface{}) (*Trace, error) {
	return nil, nil
}

type Trace struct {
	// ...
}

func (mysql MySQL) Profile(ctx context.Context, query string, args ...interface{}) (*Profile, error) {
	return nil, nil
}

type Profile struct {
	// ...
}

func (mysql MySQL) Trxs(ctx context.Context, query string, args ...interface{}) ([]Trx, error) {
	return nil, nil
}

// https://dev.mysql.com/doc/refman/8.0/en/information-schema-innodb-trx-table.html
type Trx struct {
	// ...
}

func (mysql MySQL) Locks(ctx context.Context, query string, args ...interface{}) ([]Lock, error) {
	return nil, nil
}

type Lock struct {
}

func (mysql MySQL) ProcessList(ctx context.Context, query string, args ...interface{}) (*ProcessList, error) {
	return nil, nil
}

type ProcessList struct {
	// ...
}

// GetCharacterSets get session character set from mysql `performance_schema.session_variables`
func (mysql MySQL) GetCharacterSetVars(ctx context.Context) (map[string]*Var, error) {
	var vars = make(map[string]*Var)
	rows, err := mysql.DB.QueryContext(ctx, CharacterSetVarsQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			name  string
			value string
		)
		if err := rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		vars[name] = &Var{
			Name:  name,
			Value: value,
		}
	}
	return vars, rows.Err()
}

// Var variable
type Var struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

var (
	TablesQuery = "SELECT table_name, table_rows FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?;"

	// CharacterSetVarsQuery see the values of the character set and collation system variables that apply to the current session
	CharacterSetVarsQuery = `SELECT * FROM performance_schema.session_variables
    WHERE VARIABLE_NAME IN (
        'character_set_client', 
        'character_set_connection',
        'character_set_results', 
        'collation_connection'
    ) ORDER BY VARIABLE_NAME;`
)
