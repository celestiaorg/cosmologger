package database

import (
	"database/sql"
	"fmt"
	"strings"
)

/*-----------------------*/

func New(DatabaseType DBType, params ...string) *Database {
	var newDB Database

	newDB.Type = DatabaseType

	switch DatabaseType {
	case Postgres:
		if len(params) == 0 {
			return nil
		}
		newDB.SQLConn = NewPostgresDB(params[0])
		// newDB.PostgresInit()
	}

	return &newDB
}

/*-----------------------*/

func (db *Database) Close() {
	switch db.Type {
	case Postgres:
		db.PostgresClose()
	}
}

/*-----------------------*/

func (db *Database) BatchInsert(table string, bulkFields ...RowType) (ExecResult, error) {
	switch db.Type {
	case Postgres:
		if len(bulkFields) == 0 {
			return ExecResult{}, nil
		}

		fieldNames := make([]string, len(bulkFields[0]))
		values := make([][]interface{}, len(bulkFields))

		i := 0
		for fieldName, _ := range bulkFields[0] {
			fieldNames[i] = fieldName
			i++
		}

		for i, fields := range bulkFields {
			values[i] = make([]interface{}, len(bulkFields[0]))
			for j, fieldName := range fieldNames {
				values[i][j] = fields[fieldName]
			}
		}

		return db.PostgresBatchInsert(table, fieldNames, values)
	}
	return ExecResult{}, fmt.Errorf("no db.Type is set")
}

func (db *Database) Insert(table string, fields RowType) (ExecResult, error) {
	switch db.Type {
	case Postgres:
		fieldNames := make([]string, len(fields))
		values := make([]interface{}, len(fields))

		i := 0
		for fieldName, value := range fields {
			fieldNames[i] = fieldName
			values[i] = value
			i++
		}

		batchValues := make([][]interface{}, 1)
		batchValues[0] = values

		return db.PostgresBatchInsert(table, fieldNames, batchValues)
	}
	return ExecResult{}, fmt.Errorf("no db.Type is set")
}

/*-----------------------*/

func (db *Database) Update(table string, fields RowType, conditions RowType) (ExecResult, error) {

	switch db.Type {
	case Postgres:
		return db.PostgresUpdate(table, fields, conditions)
	}

	return ExecResult{}, fmt.Errorf("no db.Type is set")
}

/*-----------------------*/

func (db *Database) Delete(table string, conditions RowType) (ExecResult, error) {

	switch db.Type {
	case Postgres:
		return db.PostgresDelete(table, conditions)
	}

	return ExecResult{}, fmt.Errorf("no db.Type is set")
}

/*-----------------------*/

func (db *Database) Load(table string, searchOnFields RowType) (QueryResult, error) {

	switch db.Type {
	case Postgres:
		return db.PostgresLoad(table, searchOnFields)
	}

	return QueryResult{}, fmt.Errorf("no db.Type is set")

}

/*-----------------------*/

func (db *Database) Query(query string, params QueryParams) (QueryResult, error) {

	switch db.Type {
	case Postgres:
		return db.PostgresQuery(query, params)
	}

	return QueryResult{}, fmt.Errorf("no db.Type is set")

}

/*-----------------------*/

func (db *Database) Exec(query string, params QueryParams) (ExecResult, error) {

	switch db.Type {
	case Postgres:
		return db.PostgresExec(query, params...)
	}

	return ExecResult{}, fmt.Errorf("no db.Type is set")

}

func (db *Database) IsErrNotFound(err error) bool {

	if err == nil {
		return false
	}

	switch db.Type {
	case Postgres:
		return sql.ErrNoRows == err
	}

	return false
}

func (db *Database) IsErrDuplicate(err error) bool {

	if err == nil {
		return false
	}

	switch db.Type {
	case Postgres:
		return strings.Contains(err.Error(), "duplicate key value")
	}

	return false
}
