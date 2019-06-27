package datautils

import (
	"reflect"
	"strings"

	cfg "github.com/eaglebush/config"
	dh "github.com/eaglebush/datahelper"
	dt "github.com/eaglebush/datatable"
)

// QueryResult - the result of the query
type QueryResult struct {
	OK              bool
	Data            []dt.Row
	HasData         bool
	HasAffectedRows bool
}

// BatchQuery - the BatchQuery struct
type BatchQuery struct {
	internDH          *dh.DataHelper
	connected         bool
	errorText         string
	scopeActionNumber int
	actionNumber      int
	scopeName         string
	lastQuery         string
}

// NewBatchQuery - create a new BatchQuery object
func NewBatchQuery(config *cfg.Configuration) *BatchQuery {
	ms := &BatchQuery{scopeName: "main", lastQuery: ""}
	ms.internDH = dh.NewDataHelper(config)

	return ms
}

// Connect - connect the BatchQuery to the databse
func (m *BatchQuery) Connect(connectionID string) bool {
	m.errorText = ""
	m.lastQuery = ""
	m.actionNumber = 1
	m.scopeActionNumber = 1

	connected, err := m.internDH.Connect(connectionID)
	if err != nil {
		m.errorText = err.Error()
		return false
	}
	m.connected = connected
	return m.connected
}

// Disconnect - disconnect from the dataabse
func (m *BatchQuery) Disconnect() {
	m.actionNumber = 0
	m.scopeActionNumber = 0
	m.errorText = ""
	m.lastQuery = ""
	m.internDH.Disconnect()
}

// Get - get data from the database
func (m *BatchQuery) Get(preparedSQL string, args ...interface{}) QueryResult {
	if m.errorText != "" {
		return QueryResult{}
	}

	if !m.connected {
		m.errorText = "Not connected"
		return QueryResult{}
	}

	m.actionNumber++
	m.scopeActionNumber++
	m.lastQuery = preparedSQL

	dtr, err := m.internDH.GetData(preparedSQL, args...)
	if err != nil {
		m.errorText = err.Error()
		return QueryResult{}
	}

	return QueryResult{
		HasData: dtr.RowCount > 0,
		Data:    dtr.Rows,
		OK:      true,
	}
}

// Set - set data in the database
func (m *BatchQuery) Set(preparedSQL string, args ...interface{}) QueryResult {
	if m.errorText != "" {
		return QueryResult{}
	}

	if !m.connected {
		m.errorText = "Not connected"
		return QueryResult{}
	}

	m.actionNumber++
	m.scopeActionNumber++
	m.lastQuery = preparedSQL

	sqr, err := m.internDH.Exec(preparedSQL, args...)
	if err != nil {
		m.errorText = err.Error()
		return QueryResult{}
	}

	var i int64
	ra, _ := sqr.RowsAffected()
	li, _ := sqr.LastInsertId()

	dtr := dt.DataTable{}
	dtr.AddColumn("Affected", reflect.TypeOf(i), 0, "int")
	dtr.AddColumn("LastInsertId", reflect.TypeOf(i), 0, "int")

	r := dtr.NewRow()
	r.SetValueByOrd(ra, 0)
	r.SetValueByOrd(li, 1)
	dtr.AddRow(&r)

	return QueryResult{
		HasAffectedRows: ra != 0,
		HasData:         dtr.RowCount > 0,
		Data:            dtr.Rows,
		OK:              true,
	}
}

// Do - execute a stored procedure
func (m *BatchQuery) Do(preparedSQL string, args ...interface{}) QueryResult {
	if m.errorText != "" {
		return QueryResult{}
	}

	if !m.connected {
		m.errorText = "Not connected"
		return QueryResult{}
	}

	pl := strings.ToLower(preparedSQL)
	pl = strings.TrimPrefix(pl, "")
	if pl != "exec" && pl != "execute" {
		preparedSQL = "EXECUTE " + preparedSQL
	}

	m.actionNumber++
	m.scopeActionNumber++
	m.lastQuery = preparedSQL

	// Stored proc may have data returned
	dtr, err := m.internDH.GetData(preparedSQL, args...)
	if err != nil {
		m.errorText = err.Error()
		return QueryResult{}
	}
	return QueryResult{
		HasData: dtr.RowCount > 0,
		Data:    dtr.Rows,
		OK:      true,
	}
}

// Begin - begin a transaction
func (m *BatchQuery) Begin() {
	m.actionNumber++
	m.scopeActionNumber++

	_, err := m.internDH.Begin()
	if err != nil {
		m.errorText = err.Error()
	}
}

// Rollback - rollback a transaction
func (m *BatchQuery) Rollback() {
	m.actionNumber++
	m.scopeActionNumber++

	err := m.internDH.Rollback()
	if err != nil {
		m.errorText = err.Error()
	}
}

// Commit - commit a transaction
func (m *BatchQuery) Commit() {
	m.actionNumber++
	m.scopeActionNumber++

	err := m.internDH.Commit()
	if err != nil {
		m.errorText = err.Error()
	}
}

// OK - checks if all queries are OK
func (m *BatchQuery) OK() bool {
	return m.errorText == ""
}

// Waive - resets the error message back to zero to allow sending query again
func (m *BatchQuery) Waive() {
	m.errorText = ""
}

// Settings - returns the internal datahelper settings
func (m *BatchQuery) Settings() cfg.Configuration {
	return m.internDH.Settings
}

// ScopeName - name of a function where this query is currently running for debugging purposes. This must be set before any query is executed. The default scope name is 'main'
func (m *BatchQuery) ScopeName(scopeName string) {
	m.scopeActionNumber = 0
	m.scopeName = scopeName
}

// LastScopeName - returns the name of the last scope
func (m *BatchQuery) LastScopeName() string {
	return m.scopeName
}

// LastScopeActionNumber - returns the last scope action number
func (m *BatchQuery) LastScopeActionNumber() int {
	return m.scopeActionNumber
}

// LastActionNumber - returns the last action number
func (m *BatchQuery) LastActionNumber() int {
	return m.actionNumber
}

// LastErrorText - returns the last error text
func (m *BatchQuery) LastErrorText() string {
	return m.errorText
}

// LastQuery - returns the last query
func (m *BatchQuery) LastQuery() string {
	return m.lastQuery
}

// Get - shortcut to get row
func (q *QueryResult) Get(rowIndex int) *dt.Row {
	rc := len(q.Data)

	if rc == 0 {
		return nil
	}

	if rowIndex > rc {
		return nil
	}

	return &q.Data[rowIndex]
}

// First - shortcut to get the first row row
func (q *QueryResult) First() *dt.Row {
	rc := len(q.Data)

	if rc == 0 {
		return nil
	}

	return &q.Data[0]
}
