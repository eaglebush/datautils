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
	Error             string
	ScopeActionNumber int
	ActionNumber      int
	scopeName         string
}

// NewBatchQuery - create a new BatchQuery object
func NewBatchQuery(config *cfg.Configuration) *BatchQuery {
	ms := &BatchQuery{scopeName: "main"}
	ms.internDH = dh.NewDataHelper(config)

	return ms
}

// Connect - connect the BatchQuery to the databse
func (m *BatchQuery) Connect(connectionID string) bool {
	m.Error = ""
	m.ActionNumber = 1
	m.ScopeActionNumber = 1

	connected, err := m.internDH.Connect(connectionID)
	if err != nil {
		m.Error = err.Error()
		return false
	}
	m.connected = connected
	return m.connected
}

// Disconnect - disconnect from the dataabse
func (m *BatchQuery) Disconnect() {
	m.ActionNumber = 0
	m.ScopeActionNumber = 0
	m.internDH.Disconnect()
}

// Get - get data from the database
func (m *BatchQuery) Get(preparedSQL string, args ...interface{}) QueryResult {
	if m.Error != "" {
		return QueryResult{}
	}

	if !m.connected {
		m.Error = "Not connected"
		return QueryResult{}
	}

	m.ActionNumber++
	m.ScopeActionNumber++

	dtr, err := m.internDH.GetData(preparedSQL, args...)
	if err != nil {
		m.Error = err.Error()
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
	if m.Error != "" {
		return QueryResult{}
	}

	if !m.connected {
		m.Error = "Not connected"
		return QueryResult{}
	}

	m.ActionNumber++
	m.ScopeActionNumber++

	sqr, err := m.internDH.Exec(preparedSQL, args...)
	if err != nil {
		m.Error = err.Error()
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
	if m.Error != "" {
		return QueryResult{}
	}

	if !m.connected {
		m.Error = "Not connected"
		return QueryResult{}
	}

	m.ActionNumber++
	m.ScopeActionNumber++

	pl := strings.ToLower(preparedSQL)
	pl = strings.TrimPrefix(pl, "")
	if pl != "exec" && pl != "execute" {
		preparedSQL = "EXECUTE " + preparedSQL
	}

	// Stored proc may have data returned
	dtr, err := m.internDH.GetData(preparedSQL, args...)
	if err != nil {
		m.Error = err.Error()
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
	_, err := m.internDH.Begin()
	if err != nil {
		m.ActionNumber++
		m.ScopeActionNumber++
		m.Error = err.Error()
	}
}

// Rollback - rollback a transaction
func (m *BatchQuery) Rollback() {
	m.ActionNumber++
	m.ScopeActionNumber++

	err := m.internDH.Rollback()
	if err != nil {
		m.ActionNumber++
		m.Error = err.Error()
	}
}

// Commit - commit a transaction
func (m *BatchQuery) Commit() {
	err := m.internDH.Commit()
	if err != nil {
		m.ActionNumber++
		m.Error = err.Error()
	}
}

// OK - checks if all queries are OK
func (m *BatchQuery) OK() bool {
	return m.Error == ""
}

// Settings - returns the internal datahelper settings
func (m *BatchQuery) Settings() cfg.Configuration {
	return m.internDH.Settings
}

// ScopeName - name of a function where this query is currently running for debugging purposes. This must be set before any query is executed. The default scope name is 'main'
func (m *BatchQuery) ScopeName(scopeName string) {
	m.ScopeActionNumber = 0
	m.scopeName = scopeName
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
