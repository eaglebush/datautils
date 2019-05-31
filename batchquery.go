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
	OK      bool
	Data    []dt.Row
	HasData bool
}

// BatchQuery - the BatchQuery struct
type BatchQuery struct {
	internDH     *dh.DataHelper
	connected    bool
	Error        string
	ActionNumber int
}

// NewBatchQuery - create a new BatchQuery object
func NewBatchQuery(config *cfg.Configuration) *BatchQuery {
	ms := &BatchQuery{}
	ms.internDH = dh.NewDataHelper(config)

	return ms
}

// Connect - connect the BatchQuery to the databse
func (m *BatchQuery) Connect(connectionID string) bool {
	m.Error = ""
	m.ActionNumber = 0
	connected, err := m.internDH.Connect(connectionID)
	if err != nil {
		m.ActionNumber = 1
		m.Error = err.Error()
		return false
	}
	m.connected = connected
	return m.connected
}

// Disconnect - disconnect from the dataabse
func (m *BatchQuery) Disconnect() {
	m.internDH.Disconnect()
}

// Get - get data from the database
func (m *BatchQuery) Get(preparedSQL string, args ...interface{}) QueryResult {
	m.ActionNumber++

	if m.Error != "" {
		return QueryResult{}
	}

	if !m.connected {
		m.ActionNumber++
		m.Error = "Not connected"
		return QueryResult{}
	}

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
	m.ActionNumber++

	if m.Error != "" {
		return QueryResult{}
	}

	if !m.connected {
		m.ActionNumber++
		m.Error = "Not connected"
		return QueryResult{}
	}

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
		HasData: dtr.RowCount > 0,
		Data:    dtr.Rows,
		OK:      true,
	}
}

// Do - execute a stored procedure
func (m *BatchQuery) Do(preparedSQL string, args ...interface{}) QueryResult {
	m.ActionNumber++

	if m.Error != "" {
		return QueryResult{}
	}

	if !m.connected {
		m.ActionNumber++
		m.Error = "Not connected"
		return QueryResult{}
	}

	pl := strings.ToLower(preparedSQL)
	pl = strings.TrimPrefix(pl, "")
	if pl != "exec" && pl != "execute" {
		preparedSQL = "EXEC " + preparedSQL
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
		m.Error = err.Error()
	}
}

// Rollback - rollback a transaction
func (m *BatchQuery) Rollback() {
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