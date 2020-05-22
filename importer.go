package datautils

import (
	"database/sql"
	//"eaglebush/datatable"
	"log"
	"strconv"

	dh "github.com/eaglebush/datahelper"
	"github.com/eaglebush/datatable"
)

// DataQuery - a data query set
type DataQuery struct {
	PreparedQuery string        // A prepared SQL query
	Args          []interface{} // Value arguments
}

// DataConfiguration - a configuration for import/export
type DataConfiguration struct {
	DataQuery                // DataQuery object
	Helper    *dh.DataHelper // DataHelper object
}

// Importer - imports data and store data to the destination
type Importer struct {
	ID               string            // ID of the Importer
	Source           DataConfiguration // Source data access configuration
	Destination      DataConfiguration // Destination data access
	DestinationCheck DataQuery         // A check query before importing data. If set, the importer will utilize this query
	Log              bool              // Log actions
	checkerIndex     []int             // Checker Indexes
}

// Run - run the importer
func (imp *Importer) Run() (selected int64, inserted int64, err error) {

	var (
		rc        int64
		ri        int64
		exists    bool
		aff       int64
		rsrc      datatable.Row
		affected  sql.Result
		checkArgs []interface{}
	)

	// Get records from source
	rsrc, err = imp.Source.Helper.GetDataReader(imp.Source.PreparedQuery, imp.Source.Args...)
	if err != nil {
		if imp.Log {
			log.Println(`SOURCE: `+imp.ID+` -> `, err)
		}
		return 0, 0, err
	}

	rc = 0
	ri = 0
	broke := false

	checkArgs = make([]interface{}, len(imp.checkerIndex))

	for rsrc.Next() {

		exists = false

		// if set, we check records to validate
		if imp.DestinationCheck.PreparedQuery != "" && len(imp.checkerIndex) > 0 {

			// populate checker arguments
			for i, v := range imp.checkerIndex {
				checkArgs[i] = rsrc.ResultRows[v]
			}

			exists, err = imp.Destination.Helper.Exists(imp.DestinationCheck.PreparedQuery, checkArgs...)
			if err != nil {
				if imp.Log {
					log.Printf(`DESTINATION CHECK: `+imp.ID+` -> Error checking record: %v\r\n`, err.Error())
				}

				broke = true
				break
			}

			if exists {
				if imp.Log {
					log.Printf(`DESTINATION CHECK: ` + imp.ID + ` -> Record exists.\r\n`)
				}
			}

		}

		// Insert rows
		if !exists {
			affected, err = imp.Destination.Helper.Exec(imp.Destination.PreparedQuery, rsrc.ResultRows...)
			if err != nil {
				if imp.Log {
					log.Printf(`DESTINATION: `+imp.ID+` -> Error inserting records: %v\r\n`, err.Error())
				}

				broke = true
				break
			}

			aff, _ = affected.RowsAffected()
			ri += aff
			rc++
		}

	}
	rsrc.Close()

	if broke {
		return rc, ri, err
	}

	if imp.Log {
		log.Printf(imp.ID + ` ` + strconv.Itoa(int(rc)) + " rows inserted.")
	}

	return rc, ri, nil
}

// SetArgs - set the arguments
func (dc *DataConfiguration) SetArgs(args ...interface{}) {
	dc.Args = make([]interface{}, len(args))
	for i, v := range args {
		dc.Args[i] = v
	}
}

// SetCheckerIndex - set the checker result index for the DestinationChecker
func (imp *Importer) SetCheckerIndex(argindex ...int) {
	imp.checkerIndex = argindex
}
