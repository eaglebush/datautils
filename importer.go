package datautils

import (
	"log"
	"strconv"

	dh "github.com/eaglebush/datahelper"
)

// DataConfiguration - a configuration for import/export
type DataConfiguration struct {
	Helper        *dh.DataHelper
	PreparedQuery string
	Args          []interface{}
}

// Importer - imports data and store data to the destination
type Importer struct {
	ID          string
	Source      DataConfiguration
	Destination DataConfiguration
	Log         bool
}

// Run - run the importer
func (imp *Importer) Run() (selected int64, inserted int64, err error) {
	// Get records from source
	rsrc, err := imp.Source.Helper.GetDataReader(imp.Source.PreparedQuery, imp.Source.Args...)
	if err != nil {
		if imp.Log {
			log.Println(`SOURCE: `+imp.ID+` -> `, err)
		}
		return 0, 0, err
	}

	// Prepare the destination
	stmt, err := imp.Destination.Helper.Prepare(imp.Destination.PreparedQuery)
	defer stmt.Close()
	if err != nil {
		if imp.Log {
			log.Println(`DESTINATION: `+imp.ID+` -> `, err)
		}

		return 0, 0, err
	}

	var rc int64
	var ri int64
	rc = 0
	ri = 0
	broke := false
	for rsrc.Next() {
		// Insert rows
		affected, err := stmt.Exec(rsrc.ResultRows...)
		if err != nil {
			if imp.Log {
				log.Printf(`DESTINATION: `+imp.ID+` -> Error inserting records: %v\r\n`, err.Error())
			}

			broke = true
			break
		}

		aff, _ := affected.RowsAffected()
		ri += aff
		rc++
	}
	rsrc.Close()
	stmt.Close()

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
