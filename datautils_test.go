package datautils

import (
	//"eaglebush/datahelper"
	"log"
	"strconv"
	"testing"

	"github.com/eaglebush/datahelper"

	_ "github.com/denisenkom/go-mssqldb"
	cfg "github.com/eaglebush/config"
)

func TestBatchQuery(t *testing.T) {

	config, err := cfg.LoadConfig("config.json")
	if err != nil {
		log.Fatal("Configuration file not found!")
	}

	ms := NewBatchQuery(config)

	if !ms.Connect("ZX_APPSDB") {
		log.Fatal(`Connection to database failed`)
	}
	defer ms.Disconnect()

	ms.Begin()

	// Long method of getting the value
	qr := ms.Get(`SELECT * FROM tcoWarehouse ORDER BY WhseID`)
	if qr.OK {
		if qr.HasData {
			// Data is an array of rows.
			log.Println("Data found (Long Method): " + qr.Data[0].ValueString("WhseID") + ", ActionNumber: " + strconv.Itoa(int(ms.LastActionNumber())))
		}
	}

	// The OK property could be ignored and get directly to the checking of HasData
	qr = ms.Get(`SELECT * FROM tamEquipment ORDER BY BrandName`)
	if qr.HasData {
		log.Println("Data found (Shortcut): " + qr.Data[0].ValueString("BrandName") + ", ActionNumber: " + strconv.Itoa(int(ms.LastActionNumber())))
	}

	// The OK property could be ignored and get directly to the checking of HasData. If style
	if qr = ms.Get(`SELECT *
					FROM tamEquipment
					ORDER BY BrandName`); qr.HasData {
		log.Println("Data found (Shortcut): " + qr.Data[0].ValueString("BrandName") + ", ActionNumber: " + strconv.Itoa(int(ms.LastActionNumber())))
	}

	// Shortcut getting data (qr.Get(0).ValueString("ManufacturerID"))
	qr = ms.Get(`SELECT * FROM tcoManufacturer ORDER BY ManufacturerID DESC`)
	if qr.HasData {
		log.Println("Data found (Row Shortcut): " + qr.Get(0).ValueString("ManufacturerID") + ", ActionNumber: " + strconv.Itoa(int(ms.LastActionNumber())))
	}

	// Sure checking of getting data (for SUM, COUNT etc)
	qr = ms.Get(`SELECT COUNT(*) FROM tcoManufacturer`)
	//log.Println("Data found (Sure Shortcut): " + string(int(qr.Get(0).ValueInt64Ord(0))) + ", ActionNumber: " + strconv.Itoa(int(ms.LastActionNumber())))

	qr = ms.Get(`SELECT * FROM tamRepairDetail ORDER BY Symptoms`)
	if qr.OK {
		if qr.HasData {
			log.Println("Data found: " + qr.Data[0].ValueString("Symptoms") + ", ActionNumber: " + strconv.Itoa(int(ms.LastActionNumber())))
		} else {
			log.Println("No data found, ActionNumber: " + strconv.Itoa(int(ms.LastActionNumber())))
		}
	}

	// Set queries checking can also be skipped. If error was encountered, succeeding queries will not execute and go down directly
	qr = ms.Set(`INSERT INTO tbcGroupName (GroupId, GroupName, GroupNameCode) VALUES (100, 'TEST', 'TEST')`)

	qr = ms.Get(`SELECT * FROM tcoManufacturer ORDER BY ManufacturerID`)
	if qr.OK {
		if qr.HasData {
			log.Println("Data found: " + qr.Data[0].ValueString("ManufacturerID") + ", ActionNumber: " + strconv.Itoa(int(ms.LastActionNumber())))
		}
	}

	if ms.OK() {
		ms.Commit()
		log.Println("Queries OK!")
	} else {
		ms.Rollback()
		log.Println("Queries failed on last action number " + strconv.Itoa(ms.LastActionNumber()-1) + ". Details: " + ms.LastErrorText())
	}
}

func TestGetNextBlockSurrogateKey(t *testing.T) {
	config, err := cfg.LoadConfig("config.json")
	if err != nil {
		log.Fatal("Configuration file not found!")
	}

	bq := NewBatchQuery(config)
	frequency := 10

	tableName := "TestTable"

	if !bq.Connect("DEST_MDCI") {
		log.Fatal(`Connection to database failed`)
	}
	defer bq.Disconnect()

	bq.ScopeName("TestGetNextBlockSurrogateKey")

	qr := bq.Get(`SELECT NextKey FROM tciSurrogateKey WHERE TableName=?;`, tableName)
	if qr.HasData {
		//key := int(qr.Get(0).ValueInt64Ord(0)) // value of the key is what was defined before (therefore NextKey)

		bq.Set(`UPDATE tciSurrogateKey SET NextKey = NextKey + ? WHERE TableName=?;`, int64(frequency)+1, tableName)
		log.Println("OK")
	}

	bq.Set(`INSERT INTO tciSurrogateKey
			SELECT ?,?`, tableName, int64(frequency)+1)

	// 10
	// outputs the next key as 11 to prepare for next
}

func TestImporter(t *testing.T) {
	var (
		err    error
		config *cfg.Configuration
	)

	config, err = cfg.LoadConfig(`config.json`)
	if err != nil {
		log.Fatal("Configuration file not found!")
	}

	src := datahelper.NewDataHelper(config)
	_, err = src.Connect(`SOURCE`)
	defer src.Disconnect(false)

	if err != nil {
		log.Fatal(err)
	}

	// Destination picklist
	dst := datahelper.NewDataHelper(config)
	_, err = dst.Connect(`DESTINATION`)
	defer dst.Disconnect(false)

	if err != nil {
		log.Fatal(err)
	}

	// Initialization
	so := &Importer{ID: "Sent", Log: true}

	so.Source = DataConfiguration{
		Helper: src,
	}

	so.Source.PreparedQuery = `SELECT [EmailKey]
								,[Subject]
								,[Body]
								,[Format]
								,[Importance]
								,[SenderName]
								,[SenderAddress]
								,[ApplicationID]
								,[DateQueued]
								,[DateSent]
							  FROM [dbo].[tnfEmailSent];`

	// so.Source.SetArgs(dbeg, dend, tranid, tranid)

	so.Destination = DataConfiguration{
		Helper: dst,
	}

	// Destination
	so.Destination.PreparedQuery = `INSERT INTO [dbo].[tnfEmailSent]
										([EmailKey]
										,[Subject]
										,[Body]
										,[Format]
										,[Importance]
										,[SenderName]
										,[SenderAddress]
										,[ApplicationID]
										,[DateQueued]
										,[DateSent])
									VALUES
										(?,?,?,?,?,?,?,?,?,?);`

	// Destination checking
	so.DestinationCheck.PreparedQuery = `[dbo].[tnfEmailSent] WHERE EmailKey = ? AND SenderAddress = ?;`
	so.SetCheckerIndex(0, 6)

	// Run
	cnt, ins, err := so.Run()
	if err != nil {
		log.Fatal(`SOURCE: [1] tsoSalesOrder: `, err)
	}

	log.Println(cnt, ins)

	so = nil
}
