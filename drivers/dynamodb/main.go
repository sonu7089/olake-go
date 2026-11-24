package main

import (
	"github.com/datazip-inc/olake"
	driver "github.com/datazip-inc/olake/drivers/dynamodb/internal"
)

func main() {
	driver := &driver.DynamoDB{}
	defer driver.CloseConnection()
	olake.RegisterDriver(driver)
}
