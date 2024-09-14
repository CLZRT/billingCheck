package internal

import (
	"log"
	"time"
)

func GetWeekRange(last, cur string) string {
	tmpDate, err := time.Parse("2006-01-02", last)
	if err != nil {
		log.Println(err)
	}
	lastWeekFirstDay := tmpDate.AddDate(0, 0, 1).Format("2006-01-02")
	return lastWeekFirstDay + " ~ " + cur
}
