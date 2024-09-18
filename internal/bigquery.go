package internal

import (
	"cloud.google.com/go/bigquery"
	"clzrt.io/billingUsage/internal/config"
	"context"
	"google.golang.org/api/iterator"
	"log"
	"math"
	"time"
)

type BigQueryUserCase struct {
	Client *bigquery.Client
	Config *config.Config
}

func NewBigQueryUserCase(projectID string, ctx context.Context) *BigQueryUserCase {

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Println(err)
	}
	config, err := config.LoadConfig("config_bk.yaml")
	return &BigQueryUserCase{client, config}
}
func (u *BigQueryUserCase) WeekUsage(ctx context.Context) ([][]bigquery.Value, error) {
	//第一天为周日
	last, cur, next := getFirstWeekDay()
	lastWeekRange := GetWeekRange(last, cur)
	curWeekRange := GetWeekRange(cur, next)

	log.Println("LastWeekRange: [" + lastWeekRange + "] CurWeekRange: [" + curWeekRange + "]")
	q := u.Client.Query(
		"SELECT project_id, lastWeek_cost, curWeek_cost, curWeek_cost - lastWeek_cost AS cost_difference " +
			"FROM ( " +
			"   SELECT project.id AS project_id, " +
			"       SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, WEEK) = TIMESTAMP(\"" + last + "\") THEN cost ELSE 0 END) - " +
			"       SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(\"" + last + "\") THEN cost ELSE 0 END) + " +
			"       SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(\"" + cur + "\") THEN cost ELSE 0 END) AS lastWeek_cost, " +
			"       SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, WEEK) = TIMESTAMP(\"" + cur + "\") THEN cost ELSE 0 END) - " +
			"       SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(\"" + next + "\") THEN cost ELSE 0 END) + " +
			"       SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(\"" + next + "\") THEN cost ELSE 0 END) AS curWeek_cost " +
			"   FROM  `" +
			u.Config.BigQuery.TableID + "`" +
			"   WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, WEEK) IN (TIMESTAMP(\"" + last + "\"), TIMESTAMP(\"" + cur + "\"), TIMESTAMP(\"" + next + "\")) " +
			"   GROUP BY project.id " +
			") AS costs " +
			"ORDER BY cost_difference DESC ")

	rows, err := u.getValues(ctx, q)

	if err != nil {
		return nil, err
	}
	return rows, nil

}
func (u *BigQueryUserCase) WeekCheck(ctx context.Context) ([][]bigquery.Value, error) {
	rows, err := u.WeekUsage(ctx)
	if err != nil {
		return nil, err
	}

	var res [][]bigquery.Value
	for idx, row := range rows {
		rowLen := len(rows[idx])
		if usageChange, ok := row[rowLen-1].(float64); ok {
			if lastUsage, ok2 := row[1].(float64); ok2 {
				if math.Abs(usageChange) > lastUsage*0.3 || usageChange > 500 {
					res = append(res, row)
				}
			}
		}

	}
	return res, nil

}
func (u *BigQueryUserCase) MonthUsage(ctx context.Context) ([][]bigquery.Value, error) {
	last, cur := getFirstMonthDay()
	lastMonth := last[:7]
	curMonth := cur[:7]

	log.Print("Month: " + "\n" + "lastMonth: " + lastMonth + " curMonth: " + curMonth)
	q := u.Client.Query(
		"SELECT project.id AS project_id, " +
			"SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) = TIMESTAMP(\"" + last + "\") THEN cost ELSE 0 END) AS lastMonth_cost, " +
			"SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) = TIMESTAMP(\"" + cur + "\") THEN cost ELSE 0 END) AS curMonth_cost, " +
			"SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) = TIMESTAMP(\"" + cur + "\") THEN cost ELSE 0 END) -" +
			"SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) = TIMESTAMP(\"" + last + "\") THEN cost ELSE 0 END) AS cost_difference" +
			" FROM  `" +
			u.Config.BigQuery.TableID + "`" +
			" WHERE  TIMESTAMP_TRUNC(_PARTITIONTIME, MONTH) IN (TIMESTAMP(\"" + last + "\"), TIMESTAMP(\"" + cur + "\")) " +
			" GROUP BY project.id " +
			" ORDER BY  cost_difference DESC ")
	rows, err := u.getValues(ctx, q)
	if err != nil {
		return nil, err
	}
	return rows, nil

}
func (u *BigQueryUserCase) MonthCheck(ctx context.Context) ([][]bigquery.Value, error) {
	rows, err := u.MonthUsage(ctx)
	if err != nil {
		return nil, err
	}

	var res [][]bigquery.Value
	for idx, row := range rows {
		rowLen := len(rows[idx])
		if usageChange, ok := row[rowLen-1].(float64); ok {
			if lastUsage, ok2 := row[1].(float64); ok2 {
				if math.Abs(usageChange) > lastUsage*0.3 {
					res = append(res, row)
				}
			}
		}

	}
	return res, nil

}
func (u *BigQueryUserCase) DailyUsage(ctx context.Context) ([][]bigquery.Value, error) {
	yesterday, today := getTodayAndYesterday()
	log.Println("Day: \n" + "Yesterday: " + yesterday + " today: " + today)
	q := u.Client.Query(
		"SELECT project.id AS project_id, " +
			"SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(\"" + yesterday + "\") THEN cost ELSE 0 END) AS lastDay_cost, " +
			"SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(\"" + today + "\") THEN cost ELSE 0 END) AS curDay_cost, " +
			"SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(\"" + today + "\") THEN cost ELSE 0 END) - " +
			"SUM(CASE WHEN TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) = TIMESTAMP(\"" + yesterday + "\") THEN cost ELSE 0 END) AS cost_difference " +
			"FROM `billing-ftl-cloud.Daily_billing_gcp.gcp_billing_export_v1_017DBD_1FB85B_839E84` " +
			"GROUP BY project.id " +
			"HAVING lastDay_cost >= 15 OR curDay_cost >= 15 " +
			"ORDER BY cost_difference DESC ")

	rows, err := u.getValues(ctx, q)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
func (u *BigQueryUserCase) DailyCheck(ctx context.Context) ([][]bigquery.Value, error) {
	rows, err := u.DailyUsage(ctx)
	if err != nil {
		return nil, err
	}

	var res [][]bigquery.Value
	for idx, row := range rows {
		rowLen := len(rows[idx])
		if usageChange, ok := row[rowLen-1].(float64); ok {
			if lastUsage, ok2 := row[1].(float64); ok2 {
				if math.Abs(usageChange) > lastUsage*0.3 {
					res = append(res, row)
				}
			}
		}

	}
	return res, nil
}

func (u *BigQueryUserCase) getValues(ctx context.Context, q *bigquery.Query) ([][]bigquery.Value, error) {
	// Location must match that of the dataset(s) referenced in the query.
	q.Location = "asia-southeast1"
	// Run the query and print results when the query job is completed.
	job, err := q.Run(ctx)
	if err != nil {
		return nil, err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if err := status.Err(); err != nil {
		return nil, err
	}
	it, err := job.Read(ctx)
	var rows [][]bigquery.Value
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		//fmt.Println(row)
		rows = append(rows, row)

	}
	return rows, nil
}

func getFirstWeekDay() (last, cur, next string) {
	date := time.Now()
	weekday := int(date.Weekday())
	return date.AddDate(0, 0, -weekday-14).Format("2006-01-02"), date.AddDate(0, 0, -weekday-7).Format("2006-01-02"), date.AddDate(0, 0, -weekday).Format("2006-01-02")
}

func getFirstMonthDay() (last, cur string) {
	date := time.Now()

	// 获取当前月的第一天
	curFirstDay := time.Date(date.Year(), date.Month(), 1, 0, 0, 0, 0, date.Location())

	// 获取上个月的第一天
	lastFirstDay := curFirstDay.AddDate(0, -1, 0)

	// 返回格式化的日期
	return lastFirstDay.Format("2006-01-02"), curFirstDay.Format("2006-01-02")
}

func getTodayAndYesterday() (yes, today string) {
	date := time.Now()
	date = date.AddDate(0, 0, -1)
	yesDate := date.AddDate(0, 0, -1)
	return yesDate.Format("2006-01-02"), date.Format("2006-01-02")
}
