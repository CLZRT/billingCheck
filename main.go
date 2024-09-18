package billingUsage

import (
	"clzrt.io/billingUsage/internal"
	"clzrt.io/billingUsage/internal/config"
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"log"
	"time"
)

type MessagePublishedData struct {
	Message PubSubMessage
}
type PubSubMessage struct {
	Data []byte `json:"data"`
}

func usageCheck() {
	ctx := context.Background()

	// Load configuration from YAML file
	loadConfig, err := config.LoadConfig("config_bk.yaml")
	if err != nil {
		log.Fatalf("failed to load loadConfig: %v", err)
	}

	// Initialize cases with configuration
	bgUserCase := internal.NewBigQueryUserCase(loadConfig.BigQuery.ProjectID, ctx)
	defer bgUserCase.Client.Close()

	webHookUserCase := internal.NewWebHookUserCaseWithDingTalk(loadConfig.Webhook.URL)
	storageCase, err := internal.NewStorageCase(ctx, loadConfig.Storage.Bucket, loadConfig.Storage.ProjectID)
	defer storageCase.Close()

	emailCase := internal.NewEmailUseCase(storageCase, loadConfig.Email.SMTPHost, loadConfig.Email.SMTPPort, loadConfig.Email.Username, loadConfig.Email.Password)
	recipients := loadConfig.Recipients

	dailyUsage, err := bgUserCase.DailyCheck(ctx)
	if err != nil {
		log.Println(err)
	}
	// 日用量有异常才发送
	if dailyUsage != nil {
		webHookUserCase.Send2DingTalk(dailyUsage, "daily Warning")
	} else {
		webHookUserCase.Send2DingTalk(dailyUsage, "日用量无异常")
		log.Println("日用量无异常")
	}

	//周用量有异常 才发送
	if isTodayTuesday() {
		// 检查周用量数据异常
		weekUsageCheck, err := bgUserCase.WeekCheck(ctx)
		if err != nil {
			return
		}
		if weekUsageCheck != nil {
			webHookUserCase.Send2DingTalk(weekUsageCheck, "周用量异常")
		} else {
			webHookUserCase.Send2DingTalk(nil, "周用量无异常")
			log.Println("周用量无异常")
		}

	}

	// 月用量异常 发送

	if isTodaySecond() {
		monthUsageCheck, err := bgUserCase.MonthCheck(ctx)
		if err != nil {
			return
		}
		if monthUsageCheck != nil {
			webHookUserCase.Send2DingTalk(monthUsageCheck, "月用量异常")
		} else {
			webHookUserCase.Send2DingTalk(nil, "月用量无异常")
			log.Println("月用量无异常")
		}

	}

	// 每周一，检查 (上周用量,上上周）和（本月，上月）用量
	// 判断当天 是否为周一，周一才统计周，月用量
	if isTodayMonthDay() {

		weekUsage, err := bgUserCase.WeekUsage(ctx)
		if err != nil {
			log.Println(err)
		}
		monthUsage, err := bgUserCase.MonthUsage(ctx)
		if err != nil {
			log.Println(err)
		}

		// 存储周使用量数据
		err = storageCase.StoreWeekUsage(ctx, weekUsage)
		if err != nil {
			log.Printf("error storing week usage: %v", err)
		}

		// 存储月使用量数据
		err = storageCase.StoreMonthUsage(ctx, monthUsage)
		if err != nil {
			log.Printf("error storing month usage: %v", err)
		}

		for _, recipient := range recipients {
			// 发送周使用量报告
			err = emailCase.SendWeekUsageReport(ctx, recipient)
			if err != nil {
				log.Printf("Error sending week usage report: %v", err)
				// 继续执行，不要因为发送邮件失败就中断整个流程
			}

			// 发送月使用量报告
			err = emailCase.SendMonthUsageReport(ctx, recipient)
			if err != nil {
				log.Printf("Error sending month usage report: %v", err)
				// 继续执行，不要因为发送邮件失败就中断整个流程
			}
		}
	}
}

func isTodayMonthDay() bool {
	return time.Now().Weekday() == time.Monday
}

func isTodaySecond() bool {
	return time.Now().Day() == 2
}

func isTodayTuesday() bool {

	return time.Now().Weekday() == time.Tuesday
}

func DailyRun(ctx context.Context, e event.Event) error {
	var msg MessagePublishedData
	if err := e.DataAs(&msg); err != nil {
		return fmt.Errorf("event.DataAs: %v", err)
	}
	usageCheck()
	return nil

}
