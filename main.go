package billingUsage

import (
	"clzrt.io/billingUsage/internal"
	"context"
	"fmt"
	"github.com/cloudevents/sdk-go/v2/event"
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"log"
	"os"
	"time"
)

type MessagePublishedData struct {
	Message PubSubMessage
}
type PubSubMessage struct {
	Data []byte `json:"data"`
}
type Config struct {
	BigQuery struct {
		ProjectID string `yaml:"projectID"`
	} `yaml:"bigQuery"`

	Webhook struct {
		URL     string `yaml:"url"`
		KeyWord string `yaml:"keyWord"`
	} `yaml:"webhook"`

	Storage struct {
		Bucket    string `yaml:"bucket"`
		ProjectID string `yaml:"projectID"`
	} `yaml:"storage"`

	Email struct {
		SMTPHost string `yaml:"smtpHost"`
		SMTPPort int    `yaml:"smtpPort"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"email"`

	Recipients []string `yaml:"recipients"`
}

// LoadConfig reads the YAML configuration from the given file path
func LoadConfig(configPath string) (*Config, error) {
	configFile, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer configFile.Close()

	configData, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}

	var config Config
	err = yaml.Unmarshal(configData, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func usageCheck() {
	ctx := context.Background()

	// Load configuration from YAML file
	config, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Initialize cases with configuration
	bgUserCase := internal.NewBigQueryUserCase(config.BigQuery.ProjectID, ctx)
	defer bgUserCase.Client.Close()

	webHookUserCase := internal.NewWebHookUserCaseWithDingTalk(config.Webhook.URL)
	storageCase, err := internal.NewStorageCase(ctx, config.Storage.Bucket, config.Storage.ProjectID)
	defer storageCase.Close()

	emailCase := internal.NewEmailUseCase(storageCase, config.Email.SMTPHost, config.Email.SMTPPort, config.Email.Username, config.Email.Password)
	recipients := config.Recipients

	dailyUsage, err := bgUserCase.DailyCheck(ctx)
	if err != nil {
		log.Println(err)
	}

	// 日用量有异常才发送
	if dailyUsage != nil {
		webHookUserCase.Send2DingTalk(dailyUsage, "daily Warning")
	} else {
		webHookUserCase.Send2DingTalk(dailyUsage, "everything is well")
		log.Println("everything is well")
	}

	fmt.Println(dailyUsage)

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

//减少项目，项目用量为0

func DailyRun(ctx context.Context, e event.Event) error {
	var msg MessagePublishedData
	if err := e.DataAs(&msg); err != nil {
		return fmt.Errorf("event.DataAs: %v", err)
	}
	usageCheck()
	return nil

}
