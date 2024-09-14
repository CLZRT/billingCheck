package internal

import (
	"bytes"
	"cloud.google.com/go/bigquery"
	"clzrt.io/billingUsage"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
)

type WebHookUserCase struct {
	dingTalk string
	wechat   string
	feiShu   string
}

func NewWebHookUserCaseWithDingTalk(dingTalk string) *WebHookUserCase {
	return &WebHookUserCase{dingTalk: dingTalk}
}
func NewWebHookUserCaseWithWeChat(weChat string) *WebHookUserCase {
	return &WebHookUserCase{dingTalk: weChat}
}
func NewWebHookUserCaseWithFeiShu(feiShu string) *WebHookUserCase {
	return &WebHookUserCase{dingTalk: feiShu}
}
func NewWebHookUserCase(dingTalk, weChat, feiShu string) *WebHookUserCase {
	return &WebHookUserCase{
		dingTalk: dingTalk,
		wechat:   weChat,
		feiShu:   feiShu,
	}
}

func (u *WebHookUserCase) Send2DingTalk(rows [][]bigquery.Value, title string) string {
	config, err := billingUsage.LoadConfig("config.yaml")

	message := map[string]interface{}{
		"msgtype": "text",
		"text": map[string]string{
			"content": title + "\n " + config.Webhook.KeyWord + ": \n" + formatRowsToString(rows),
		},
	}
	reqBody, err := json.Marshal(message)
	if err != nil {
		log.Println(err)
	}
	resp, err := http.Post(u.dingTalk, "application/json", bytes.NewReader(reqBody))

	log.Println(resp)
	if err != nil {
		log.Println(err)
	}
	return ""
}

func formatRowsToString(rows [][]bigquery.Value) string {
	var result strings.Builder
	for _, row := range rows {
		for i, col := range row {
			switch i {
			case 0:
				result.WriteString(fmt.Sprintf("%v: \n", col))
			case 1:
				result.WriteString(fmt.Sprintf("\t前天用量: %v", col))
			case 2:
				result.WriteString(fmt.Sprintf("\t昨天用量: %v", col))
			case 3:
				result.WriteString(fmt.Sprintf("\t用量差: %v", col))
			}
		}
		result.WriteString("\n")
	}
	return result.String()
}
