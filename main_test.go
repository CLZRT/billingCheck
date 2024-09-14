package billingUsage

import (
	"context"
	"github.com/cloudevents/sdk-go/v2/event"
	"testing"

	"github.com/stretchr/testify/assert"
)

// 测试函数
func TestDailyRun(t *testing.T) {
	// 创建模拟上下文
	ctx := context.Background()

	// 创建模拟的事件数据
	msg := MessagePublishedData{}

	// 将模拟的数据转换为 Cloud Event 的 Data
	e := event.New()
	err := e.SetData(event.ApplicationJSON, msg)
	assert.NoError(t, err, "failed to set event data")

	// 调用待测试的函数
	err = DailyRun(ctx, e)

	// 验证是否没有错误
	assert.NoError(t, err, "DailyRun should not return an error")
}
