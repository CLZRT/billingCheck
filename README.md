# billingCheck
use to check billingUsage everyDay in Google Cloud
# 前提条件
- Gcp账单导入到 bigquery
- 钉钉机器人配置
- 邮箱配置
# 如何使用
- 填写config.yaml文件配置
- 将该项目，部署至 cloud run函数中
- 配置定时器运行
# 效果
- 每天检查用量，用量异常，发送至钉钉。
- 每周一统计上周与上上周用量，并做对比。
