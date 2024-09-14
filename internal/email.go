package internal

import (
	"context"
	"fmt"
	"gopkg.in/gomail.v2"
	"io"
	"log"
	"time"
)

type EmailUseCase struct {
	storageCase  *StorageCase
	smtpHost     string
	smtpPort     int
	smtpUsername string
	smtpPassword string
}

func NewEmailUseCase(storageCase *StorageCase, smtpHost string, smtpPort int, smtpUsername, smtpPassword string) *EmailUseCase {
	return &EmailUseCase{
		storageCase:  storageCase,
		smtpHost:     smtpHost,
		smtpPort:     smtpPort,
		smtpUsername: smtpUsername,
		smtpPassword: smtpPassword,
	}
}

func (e *EmailUseCase) SendExcelAttachment(ctx context.Context, fileName, recipient, subject, body string) error {
	// 从 StorageCase 获取 Excel 文件
	content, err := e.storageCase.GetExcelFile(ctx, fileName)
	if err != nil {
		return fmt.Errorf("error getting Excel file: %v", err)
	}

	// 创建邮件
	m := gomail.NewMessage()
	m.SetHeader("From", e.smtpUsername)
	m.SetHeader("To", recipient)
	m.SetHeader("Subject", subject)
	m.SetBody("text/plain", body)
	m.Attach(fileName, gomail.SetCopyFunc(func(w io.Writer) error {
		_, err := w.Write(content)
		return err
	}))

	// 发送邮件
	d := gomail.NewDialer(e.smtpHost, e.smtpPort, e.smtpUsername, e.smtpPassword)
	if err := d.DialAndSend(m); err != nil {
		return fmt.Errorf("error sending email: %v", err)
	}

	log.Printf("Email sent to %s with attachment %s", recipient, fileName)
	return nil
}

func (e *EmailUseCase) SendWeekUsageReport(ctx context.Context, recipient string) error {
	fileName := fmt.Sprintf("week_usage_%s.xlsx", time.Now().Format("2006-01-02"))
	subject := "Weekly Usage Report"
	body := "Please find attached the weekly usage report."
	return e.SendExcelAttachment(ctx, fileName, recipient, subject, body)
}

func (e *EmailUseCase) SendMonthUsageReport(ctx context.Context, recipient string) error {
	fileName := fmt.Sprintf("month_usage_%s.xlsx", time.Now().Format("2006-01-02"))
	subject := "Monthly Usage Report"
	body := "Please find attached the monthly usage report."
	return e.SendExcelAttachment(ctx, fileName, recipient, subject, body)
}

func (e *EmailUseCase) SendDailyUsageReport(ctx context.Context, recipient string) error {
	fileName := fmt.Sprintf("daily_usage_%s.xlsx", time.Now().Format("2006-01-02"))
	subject := "Daily Usage Report"
	body := "Please find attached the daily usage report."
	return e.SendExcelAttachment(ctx, fileName, recipient, subject, body)
}
