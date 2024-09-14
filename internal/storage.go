package internal

import (
	"bytes"
	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/xuri/excelize/v2"
	"io"
	"log"
	"time"
)

type StorageCase struct {
	bucketName string
	projectID  string
	client     *storage.Client
}

func NewStorageCase(ctx context.Context, bucketName, projectID string) (*StorageCase, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating storage client: %v", err)
	}
	return &StorageCase{
		bucketName: bucketName,
		projectID:  projectID,
		client:     client,
	}, nil
}

func (s *StorageCase) storeAsExcel(ctx context.Context, data [][]bigquery.Value, fileName string, headers []string) error {
	// 创建新的Excel文件
	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			log.Println("Error closing Excel file:", err)
		}
	}()

	// 添加工作表
	sheetName := "Sheet1"
	index, err := f.NewSheet(sheetName)
	if err != nil {
		return fmt.Errorf("error creating new sheet: %v", err)
	}
	f.SetActiveSheet(index)

	// 写入表头
	for colIndex, header := range headers {
		cell, _ := excelize.CoordinatesToCellName(colIndex+1, 1) // 表头在第一行
		f.SetCellValue(sheetName, cell, header)
	}

	// 写入数据
	for rowIndex, row := range data {
		for colIndex, value := range row {
			cell, _ := excelize.CoordinatesToCellName(colIndex+1, rowIndex+2)
			f.SetCellValue(sheetName, cell, value)
		}
	}

	// 创建一个内存缓冲区来存储Excel文件
	buffer := new(bytes.Buffer)
	if err := f.Write(buffer); err != nil {
		return fmt.Errorf("error writing Excel to buffer: %v", err)
	}

	// 获取bucket引用
	bucket := s.client.Bucket(s.bucketName)

	// 创建新的对象
	obj := bucket.Object(fileName)
	writer := obj.NewWriter(ctx)

	// 将Excel文件写入Google Cloud Storage
	if _, err = io.Copy(writer, buffer); err != nil {
		return fmt.Errorf("error copying Excel to storage: %v", err)
	}

	// 关闭writer
	if err := writer.Close(); err != nil {
		return fmt.Errorf("error closing storage writer: %v", err)
	}

	log.Printf("File %s uploaded to bucket %s", fileName, s.bucketName)
	return nil
}

func (s *StorageCase) GetExcelFile(ctx context.Context, fileName string) ([]byte, error) {
	bucket := s.client.Bucket(s.bucketName)
	obj := bucket.Object(fileName)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("error reading object from bucket: %v", err)
	}
	defer reader.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error reading object content: %v", err)
	}

	return content, nil
}

func (s *StorageCase) StoreWeekUsage(ctx context.Context, data [][]bigquery.Value) error {
	fileName := fmt.Sprintf("week_usage_%s.xlsx", time.Now().Format("2006-01-02"))

	headers := []string{"项目id", "上上周用量", "上周用量", "周用量差"}
	return s.storeAsExcel(ctx, data, fileName, headers)
}

func (s *StorageCase) StoreMonthUsage(ctx context.Context, data [][]bigquery.Value) error {
	fileName := fmt.Sprintf("month_usage_%s.xlsx", time.Now().Format("2006-01-02"))
	headers := []string{"项目id", "上月总用量", "本月已用量", "月用量差"}
	return s.storeAsExcel(ctx, data, fileName, headers)
}

func (s *StorageCase) StoreDailyUsage(ctx context.Context, data [][]bigquery.Value) error {
	fileName := fmt.Sprintf("daily_usage_%s.xlsx", time.Now().Format("2006-01-02"))
	return s.storeAsExcel(ctx, data, fileName, nil)
}

func (s *StorageCase) Close() error {
	return s.client.Close()
}
