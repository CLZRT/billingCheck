package config

import (
	"gopkg.in/yaml.v3"
	"io/ioutil"
	"os"
)

type Config struct {
	BigQuery struct {
		ProjectID string `yaml:"projectID"`
		TableID   string `yaml:"tableID"`
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
