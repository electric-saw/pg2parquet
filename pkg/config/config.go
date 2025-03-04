package config

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"text/template"

	"github.com/Masterminds/sprig"
	"github.com/dustin/go-humanize"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Slot       *Slot    `yaml:"slot"`
	Connection string   `yaml:"connection"`
	Tables     []string `yaml:"tables"`
	Parquet    *Parquet `yaml:"parquet"`
	Storage    *Storage `yaml:"storage"`
}

type Parquet struct {
	PartSize        any `yaml:"size"`
	partSizeBytes   int64
	CompressionType string `yaml:"compression_type"`
	PageSize        any    `yaml:"page_size"`
	pageSizeBytes   int64
}

type Storage struct {
	tmpl     *template.Template
	Type     string `yaml:"type"`
	Path     string `yaml:"path"`
	Bucket   string `yaml:"bucket"`
	Endpoint string `yaml:"endpoint"`
}

type Slot struct {
	Name           string `yaml:"name"`
	Temporary      bool   `yaml:"temporary"`
	MaxWalMessages int    `yaml:"max_messages"`
}

func defaultConfig() *Config {
	return &Config{
		Slot: &Slot{
			Name:           "pg2parquet",
			Temporary:      true,
			MaxWalMessages: 1024,
		},
		Tables:     []string{},
		Connection: "postgresql://postgres:123@localhost/postgres?replication=database",
		Parquet: &Parquet{
			PartSize:        1,
			CompressionType: "SNAPPY",
			PageSize:        8 * 1024,
		},
		Storage: &Storage{},
	}
}

func Load() (*Config, error) {
	if len(os.Args) < 2 {
		logrus.Warnf("config path not informed... example: %s config.yaml", path.Base(os.Args[0]))
		return nil, fmt.Errorf("can't found config file")
	}

	filePath := os.Args[1]

	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	var config = defaultConfig()
	err = yaml.Unmarshal(file, config)
	if err != nil {
		return nil, err
	}

	if tmpl, err := template.New("storage path").Funcs(sprig.TxtFuncMap()).Parse(config.Storage.Path); err != nil {
		return nil, err
	} else {
		config.Storage.tmpl = tmpl
	}

	if err := config.Parquet.feedBytes(); err != nil {
		return nil, err
	}

	return config, nil
}

func (s *Storage) FormatedPath(args map[string]any) (string, error) {
	var buf bytes.Buffer
	if err := s.tmpl.Execute(&buf, args); err != nil {
		return "", err
	} else {
		return buf.String(), nil
	}
}

func (p *Parquet) PartSizeBytes() int64 {
	return p.partSizeBytes
}

func (p *Parquet) PageSizeBytes() int64 {
	return p.pageSizeBytes
}

func parseBytes(value any) (uint64, error) {
	switch value := value.(type) {
	case string:
		return humanize.ParseBytes(value)
	default:
		return uint64(value.(int)), nil
	}
}

func (p *Parquet) feedBytes() error {
	if b, err := parseBytes(p.PageSize); err != nil {
		return err
	} else {
		p.pageSizeBytes = int64(int(b))
	}

	if b, err := parseBytes(p.PartSize); err != nil {
		return err
	} else {
		p.partSizeBytes = int64(int(b))
	}

	return nil
}
