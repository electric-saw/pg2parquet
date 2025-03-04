package main

import (
	"context"

	"github.com/electric-saw/pg2parquet/internal/version"
	"github.com/electric-saw/pg2parquet/pkg/config"
	"github.com/electric-saw/pg2parquet/pkg/controller"
	"github.com/electric-saw/pg2parquet/pkg/metrics"

	"github.com/sirupsen/logrus"
)

func main() {

	logrus.Infof("Pg2parquet version: %s", version.GetVersion())
	go metrics.ListenMetrics()
	logrus.SetLevel(logrus.InfoLevel)
	appConfig, err := config.Load()
	if err != nil {
		logrus.WithError(err).Fatal("can't load config file")
	}

	c := controller.NewController(appConfig)

	if err := c.Run(context.Background()); err != nil {
		logrus.WithError(err).Fatal("failed on start controller")
	}
}
