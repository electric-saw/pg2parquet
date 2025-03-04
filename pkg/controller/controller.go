package controller

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/electric-saw/pg2parquet/pkg/config"
	"github.com/electric-saw/pg2parquet/pkg/pgoutputv2"
	"github.com/electric-saw/pg2parquet/pkg/postgres"

	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"
)

type Controller struct {
	config *config.Config
}

func NewController(config *config.Config) *Controller {
	return &Controller{
		config: config,
	}
}

func (c *Controller) initializeConsumer(ctx context.Context) (postgres.Consumer, error) {
	pg, err := postgres.NewPostgres(ctx, c.config)
	if err != nil {
		return nil, fmt.Errorf("failed on create postgres connection: %w", err)
	}

	consumer, err := postgres.NewSlotConsumer(ctx, pg, c.config)
	if err != nil {
		return nil, fmt.Errorf("failed on create wal listener: %w", err)
	}

	return consumer, nil
}

func (c *Controller) Run(ctx context.Context) error {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		signal.Stop(sig)
		cancel()
	}()

	consumer, err := c.initializeConsumer(ctx)
	if err != nil {
		return err
	}

	manager := newManager(consumer.Relations(), c.config)

	if err := consumer.Start(ctx); err != nil {
		return fmt.Errorf("failed on start consumer: %w", err)
	}

	for {
		select {
		case evt := <-consumer.Events():
			logrus.WithFields(
				logrus.Fields{
					"Type":  evt.Message.Type(),
					"Value": pgoutputv2.DebugMsg(evt.Message),
				}).Debug("Received message")

			if logrus.GetLevel() <= logrus.TraceLevel {
				consumer.DebugEvent(evt)
			}

			switch msg := evt.Message.(type) {
			case *pglogrepl.RelationMessage:
				manager.handleTable(msg, evt.LSN)
			case *pglogrepl.RelationMessageV2:
				manager.handleTable(&msg.RelationMessage, evt.LSN)
			case *pglogrepl.InsertMessage:
				manager.handleRow(msg.RelationID, msg.Tuple, evt.LSN)
			case *pglogrepl.InsertMessageV2:
				manager.handleRow(msg.RelationID, msg.Tuple, evt.LSN)
			case *pglogrepl.UpdateMessage:
				manager.handleRow(msg.RelationID, msg.NewTuple, evt.LSN)
			case *pglogrepl.UpdateMessageV2:
				manager.handleRow(msg.RelationID, msg.NewTuple, evt.LSN)
			case *pglogrepl.DeleteMessage:
				manager.handleRow(msg.RelationID, msg.OldTuple, evt.LSN)
			case *pglogrepl.DeleteMessageV2:
				manager.handleRow(msg.RelationID, msg.OldTuple, evt.LSN)
			}

			consumer.SetBufferLSN(manager.minBuffredLSN())
			consumer.SetAppliedLSN(manager.minCommitedLSN())
		case <-sig:
			goto exit
		case <-ctx.Done():
			goto exit
		}
	}
exit:
	consumer.Stop()
	manager.stop()

	return nil
}
