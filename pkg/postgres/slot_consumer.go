package postgres

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/electric-saw/pg2parquet/pkg/config"
	"github.com/electric-saw/pg2parquet/pkg/pgoutputv2"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/sirupsen/logrus"
)

const (
	standbyMessageTimeout = time.Second * 3
	waitTimeout           = time.Second * 3
	waitTimeoutSendStatus = time.Second * 15
)

type Consumer interface {
	CommitCurrentLSN(context.Context) error
	GetBufferLSN() pglogrepl.LSN
	SetBufferLSN(pglogrepl.LSN)
	GetAppliedLSN() pglogrepl.LSN
	SetAppliedLSN(pglogrepl.LSN)
	Start(context.Context) error
	Stop()
	Events() <-chan *Event
	Relations() *pgoutputv2.RelationManager
	DebugEvent(event *Event)
}

type Event struct {
	LSN     pglogrepl.LSN
	Message pglogrepl.Message
}

type SlotConsumer struct {
	lsnMu        sync.RWMutex
	receivedLSN  pglogrepl.LSN
	bufferedLSN  pglogrepl.LSN
	appliedLSN   pglogrepl.LSN
	startLSN     pglogrepl.LSN
	endServerLSN pglogrepl.LSN
	pg           *Postgres
	config       *config.Config
	events       chan *Event
	cancel       context.CancelFunc
	repository   *Repository
	relations    *pgoutputv2.RelationManager
	maxMessages  int
	inStream     bool
	sync.Mutex
}

func NewSlotConsumer(ctx context.Context, pg *Postgres, config *config.Config) (Consumer, error) {
	repo, err := NewRepository(ctx, config)
	if err != nil {
		return nil, err
	}

	return &SlotConsumer{
		pg:          pg,
		config:      config,
		repository:  repo,
		relations:   pgoutputv2.NewRelationManager(),
		maxMessages: config.Slot.MaxWalMessages,
		inStream:    false,
	}, nil
}

func (sc *SlotConsumer) sendStatus(ctx context.Context) error {
	status := pglogrepl.StandbyStatusUpdate{
		ReplyRequested: false,
	}
	status.WALWritePosition = sc.getReceivedLSN()
	status.WALApplyPosition = sc.GetBufferLSN()
	status.WALFlushPosition = sc.GetAppliedLSN()

	if status.WALApplyPosition == 0 && status.WALFlushPosition == 0 && status.WALWritePosition == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, waitTimeoutSendStatus)

	sc.Lock()
	err := pglogrepl.SendStandbyStatusUpdate(ctx, sc.pg.conn, status)
	sc.Unlock()
	if err != nil {
		logrus.WithError(err).Error("SendStandbyStatusUpdate failed")
	}

	cancel()
	logrus.WithFields(logrus.Fields{
		"WALWritePosition": status.WALWritePosition,
		"WALFlushPosition": status.WALFlushPosition,
		"WALApplyPosition": status.WALApplyPosition,
	}).Println("status updated")
	return nil
}

func (sc *SlotConsumer) CommitCurrentLSN(ctx context.Context) error {
	return sc.sendStatus(ctx)
}

func (sc *SlotConsumer) SetAppliedLSN(lsn pglogrepl.LSN) {
	sc.lsnMu.Lock()
	sc.appliedLSN = lsn
	sc.lsnMu.Unlock()
}

func (sc *SlotConsumer) GetAppliedLSN() pglogrepl.LSN {
	sc.lsnMu.RLock()
	defer sc.lsnMu.RUnlock()
	return sc.appliedLSN
}

func (sc *SlotConsumer) SetBufferLSN(lsn pglogrepl.LSN) {
	sc.lsnMu.Lock()
	sc.bufferedLSN = lsn
	sc.lsnMu.Unlock()
}

func (sc *SlotConsumer) GetBufferLSN() pglogrepl.LSN {
	sc.lsnMu.RLock()
	defer sc.lsnMu.RUnlock()
	return sc.bufferedLSN
}

func (sc *SlotConsumer) Relations() *pgoutputv2.RelationManager {
	return sc.relations
}

func (sc *SlotConsumer) setReceivedLSN(lsn pglogrepl.LSN) {
	sc.lsnMu.Lock()
	sc.receivedLSN = lsn
	sc.lsnMu.Unlock()
}

func (sc *SlotConsumer) getReceivedLSN() pglogrepl.LSN {
	sc.lsnMu.RLock()
	defer sc.lsnMu.RUnlock()
	return sc.receivedLSN
}

func (sc *SlotConsumer) startReplication(ctx context.Context) error {
	pluginArguments := []string{
		"proto_version '2'",
		"messages 'true'",
		// "stream 'true'",
		fmt.Sprintf("publication_names '%s'", sc.config.Slot.Name),
	}
	pluginConfig := pglogrepl.StartReplicationOptions{
		Mode:       pglogrepl.LogicalReplication,
		PluginArgs: pluginArguments,
	}

	return pglogrepl.StartReplication(ctx, sc.pg.conn, sc.config.Slot.Name, sc.getReceivedLSN(), pluginConfig)
}

func (sc *SlotConsumer) Start(ctx context.Context) error {
	sysident, err := pglogrepl.IdentifySystem(ctx, sc.pg.conn)
	if err != nil {
		logrus.Fatalln("identifySystem failed:", err)
	}

	logrus.WithFields(logrus.Fields{
		"SystemID": sysident.SystemID,
		"Timeline": sysident.Timeline,
		"XLogPos":  sysident.XLogPos,
		"DBName":   sysident.DBName,
	}).Info("identify system result:")

	var currentLsn pglogrepl.LSN

	sc.endServerLSN = sysident.XLogPos
	currentLsn = sysident.XLogPos

	if err := sc.repository.CreatePublication(sc.config.Slot.Name, sc.config.Tables); err != nil {
		logrus.WithError(err).Fatalf("failed on create publication %s", sc.config.Slot.Name)
	}

	if lsn, err := sc.repository.CreateReplicationSlot(sc.config.Slot.Name, sc.config.Slot.Temporary, sc.pg.conn); err != nil {
		logrus.WithError(err).Fatalf("failed on create slot %s", sc.config.Slot.Name)
	} else if lsn > 0 {
		currentLsn = lsn
	}

	sc.startLSN = currentLsn
	sc.setReceivedLSN(currentLsn)
	sc.SetAppliedLSN(currentLsn)
	sc.SetBufferLSN(currentLsn)

	sc.events = make(chan *Event, sc.maxMessages)
	if err := sc.startReplication(ctx); err != nil {
		return err
	}

	go sc.work(ctx)
	go sc.statusWork(ctx)
	go sc.workMetrics(ctx)
	return nil
}

func (sc *SlotConsumer) Stop() {
	logrus.Info("Stop slot consumer")
	if sc.cancel != nil {
		sc.cancel()
	}
	if sc.events != nil {
		close(sc.events)
	}

	_ = sc.CommitCurrentLSN(context.Background())
}

func (sc *SlotConsumer) Events() <-chan *Event {
	return sc.events
}

func (sc *SlotConsumer) statusWork(ctx context.Context) {
	standbyTick := time.NewTicker(standbyMessageTimeout)
	defer standbyTick.Stop()
	for {
		select {
		case <-standbyTick.C:
			err := sc.sendStatus(ctx)
			if err != nil {
				logrus.WithError(err).Errorln("failed to send status to postgres")
			} else {
				logrus.Debugln("sending status to postgres")
			}
		case <-ctx.Done():
			return
		}
	}
}

func (sc *SlotConsumer) work(ctx context.Context) {
	ctx, sc.cancel = context.WithCancel(ctx)
	for {
		select {
		case <-ctx.Done():
			if err := sc.sendStatus(context.Background()); err != nil {
				logrus.WithError(err).Error("failed on send status to postgres")
			}
			return
		default:
			wctx, cancel := context.WithTimeout(ctx, waitTimeout)
			sc.Lock()
			rawMsg, err := sc.pg.conn.ReceiveMessage(wctx)
			sc.Unlock()
			cancel()

			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
			}

			if ctx.Err() != nil {
				return
			}

			switch msg := rawMsg.(type) {
			case *pgproto3.CopyData:
				switch msg.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
					if err != nil {
						logrus.WithError(err).Fatalf("Failed on parse primary keep alive message")
					}

					logrus.WithFields(map[string]any{
						"ServerWALEnd":   pkm.ServerWALEnd,
						"ServerTime":     pkm.ServerTime,
						"ReplyRequested": pkm.ReplyRequested,
					}).Debug("primary keep alive message")

					if pkm.ReplyRequested {
						_ = sc.sendStatus(ctx)
					}
					sc.endServerLSN = pkm.ServerWALEnd

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
					if err != nil {
						logrus.WithError(err).Fatalf("failed on parse XLogData")
					}

					parsedMsg, err := pgoutputv2.ParseWalMessage(xld.WALData, sc.inStream)
					if err != nil {
						logrus.WithError(err).Fatalf("failed on parse Wal message")
					}

					logrus.WithFields(map[string]any{
						"WALStart":     xld.WALStart,
						"ServerWALEnd": xld.ServerWALEnd,
						"ServerTime":   xld.ServerTime,
						"MessageType":  parsedMsg.Type(),
					}).Debug("received XLogData")

					walPos := xld.WALStart + pglogrepl.LSN(len(xld.WALData))

					switch msg := parsedMsg.(type) {
					case *pglogrepl.RelationMessageV2:
						sc.relations.Set(&msg.RelationMessage)

						if err := sc.relations.RefreshTypes(ctx, sc.repository.conn); err != nil {
							logrus.WithError(err).Error("can't refresh pg types")
						}
					case *pglogrepl.CommitMessage:
						walPos = msg.TransactionEndLSN
					default:
						updateCountEvents(msg)
					}

					sc.events <- &Event{
						LSN:     walPos,
						Message: parsedMsg,
					}

					sc.setReceivedLSN(xld.ServerWALEnd)
				}

			default:
				logrus.Errorf("received unexpected message: %#v", msg)

			}
		}
	}

}

func (sc *SlotConsumer) workMetrics(ctx context.Context) {
	tick := time.NewTicker(time.Second * 1)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			processedBytes.Set(float64(sc.startLSN - sc.appliedLSN))
			writeLag.Set(float64(sc.endServerLSN - sc.receivedLSN))
			flushLag.Set(float64(sc.endServerLSN - sc.bufferedLSN))
			applyLag.Set(float64(sc.endServerLSN - sc.appliedLSN))
		case <-ctx.Done():
			return
		}
	}
}

func (sc *SlotConsumer) DebugEvent(event *Event) {
	dump := func(relation uint32, tuple *pglogrepl.TupleData) {
		values, err := sc.relations.ParseValues(relation, tuple)
		if err != nil {
			logrus.Errorf("error parsing values: %s", err)
		}
		for _, value := range values {
			val := value.Get()
			logrus.Tracef("%s (%T): %#v - kind %s", value.Name, val, val, value.PgTypeName)
		}
	}

	switch msg := event.Message.(type) {
	case *pglogrepl.InsertMessage:
		dump(msg.RelationID, msg.Tuple)
	case *pglogrepl.InsertMessageV2:
		dump(msg.RelationID, msg.Tuple)
	case *pglogrepl.UpdateMessage:
		dump(msg.RelationID, msg.NewTuple)
		dump(msg.RelationID, msg.OldTuple)
	case *pglogrepl.UpdateMessageV2:
		dump(msg.RelationID, msg.NewTuple)
		dump(msg.RelationID, msg.OldTuple)
	case *pglogrepl.DeleteMessage:
		dump(msg.RelationID, msg.OldTuple)
	case *pglogrepl.DeleteMessageV2:
		dump(msg.RelationID, msg.OldTuple)
	}
}
