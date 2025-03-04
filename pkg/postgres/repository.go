package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/electric-saw/pg2parquet/pkg/config"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirupsen/logrus"
)

type Repository struct {
	config *config.Config
	conn   *pgx.Conn
	ctx    context.Context
}

func NewRepository(ctx context.Context, config *config.Config) (*Repository, error) {
	connConfig, err := pgx.ParseConfig(config.Connection)
	if err != nil {
		logrus.WithError(err).Fatal("can't parse connection config")
		return nil, err
	}

	delete(connConfig.RuntimeParams, "replication")

	if conn, err := pgx.ConnectConfig(ctx, connConfig); err != nil {
		logrus.WithError(err).Fatal("can't create repository")
		return nil, err
	} else {
		return &Repository{
			config: config,
			conn:   conn,
			ctx:    ctx,
		}, nil
	}
}

func (r *Repository) PublicationExists(name string) (bool, error) {
	sql := "select 1 from pg_publication where pubname = $1;"
	rows, err := r.conn.Query(r.ctx, sql, name)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return rows.Next(), nil
}

func (r *Repository) CreatePublication(name string, tables []string) error {
	if len(tables) == 0 {
		return fmt.Errorf("it is not possible to create publication without tables")
	}

	exists, err := r.PublicationExists(name)
	if err != nil {
		return err
	}

	var sql string
	if exists {
		sql = fmt.Sprintf("alter publication %s set table %s;", name, strings.Join(tables, ","))
	} else {
		sql = fmt.Sprintf("create publication %s for table %s;", name, strings.Join(tables, ","))
	}
	_, err = r.conn.Exec(r.ctx, sql)
	return err
}

func (r *Repository) DropPublication(name string) error {
	sql := fmt.Sprintf("drop publication if exists %s;", name)
	result := r.conn.PgConn().Exec(r.ctx, sql)
	_, err := result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to drop publication %s: %e", name, err)
	}
	return nil
}

func (r *Repository) DropReplicationSlot(name string, repConn *pgconn.PgConn) error {
	return pglogrepl.DropReplicationSlot(r.ctx, repConn, name, pglogrepl.DropReplicationSlotOptions{
		Wait: true,
	})
}

func (r *Repository) SlotExists(name string) (bool, pglogrepl.LSN, error) {
	sql := "select confirmed_flush_lsn from pg_replication_slots where slot_name = $1;"
	rows, err := r.conn.Query(r.ctx, sql, name)
	if err != nil {
		return false, 0, err
	}
	defer rows.Close()
	if exists := rows.Next(); exists {
		var rawLsn string
		_ = rows.Scan(&rawLsn)
		lsn, err := pglogrepl.ParseLSN(rawLsn)
		return exists, lsn, err
	} else {
		return exists, 0, nil
	}
}

func (r *Repository) CreateReplicationSlot(name string, temp bool, repConn *pgconn.PgConn) (pglogrepl.LSN, error) {
	if exists, lsn, err := r.SlotExists(name); exists {
		return lsn, nil
	} else if err != nil {
		return 0, err
	}

	slotOptions := pglogrepl.CreateReplicationSlotOptions{Temporary: temp, Mode: pglogrepl.LogicalReplication}

	slotInfo, err := pglogrepl.CreateReplicationSlot(r.ctx, repConn, name, outputPlugin, slotOptions)
	if err == nil {
		if temp {
			logrus.Infof("Created temporary slot %s with plugin %s on consistent point %s", slotInfo.SlotName, slotInfo.OutputPlugin, slotInfo.ConsistentPoint)
		} else {
			logrus.Infof("Created slot %s with plugin %s on consistent point %s", slotInfo.SlotName, slotInfo.OutputPlugin, slotInfo.ConsistentPoint)
		}
	}
	return 0, err
}
