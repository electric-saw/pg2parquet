package postgres

import (
	"context"

	"github.com/electric-saw/pg2parquet/pkg/config"

	"github.com/jackc/pgx/v5/pgconn"
)

const outputPlugin = "pgoutput"

type Postgres struct {
	connConfig *pgconn.Config
	conn       *pgconn.PgConn
	ctx        context.Context
}

func NewPostgres(ctx context.Context, config *config.Config) (*Postgres, error) {
	connConfig, err := pgconn.ParseConfig(config.Connection)
	if err != nil {
		return nil, err
	}

	conn, err := pgconn.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, err
	}

	pg := &Postgres{
		connConfig: connConfig,
		conn:       conn,
		ctx:        ctx,
	}

	return pg, nil
}

func (ps *Postgres) Close() error {
	return ps.conn.Close(ps.ctx)
}
