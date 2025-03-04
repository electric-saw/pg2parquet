# Pg2parquet

Pg2parquet is a postgres logical replication consumer outputting wal into parquet file per table.

## TODO

- [ ] Fix timezone
- [ ] Add more metrics
- [ ] Fix config load
- [ ] adjust const params
- [ ] Check types
- [ ] Make test of all types
- [ ] Write a marshal of values (???)
- [ ] Adjust postgres alive
- [ ] adjust status sent to work with out data recived
- [ ] Store metadata on postgres (offsets, parquet files?)

## Metrics

- [x] Postgres lag
- [x] postgres ops
- [ ] per table metrics
- [ ] parquet buffer, speed
- [ ] s3?
