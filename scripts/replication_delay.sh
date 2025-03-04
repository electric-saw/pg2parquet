docker-compose exec postgres psql -U postgres -c "SELECT
    stat_rep.client_addr,
    rep_slots.slot_name,
    substr(rep_slots.slot_type, 1, 3) AS typ,
    rep_slots.active AS a,
    CASE stat_rep.application_name
    WHEN 'walreceiver' THEN 'walrec'
    WHEN 'pg_basebackup' THEN 'pbkp'
    ELSE stat_rep.application_name
    END AS app,
    COALESCE(stat_rep.pid, rep_slots.active_pid) AS pid,
    GREATEST(age(stat_rep.backend_xmin), age(rep_slots.xmin)) AS xmin,
    concat_ws(
        E' \n',
        'sent:  ' || pg_size_pretty(pg_wal_lsn_diff(l.curr_lsn, stat_rep.sent_lsn)),
        'write: ' || pg_size_pretty(pg_wal_lsn_diff(l.curr_lsn, stat_rep.write_lsn)),
        'flush: ' || pg_size_pretty(pg_wal_lsn_diff(l.curr_lsn, stat_rep.flush_lsn)),
        'repl.: ' || pg_size_pretty(pg_wal_lsn_diff(l.curr_lsn, stat_rep.replay_lsn)),
        ''
    ) AS st_diffs,
        concat_ws(
        E' \n',
        'sent:  ' || stat_rep.sent_lsn,
        'write: ' || stat_rep.write_lsn,
        'flush: ' || stat_rep.flush_lsn,
        'repl.: ' || stat_rep.replay_lsn,
        ''
    ) AS state,
    concat_ws(
        E' \n',
        '',
        'write: ' || coalesce(stat_rep.write_lag::text, '--'),
        'flush: ' || coalesce(stat_rep.flush_lag::text, '--'),
        'repl.: ' || coalesce(stat_rep.replay_lag::text, '--'),
        ''
    ) AS st_lags,
    concat_ws(
        E' \n',
        'rest: ' || pg_size_pretty(pg_wal_lsn_diff(l.curr_lsn, rep_slots.restart_lsn)),
        'cfm : ' || pg_size_pretty(pg_wal_lsn_diff(l.curr_lsn, rep_slots.confirmed_flush_lsn)),
        ''
    ) AS slot_diffs,
    concat_ws(
        E' \n',
        'rest: ' || rep_slots.restart_lsn,
        'cfm : ' || rep_slots.confirmed_flush_lsn,
        ''
    ) AS slot_lsn
FROM
    pg_stat_replication AS stat_rep
    FULL OUTER JOIN pg_replication_slots AS rep_slots ON stat_rep.pid = rep_slots.active_pid
    CROSS JOIN LATERAL (
        SELECT CASE WHEN NOT pg_is_in_recovery() THEN pg_current_wal_lsn() ELSE COALESCE(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()) END
    ) AS l(curr_lsn)
ORDER BY
    stat_rep.replay_lag desc,
    pid
limit 10
;"