#!/usr/bin/env bash
# Tags: atomic-database
# Test for double-fire bug: REFRESH EVERY ... DEPENDS ON fires twice when the
# dependency-triggered refresh crosses a timeslot boundary.
#
# The bug: a view with REFRESH EVERY 2 MINUTE DEPENDS ON dep fires once when
# the dependency completes (for the overdue timeslot), and then again when the
# next timeslot boundary is also in the past by the time the first refresh
# finishes. With APPEND TO, this causes duplicate rows.
#
# The key ingredient: the dependency uses REFRESH AFTER (not EVERY). With AFTER,
# getNextRefreshTimeslot = end_time + period, which can be far enough in the
# future that the dependent thinks its dependencies are satisfied for the NEXT
# timeslot too.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT" | sed 's/--session_timezone[= ][^ ]*//g'`"
CLICKHOUSE_CLIENT="`echo "$CLICKHOUSE_CLIENT --session_timezone Etc/UTC"`"

$CLICKHOUSE_CLIENT -q "create view refreshes as select * from system.view_refreshes where database = '$CLICKHOUSE_DATABASE' order by view"

# Source table.
$CLICKHOUSE_CLIENT -q "create table src (x Int64) engine Memory as select 1"

# Target table for the dependent view (APPEND TO).
$CLICKHOUSE_CLIENT -q "create table target (y Int64) engine MergeTree order by y"

# Dependency view: REFRESH AFTER 2 MINUTE (not EVERY — this is key to the bug).
# With AFTER, getNextRefreshTimeslot = end_time + 2 minutes, which will be far
# enough in the future to satisfy the dependent's dependency check for the next
# timeslot.
$CLICKHOUSE_CLIENT -q "
    create materialized view dep
        refresh after 2 minute
        engine Memory
        empty
        as select x from src"

# Dependent view: REFRESH EVERY 2 MINUTE DEPENDS ON dep, APPEND TO target.
# sleepEachRow(3) makes the refresh take ~3 real seconds so we can advance
# fake time past the next 2-minute boundary while it's running.
$CLICKHOUSE_CLIENT -q "
    create materialized view v
        refresh every 2 minute depends on dep
        append to target
        empty
        as select x as y from src where sleepEachRow(3) = 0"

# Do initial refreshes so both views have known state.
$CLICKHOUSE_CLIENT -q "
    system test view dep set fake time '2050-01-01 00:00:01';
    system test view v set fake time '2050-01-01 00:00:01';
    system stop view dep;
    system stop view v;"
$CLICKHOUSE_CLIENT -q "
    system refresh view dep;
    system wait view dep;
    system refresh view v;
    system wait view v;"

# Clear target so we can count fresh appends.
$CLICKHOUSE_CLIENT -q "truncate table target"

# Advance dep to 00:03:00 so it refreshes.
# After this: dep's last_completed_timeslot = 00:03:00 (AFTER: uses end_time).
# dep's getNextRefreshTimeslot = 00:03:00 + 2 min = 00:05:00.
$CLICKHOUSE_CLIENT -q "
    system test view dep set fake time '2050-01-01 00:03:00';
    system start view dep;"
while [ "`$CLICKHOUSE_CLIENT -q "select last_success_time from refreshes where view = 'dep' -- $LINENO" | xargs`" != '2050-01-01 00:03:00' ]
do
    sleep 0.5
done
$CLICKHOUSE_CLIENT -q "system stop view dep;"

# Set dependent's fake time to just before the 00:04 boundary.
# v's last_completed_timeslot = 00:00, next timeslot = 00:02.
# Since fake time (00:03:58) > 00:02, the 00:02 timeslot is overdue.
# The dependency is satisfied: dep's next timeslot (00:05) > 00:02.
# So v should fire immediately for timeslot 00:02.
$CLICKHOUSE_CLIENT -q "
    system test view v set fake time '2050-01-01 00:03:58';
    system start view v;"

# Wait for the dependent to start running.
while [ "`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'v' -- $LINENO" | xargs`" != 'Running' ]
do
    sleep 0.5
done

# While the refresh is running (takes ~3 seconds due to sleepEachRow(3)),
# advance the fake clock past the 00:04 boundary.
# After the refresh completes:
#   start_time = 00:03:58 (captured before refresh)
#   end_time = 00:04:01 (captured after, using new fake time)
#   timeslotForCompletedRefresh uses floorEvery(start_time) = 00:02
#   So last_completed_timeslot = 00:02, next timeslot = 00:04.
#   Current time = 00:04:01 >= 00:04 → timeslot is due.
#   Dependency check: dep's next timeslot (00:05) > 00:04 → satisfied!
#   → Scheduler fires a SECOND refresh for timeslot 00:04.
$CLICKHOUSE_CLIENT -q "system test view v set fake time '2050-01-01 00:04:01';"

# Wait for the view to finish and settle.
sleep 5

# Wait for it to not be Running (both refreshes should have completed by now,
# if the bug exists).
for i in $(seq 1 30); do
    status="`$CLICKHOUSE_CLIENT -q "select status from refreshes where view = 'v' -- $LINENO" | xargs`"
    [ "$status" == 'Running' ] || break
    sleep 1
done

# Count rows in target.
# Bug present: 2 rows (double fire — one for timeslot 00:02, one for 00:04).
# Bug fixed: 1 row (single fire for timeslot 00:02 only).
$CLICKHOUSE_CLIENT -q "select '<result>', count() from target"

# Check the last_completed_timeslot via next_refresh_time.
# Bug present: last_completed = 00:04, next = 00:06.
# Bug fixed: last_completed = 00:02, next = 00:04 (Scheduled).
$CLICKHOUSE_CLIENT -q "select '<next_refresh>', next_refresh_time from refreshes where view = 'v'"

$CLICKHOUSE_CLIENT -q "
    drop table target;
    drop table dep;
    drop table v;
    drop table src;
    drop table refreshes;"
