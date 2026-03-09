#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLICKHOUSE_USER_FILES="/home/alesapin/work/clickdb/user_files"
ICEBERG_TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_t1"

# Cleanup
rm -rf "${ICEBERG_TABLE_PATH}"

# Create Iceberg table with version hint and insert data
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE t1 (c0 Int, c1 Int) ENGINE = IcebergLocal('${ICEBERG_TABLE_PATH}') SETTINGS iceberg_use_version_hint = 1;
    INSERT INTO t1 VALUES (1, 2);
"

# Simulate Spark-style version hint (just a number, not a full path)
echo -n "2" > "${ICEBERG_TABLE_PATH}/metadata/version-hint.text"

# This should not crash
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    ALTER TABLE t1 DROP COLUMN c1;
"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM t1"

# Cleanup
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t1"
rm -rf "${ICEBERG_TABLE_PATH}"
