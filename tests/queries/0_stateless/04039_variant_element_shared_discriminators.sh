#!/usr/bin/env bash
# Tags: no-msan, long, no-flaky-check
# msan: too slow

# Regression test for exception in SerializationVariantElement::deserializeBinaryBulkWithMultipleStreams:
#   "Size of deserialized variant column less than the limit"
# The bug was that three loops iterated up to discriminators_data.size() instead of
# the current read's discriminators range end, which overcounted variant_limit when
# the discriminators column accumulated rows from previous reads via the substreams cache.
# See https://github.com/ClickHouse/ClickHouse/issues/99358

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_variant_type=1 --allow_suspicious_variant_types=1"

function run_queries()
{
    local settings=$1

    # Read multiple variant subcolumns in the same query — this triggers shared
    # discriminators via the substreams cache, which is the code path that was fixed.
    for query in \
        "select v from test_variant_elem format Null" \
        "select v.String from test_variant_elem format Null" \
        "select v.UInt64 from test_variant_elem format Null" \
        "select v.\`LowCardinality(String)\` from test_variant_elem format Null" \
        "select v.\`Tuple(a UInt32, b UInt32)\` from test_variant_elem format Null" \
        "select v.\`Array(UInt64)\` from test_variant_elem format Null" \
        "select v.String, v.UInt64 from test_variant_elem format Null" \
        "select v, v.String from test_variant_elem format Null" \
        "select v.String, v.UInt64, v.\`LowCardinality(String)\` from test_variant_elem format Null" \
        "select count() from test_variant_elem where isNotNull(v.String)" \
        "select count() from test_variant_elem where isNotNull(v.UInt64)" \
    ; do
        $CH_CLIENT -q "$query settings $settings" 2>&1 || echo "FAIL: $query settings $settings"
    done
}

$CH_CLIENT -q "drop table if exists test_variant_elem"

# Wide part — the main scenario for the bug.
echo "Wide part"
$CH_CLIENT -q "create table test_variant_elem (id UInt64, v Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64)))
    engine=MergeTree order by id
    settings min_rows_for_wide_part=1, min_bytes_for_wide_part=1, index_granularity_bytes=10485760, index_granularity=8192"

$CH_CLIENT -q "insert into test_variant_elem with 'Variant(String, UInt64, LowCardinality(String), Tuple(a UInt32, b UInt32), Array(UInt64))' as type
    select number, multiIf(
        number % 6 == 0, CAST(NULL, type),
        number % 6 == 1, CAST(('str_' || toString(number))::Variant(String), type),
        number % 6 == 2, CAST(number, type),
        number % 6 == 3, CAST(('lc_str_' || toString(number))::LowCardinality(String), type),
        number % 6 == 4, CAST(tuple(number, number + 1)::Tuple(a UInt32, b UInt32), type),
        CAST(range(number % 20 + 1)::Array(UInt64), type))
    from numbers(600000)"

$CH_CLIENT -q "optimize table test_variant_elem final"

# Test with various block sizes and injection probabilities — CI randomizes these settings.
for block_size in 100 1000 8192 65505; do
    for prob in 0.0 0.05 0.5 1.0; do
        run_queries "max_block_size=$block_size, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=$prob"
    done
done

# Verify correctness.
$CH_CLIENT -q "select count() from test_variant_elem where isNotNull(v.String)"
$CH_CLIENT -q "select count() from test_variant_elem where isNotNull(v.UInt64)"

$CH_CLIENT -q "drop table test_variant_elem"

echo "OK"
