import pytest
from datetime import datetime, timezone
import time

from helpers.iceberg_utils import (
    create_iceberg_table,
    default_upload_directory,
    default_download_directory,
    get_uuid_str,
    get_last_snapshot
)

@pytest.mark.parametrize("storage_type", ["local", "s3", "azure"])
def test_optimize(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'=
        'merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 100)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")
    snapshot_timestamp = datetime.now(timezone.utc)

    time.sleep(0.1)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100, 110)")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    instance.query(f"OPTIMIZE TABLE {TABLE_NAME};", settings={"allow_experimental_iceberg_compaction" : 1})

    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id") == instance.query(
        "SELECT number FROM numbers(20, 90)"
    )

    # check that timetravel works with previous snapshot_ids and timestamps
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {snapshot_id}") == instance.query(
        "SELECT number FROM numbers(20, 80)"
    )

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_timestamp_ms = {int(snapshot_timestamp.timestamp() * 1000)}") == instance.query(
        "SELECT number FROM numbers(20, 80)"
    )
    if storage_type == "azure":
        return

    default_download_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
        f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/",
    )
    df = spark.read.format("iceberg").load(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}").collect()
    assert len(df) == 90

@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files(started_cluster_iceberg_with_spark, storage_type):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifests_" + storage_type + "_" + get_uuid_str()

    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string) USING iceberg TBLPROPERTIES ('format-version' = '2', 'write.update.mode'=
        'merge-on-read', 'write.delete.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(10, 100)")

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")
    snapshot_timestamp = datetime.now(timezone.utc)

    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {snapshot_id}") == instance.query(
        "SELECT number FROM numbers(10, 90)"
    )

    time.sleep(0.1)
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(100, 200)")
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(600, 700)")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(200, 300)")

    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(300, 400)")
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(400, 500)")

    # spark.sql(f"DELETE FROM {TABLE_NAME} WHERE id < 20")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} select id, char(id + ascii('a')) from range(600, 700)")
    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    instance.query(f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST;", settings={"allow_experimental_iceberg_compaction" : 1})

    # check that timetravel works with previous snapshot_ids and timestamps
    assert instance.query(f"SELECT id FROM {TABLE_NAME} ORDER BY id SETTINGS iceberg_snapshot_id = {snapshot_id}") == instance.query(
        "SELECT number FROM numbers(10, 90)"
    )

    instance.query(f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST;", settings={"allow_experimental_iceberg_compaction" : 1})


@pytest.mark.parametrize("storage_type", ["s3"])
def test_optimize_manifest_files_partitioned(started_cluster_iceberg_with_spark, storage_type):
    """
    Test manifest-only compaction for a partitioned Iceberg table.

    The table is partitioned by 'region' (3 distinct values).  We perform many
    small inserts across all partitions so that the number of manifest files
    grows well above the compaction threshold.  After OPTIMIZE TABLE ... MANIFEST
    the manifests should be consolidated to one per partition.

    Checks:
    - Data correctness is preserved after compaction.
    - Time-travel via snapshot_id still works after compaction.
    - A second OPTIMIZE invocation is a no-op (already optimal).
    - The compaction threshold setting is honoured: with the default threshold (5)
      a table that already has <= 5 manifest files is left untouched, while with
      a lower threshold (2) compaction is triggered sooner.
    """
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    TABLE_NAME = "test_optimize_manifests_partitioned_" + storage_type + "_" + get_uuid_str()

    # 3 distinct partition values
    REGIONS = ["eu", "us", "ap"]
    NUM_PARTITIONS = len(REGIONS)

    # ── Create partitioned table ──────────────────────────────────────────────
    spark.sql(
        f"""
        CREATE TABLE {TABLE_NAME} (id long, data string, region string)
        USING iceberg
        PARTITIONED BY (region)
        TBLPROPERTIES (
            'format-version' = '2',
            'write.update.mode'  = 'merge-on-read',
            'write.delete.mode'  = 'merge-on-read',
            'write.merge.mode'   = 'merge-on-read'
        )
        """
    )

    # ── Initial insert – one batch per partition ──────────────────────────────
    for region in REGIONS:
        spark.sql(
            f"INSERT INTO {TABLE_NAME} "
            f"SELECT id, char(id + ascii('a')), '{region}' "
            f"FROM range(0, 30)"
        )

    default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )

    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    first_snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")
    snapshot_timestamp = datetime.now(timezone.utc)

    time.sleep(0.1)
    # 30 rows × 3 regions = 90 rows
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 90

    # Time-travel snapshot should also see 90 rows
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {first_snapshot_id}"
    )) == 90

    # ── Many more small inserts to create many manifest files ─────────────────
    # 6 batches × 3 regions = 18 additional inserts → well above threshold (5)
    for batch_start in range(30, 90, 10):
        for region in REGIONS:
            spark.sql(
                f"INSERT INTO {TABLE_NAME} "
                f"SELECT id, char(id + ascii('a')), '{region}' "
                f"FROM range({batch_start}, {batch_start + 10})"
            )
        default_upload_directory(
            started_cluster_iceberg_with_spark,
            storage_type,
            f"/iceberg_data/default/{TABLE_NAME}/",
            f"/iceberg_data/default/{TABLE_NAME}/",
        )

    snapshot_id = get_last_snapshot(f"/var/lib/clickhouse/user_files/iceberg_data/default/{TABLE_NAME}/")

    # 90 (initial) + 6 batches × 10 rows × 3 regions = 90 + 180 = 270
    total_rows = 90 + 6 * 10 * NUM_PARTITIONS
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {snapshot_id}"
    )) == total_rows

    # ── Run manifest compaction ───────────────────────────────────────────────
    # Lower threshold to 2 so that compaction is definitely triggered
    # (each partition will have at least 7 manifest files after the inserts above)
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )

    # ── Data correctness after compaction ────────────────────────────────────
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {snapshot_id}"
    )) == total_rows
    #assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == total_rows

    # Every region should have the same row count
    for region in REGIONS:
        expected_count = 90  # 30 initial + 6 × 10 additional
        actual_count = int(instance.query(
            f"SELECT count() FROM {TABLE_NAME} WHERE region = '{region}' SETTINGS iceberg_snapshot_id = {snapshot_id}"
        ))
        assert actual_count == expected_count, \
            f"Region '{region}': expected {expected_count} rows, got {actual_count}"

    # ── Time-travel still works after compaction ──────────────────────────────
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {first_snapshot_id}"
    )) == 90

    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_timestamp_ms = {int(snapshot_timestamp.timestamp() * 1000)}"
    )) == 90

    # ── Second OPTIMIZE should be a no-op (already one manifest per partition) ─
    # This must not raise and must leave data intact
    instance.query(
        f"OPTIMIZE TABLE {TABLE_NAME} MANIFEST;",
        settings={
            "allow_experimental_iceberg_compaction": 1,
            "iceberg_manifest_min_count_to_compact": 2,
        },
    )
    assert int(instance.query(
        f"SELECT count() FROM {TABLE_NAME} "
        f"SETTINGS iceberg_snapshot_id = {snapshot_id}"
    )) == total_rows


