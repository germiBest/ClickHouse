from helpers.iceberg_utils import get_uuid_str


def test_nodes_dont_see_each_other(started_cluster_iceberg):
    """
    Spark writes different data to each node's local directory.
    Each node only sees its own data.
    """
    node1 = started_cluster_iceberg.instances["node1"]
    node2 = started_cluster_iceberg.instances["node2"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_isolation_" + get_uuid_str()

    # Create Iceberg tables via Spark — one per node catalog
    spark.sql(
        f"""
            CREATE TABLE node1_catalog.default.{TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )

    spark.sql(
        f"""
            CREATE TABLE node2_catalog.default.{TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )

    # Write 100 rows to node1, 200 rows to node2
    spark.sql(
        f"""
            INSERT INTO node1_catalog.default.{TABLE_NAME}
            SELECT id as number FROM range(100)
        """
    )

    spark.sql(
        f"""
            INSERT INTO node2_catalog.default.{TABLE_NAME}
            SELECT id as number FROM range(200)
        """
    )

    # Create ClickHouse tables — each node reads from its own iceberg directory
    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergLocal(local,
            path = '/var/lib/clickhouse/user_files/iceberg_node1/default/{TABLE_NAME}')
        """
    )
    node2.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergLocal(local,
            path = '/var/lib/clickhouse/user_files/iceberg_node2/default/{TABLE_NAME}')
        """
    )

    # Each node should only see its own data
    rows_node1 = int(node1.query(f"SELECT count() FROM {TABLE_NAME}"))
    rows_node2 = int(node2.query(f"SELECT count() FROM {TABLE_NAME}"))

    assert rows_node1 == 100, f"node1: expected 100 rows, got {rows_node1}"
    assert rows_node2 == 200, f"node2: expected 200 rows, got {rows_node2}"

    # Append more data to node1 only
    spark.sql(
        f"""
            INSERT INTO node1_catalog.default.{TABLE_NAME}
            SELECT id + 100 as number FROM range(50)
        """
    )

    rows_node1 = int(node1.query(f"SELECT count() FROM {TABLE_NAME}"))
    rows_node2 = int(node2.query(f"SELECT count() FROM {TABLE_NAME}"))

    assert rows_node1 == 150, f"node1: expected 150 rows after append, got {rows_node1}"
    assert rows_node2 == 200, f"node2: should still have 200 rows, got {rows_node2}"


def test_ch_write_spark_read(started_cluster_iceberg):
    """
    Spark creates a table, ClickHouse writes to it, Spark reads back.
    Validates that the external_dirs mount works bidirectionally.
    """
    node1 = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_ch_write_spark_read_" + get_uuid_str()

    # Spark creates the table structure
    spark.sql(
        f"""
            CREATE TABLE node1_catalog.default.{TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )

    # Create ClickHouse table pointing to the same location
    node1.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergLocal(local,
            path = '/var/lib/clickhouse/user_files/iceberg_node1/default/{TABLE_NAME}')
        """
    )

    # ClickHouse writes data
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (42)",
        settings={"allow_insert_into_iceberg": 1},
    )
    node1.query(
        f"INSERT INTO {TABLE_NAME} VALUES (123)",
        settings={"allow_insert_into_iceberg": 1},
    )

    # ClickHouse can read its own writes
    assert int(node1.query(f"SELECT count() FROM {TABLE_NAME}")) == 2

    # Spark should also see the data written by ClickHouse.
    # Spark's catalog caches metadata, so we need to refresh it first.
    spark.sql(f"REFRESH TABLE node1_catalog.default.{TABLE_NAME}")

    df = spark.sql(
        f"SELECT * FROM node1_catalog.default.{TABLE_NAME}"
    ).collect()
    assert len(df) == 2, f"Spark expected 2 rows, got {len(df)}"

    spark_values = sorted([row.number for row in df])
    assert spark_values == [42, 123], f"Spark got unexpected values: {spark_values}"
