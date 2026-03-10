import logging

from helpers.iceberg_utils import get_uuid_str

AZURE_CONTAINER = "testcontainer"


def test_spark_write_and_read(started_cluster_iceberg):
    """Verify Spark can write to and read from Azurite via WASB emulator mode."""
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_spark_roundtrip_" + get_uuid_str()

    # Write
    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )

    spark.sql(
        f"""
            INSERT INTO {TABLE_NAME}
            SELECT id as number FROM range(100)
        """
    )

    # Read back
    df = spark.sql(f"SELECT count(*) as cnt FROM {TABLE_NAME}").collect()
    count = df[0].cnt
    logging.info(f"Spark read back {count} rows")
    assert count == 100, f"Expected 100 rows, got {count}"

    # List blobs to see what paths Spark actually wrote
    blob_client = started_cluster_iceberg.blob_service_client
    container_client = blob_client.get_container_client(AZURE_CONTAINER)
    blobs = list(container_client.list_blobs())
    print(f"Blobs in container ({len(blobs)}):")
    for blob in blobs[:20]:
        print(f"  {blob.name}")

    assert len(blobs) > 0, "No blobs written to Azurite!"

    # Now try ClickHouse reading the same data
    instance = started_cluster_iceberg.instances["node1"]
    azurite_url = started_cluster_iceberg.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]

    # Try both with and without leading slash
    blob_prefix = blobs[0].name.split("/default/")[0] if "/default/" in blobs[0].name else "iceberg_data"
    table_path = f"{blob_prefix}/default/{TABLE_NAME}/"
    print(f"Using blob_path: {table_path}")

    instance.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergAzure(azure,
            container = '{AZURE_CONTAINER}',
            storage_account_url = '{azurite_url}',
            blob_path = '{table_path}')
        """
    )

    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 100, f"Expected 100 rows, got {rows}"


def test_spark_write_ch_read_append(started_cluster_iceberg):
    """Spark writes, CH reads, Spark appends, CH reads updated data."""
    instance = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_spark_append_" + get_uuid_str()
    azurite_url = started_cluster_iceberg.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    blob_path = f"iceberg_data/default/{TABLE_NAME}/"

    # Spark creates the table and inserts initial data
    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id as number FROM range(100)")

    # Create ClickHouse table pointing to the same Azurite location
    instance.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergAzure(azure,
            container = '{AZURE_CONTAINER}',
            storage_account_url = '{azurite_url}',
            blob_path = '{blob_path}')
        """
    )

    # CH reads Spark's data
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 100, f"Expected 100 rows, got {rows}"

    result = instance.query(f"SELECT sum(number) FROM {TABLE_NAME}")
    assert int(result) == 4950, f"Expected sum 4950, got {result.strip()}"

    # Spark appends more data
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id + 100 as number FROM range(50)")

    # CH reads the updated data
    rows = int(instance.query(f"SELECT count() FROM {TABLE_NAME}"))
    assert rows == 150, f"Expected 150 rows after append, got {rows}"


def test_ch_write_spark_read_simple(started_cluster_iceberg):
    """Spark creates a table, CH appends data, Spark reads all data back."""
    instance = started_cluster_iceberg.instances["node1"]
    spark = started_cluster_iceberg.spark_session

    TABLE_NAME = "test_ch_write_simple_" + get_uuid_str()
    azurite_url = started_cluster_iceberg.env_variables["AZURITE_STORAGE_ACCOUNT_URL"]
    blob_path = f"iceberg_data/default/{TABLE_NAME}/"

    # Spark creates the table and inserts initial data
    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT
            )
            USING iceberg
            OPTIONS('format-version'='2');
        """
    )
    spark.sql(f"INSERT INTO {TABLE_NAME} SELECT id as number FROM range(10)")

    # ClickHouse table pointing to the same Azurite location
    instance.query(
        f"""
        CREATE TABLE {TABLE_NAME}
        ENGINE=IcebergAzure(azure,
            container = '{AZURE_CONTAINER}',
            storage_account_url = '{azurite_url}',
            blob_path = '{blob_path}')
        SETTINGS iceberg_use_version_hint = 1
        """
    )

    # CH reads Spark's data
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 10

    # CH writes more data
    insert_settings = {
        "allow_insert_into_iceberg": 1,
        "write_full_path_in_iceberg_metadata": 1,
    }
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (42)", settings=insert_settings)
    instance.query(f"INSERT INTO {TABLE_NAME} VALUES (123)", settings=insert_settings)

    # CH reads its own writes
    assert int(instance.query(f"SELECT count() FROM {TABLE_NAME}")) == 12
    assert int(instance.query(f"SELECT sum(number) FROM {TABLE_NAME}")) == sum(range(10)) + 42 + 123

    # Spark reads all data (restart session to clear HadoopCatalog metadata cache)
    started_cluster_iceberg.spark_session._restart()
    spark = started_cluster_iceberg.spark_session

    df = spark.sql(f"SELECT * FROM {TABLE_NAME}").collect()
    assert len(df) == 12, f"Spark expected 12 rows, got {len(df)}"
    spark_values = sorted([row.number for row in df])
    assert 42 in spark_values
    assert 123 in spark_values
