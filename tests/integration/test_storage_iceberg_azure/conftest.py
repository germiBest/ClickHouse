import pytest
import logging
import pyspark
import os
import os.path as p

from helpers.cluster import ClickHouseCluster
from helpers.spark_tools import ResilientSparkSession, write_spark_log_config


AZURE_ACCOUNT_NAME = "devstoreaccount1"
AZURE_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
AZURE_CONTAINER = "testcontainer"


def get_spark(log_dir=None):
    """
    Configure Spark to write Iceberg tables to Azurite via WASB (HTTP).

    Key insight: hadoop-azure's emulator mode config must use the FQDN
    (devstoreaccount1.blob.core.windows.net) because it does an exact match
    with the account name extracted from the WASB URI.
    """
    hadoop_version = "3.3.4"

    builder = (
        pyspark.sql.SparkSession.builder
            .appName("IcebergAzureConcurrent")
            .config("spark.jars.repositories", "https://repo1.maven.org/maven2")
            .config("spark.jars.packages",
                    f"org.apache.hadoop:hadoop-azure:{hadoop_version},"
                    f"com.microsoft.azure:azure-storage:8.6.6")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse",
                    f"wasb://{AZURE_CONTAINER}@{AZURE_ACCOUNT_NAME}.blob.core.windows.net"
                    f"/iceberg_data")
            # Enable emulator mode with FQDN — this makes hadoop-azure
            # connect to http://127.0.0.1:10000 using dev storage credentials.
            .config("spark.hadoop.fs.azure.storage.emulator.account.name",
                    f"{AZURE_ACCOUNT_NAME}.blob.core.windows.net")
            .master("local")
    )

    if log_dir:
        props_path = write_spark_log_config(log_dir)
        builder = builder.config(
            "spark.driver.extraJavaOptions",
            f"-Dlog4j2.configurationFile=file:{props_path}",
        )

    return builder.getOrCreate()


@pytest.fixture(scope="package")
def started_cluster_iceberg():
    try:
        # Force Azurite to port 10000 — the emulator mode hardcodes this port.
        cluster = ClickHouseCluster(
            __file__, with_spark=True, azurite_default_port=10000
        )
        cluster.add_instance(
            "node1",
            main_configs=[
                "configs/config.d/query_log.xml",
                "configs/config.d/cluster.xml",
                "configs/config.d/named_collections.xml",
            ],
            user_configs=["configs/users.d/users.xml"],
            with_azurite=True,
            stay_alive=True,
            mem_limit="15g",
        )

        logging.info("Starting cluster...")
        cluster.start()

        # Create test container
        container_client = cluster.blob_service_client.get_container_client(
            AZURE_CONTAINER
        )
        if not container_client.exists():
            container_client.create_container()
        cluster.azure_container_name = AZURE_CONTAINER

        cluster.spark_session = ResilientSparkSession(
            lambda: get_spark(cluster.instances_dir)
        )

        yield cluster

    finally:
        cluster.shutdown()
