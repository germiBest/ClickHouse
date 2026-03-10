import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag="26.2",
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup():
    yield
    node.restart_with_original_version(clear_data_dir=True)


def test_attach_nondeterministic_sorting_key(start_cluster):
    """Tables whose sorting key contains a non-deterministic function must
    still attach after upgrade.

    The determinism check in ``checkKeyExpression`` is meaningful at CREATE
    TABLE time but must be skipped during ATTACH, because the table already
    exists on disk.  This test injects ``dictGet`` into the sorting key
    metadata while the server is stopped, then verifies the new binary can
    attach the table.

    Background: PR #91352 added alias expansion in ``getProjectionFromAST``
    which can resolve a column alias to a non-deterministic expression (e.g.
    ``dictGet``).  The expanded expression ends up in the projection's sorting
    key and previously caused ``assertDeterministic`` to reject the table on
    attach.
    """

    # Source table for the dictionary.
    node.query(
        """
        CREATE TABLE dict_source (key String, value Int64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    node.query("INSERT INTO dict_source VALUES ('a', 1), ('b', 2), ('c', 3)")

    node.query(
        """
        CREATE DICTIONARY test_dict (key String, value Int64)
        PRIMARY KEY key
        SOURCE(CLICKHOUSE(TABLE 'dict_source'))
        LIFETIME(MIN 0 MAX 1000)
        LAYOUT(COMPLEX_KEY_HASHED())
        """
    )

    # Create a normal table and insert data.
    node.query(
        """
        CREATE TABLE test_table (key String, value Int64)
        ENGINE = MergeTree ORDER BY key
        """
    )
    node.query(
        "INSERT INTO test_table VALUES ('a', 10), ('b', 20), ('c', 30)"
    )

    assert node.query("SELECT count() FROM test_table").strip() == "3"

    def inject_nondeterministic_key(instance):
        """Replace the sorting key in the stored metadata with dictGet.

        This simulates what happens when alias expansion (PR #91352) resolves
        a column reference to a non-deterministic expression in the projection
        sorting key.
        """
        instance.exec_in_container(
            [
                "bash",
                "-c",
                r"sed -i 's/ORDER BY key/ORDER BY dictGet('\''default.test_dict'\'', '\''value'\'', key)/' "
                "/var/lib/clickhouse/metadata/default/test_table.sql",
            ]
        )

    # Upgrade to the version under test, modifying metadata in between.
    node.restart_with_latest_version(callback_onstop=inject_nondeterministic_key)

    # The table must attach and remain queryable.
    assert node.query("SELECT count() FROM test_table").strip() == "3"

    # Verify that new inserts still work.
    node.query("INSERT INTO test_table VALUES ('a', 40)")
    assert node.query("SELECT count() FROM test_table").strip() == "4"
