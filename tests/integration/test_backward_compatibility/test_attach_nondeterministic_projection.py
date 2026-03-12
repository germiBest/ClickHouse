import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    with_zookeeper=False,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
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
    """Projections whose sorting key aliases a non-deterministic function must
    still attach after upgrade.

    PR #91352 added alias expansion in ``getProjectionFromAST`` which resolves
    column aliases to their underlying expressions.  When a projection has
    ``dictGet(...) AS value ORDER BY value`` and the table also has a column
    named ``value``, the old binary resolves ``ORDER BY value`` to the table
    column (deterministic).  The new binary's Analyzer resolves it to the
    projection-level alias ``dictGet(...)`` instead, producing a
    non-deterministic sorting key that previously caused ``assertDeterministic``
    in ``checkKeyExpression`` to reject the table on attach.

    The determinism check is meaningful at CREATE TABLE time but must be
    skipped during ATTACH, because the table already exists on disk.

    This reproduces the actual CI failure from the upgrade check where
    ``03755_circular_dictionary.sql`` created tables with normal projections
    containing ``dictGet(...) AS value ORDER BY value`` on the old binary,
    and the new binary failed to attach them after upgrade.
    """

    # Source table for the dictionary.
    node.query(
        """
        CREATE TABLE dict_source (metric String, value Int64)
        ENGINE = MergeTree ORDER BY metric
        """
    )
    node.query(
        "INSERT INTO dict_source VALUES "
        "('BackgroundPoolTask', 1), ('BackgroundMergesAndMutationsPoolTask', 2)"
    )

    node.query(
        """
        CREATE DICTIONARY test_dict (metric String, value Int64)
        PRIMARY KEY metric
        SOURCE(CLICKHOUSE(TABLE 'dict_source'))
        LIFETIME(MIN 0 MAX 1000)
        LAYOUT(COMPLEX_KEY_HASHED())
        """
    )

    # Create a table with a projection whose ORDER BY references an alias
    # that shadows the table column ``value``.  The old binary resolves
    # ``ORDER BY value`` to the table column; the new binary's Analyzer
    # expands it to the projection alias ``dictGet(...)``.
    node.query(
        """
        CREATE TABLE test_table
        (
            metric String,
            value Int64,
            PROJECTION values
            (
                SELECT
                    metric,
                    dictGet('default.test_dict', 'value', metric) AS value
                ORDER BY value
            )
        )
        ENGINE = MergeTree ORDER BY metric
        """
    )
    node.query(
        "INSERT INTO test_table VALUES "
        "('BackgroundPoolTask', 10), ('BackgroundMergesAndMutationsPoolTask', 20)"
    )

    assert node.query("SELECT count() FROM test_table").strip() == "2"

    # Upgrade to the version under test — no metadata modification needed.
    # The alias expansion in getProjectionFromAST naturally produces the
    # non-deterministic sorting key during ATTACH.
    node.restart_with_latest_version()

    # The table must attach and remain queryable.
    assert node.query("SELECT count() FROM test_table").strip() == "2"

    # Verify that new inserts still work.
    node.query("INSERT INTO test_table VALUES ('BackgroundPoolTask', 30)")
    assert node.query("SELECT count() FROM test_table").strip() == "3"
