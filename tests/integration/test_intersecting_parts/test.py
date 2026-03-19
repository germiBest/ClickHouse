import logging
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


# This test constructs intersecting parts intentionally. It's not an elegant test.
# TODO(hanfei): write a test which select part 1_1 merging with part 2_2 and drop range.
def test_intersect_parts_when_restart(started_cluster):
    table = "data_" + uuid.uuid4().hex[:8]
    node.query(
        f"""
         CREATE TABLE {table} (
             key Int
         )
         ENGINE = ReplicatedMergeTree('/ch/tables/default/{table}', 'node')
         ORDER BY key;
         """
    )
    node.query(f"system stop cleanup {table}")
    node.query(f"INSERT INTO {table} values (1)")
    node.query(f"INSERT INTO {table} values (2)")
    node.query(f"INSERT INTO {table} values (3)")
    node.query(f"INSERT INTO {table} values (4)")
    node.query(f"ALTER TABLE {table} DROP PART 'all_1_1_0'")
    node.query(f"ALTER TABLE {table} DROP PART 'all_2_2_0'")
    node.query(f"OPTIMIZE TABLE {table} FINAL")

    part_path = node.query(
        f"SELECT path FROM system.parts WHERE table = '{table}' and name = 'all_0_3_1'"
    ).strip()

    assert len(part_path) != 0

    node.query(f"detach table {table}")
    new_path = part_path[:-6] + "1_2_3"
    node.exec_in_container(
        [
            "bash",
            "-c",
            "cp -r {p} {p1}".format(p=part_path, p1=new_path),
        ],
        privileged=True,
    )

    # mock empty part
    node.exec_in_container(
        [
            "bash",
            "-c",
            "echo -n 0 > {p1}/count.txt".format(p1=new_path),
        ],
        privileged=True,
    )

    node.query(f"attach table {table}")
    data_size = node.query(f"SELECT sum(key) FROM {table}").strip()
    assert data_size == "5"
