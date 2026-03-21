-- Vertical merge of Nested arrays with DEFAULT expressions referencing
-- non-existent sibling columns must not corrupt data.
-- https://github.com/ClickHouse/ClickHouse/issues/86123

DROP TABLE IF EXISTS t_nested_vertical;

CREATE TABLE t_nested_vertical (
    id UInt32,
    `n1.nums` Array(UInt32),
    `n1.numsplus` Array(UInt32),
    `n2.nums` Array(UInt32),
    `n2.numsplus` Array(UInt32)
) ENGINE = ReplacingMergeTree() ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES t_nested_vertical;

INSERT INTO t_nested_vertical VALUES (1, [1,1,1], [2,2,2], [11,11,11], [22,22,22]);
INSERT INTO t_nested_vertical VALUES (2, [2,2,2], [3,3,3], [22,22,22], [33,33,33]);
INSERT INTO t_nested_vertical VALUES (3, [3,3,3], [4,4,4], [33,33,33], [44,44,44]);
INSERT INTO t_nested_vertical VALUES (4, [4,4,4], [5,5,5], [44,44,44], [55,55,55]);

ALTER TABLE t_nested_vertical ADD COLUMN `n1.urls` Array(Array(String));
ALTER TABLE t_nested_vertical ADD COLUMN `n1.domains` Array(Array(String))
    DEFAULT arrayMap(x -> arrayMap(y -> domain(y), CAST(x AS Array(String))), `n1.urls`);

-- Verify data is intact before merge
SELECT id, `n1.nums`, `n1.numsplus`, `n2.nums`, `n2.numsplus` FROM t_nested_vertical ORDER BY id;

SYSTEM START MERGES t_nested_vertical;
OPTIMIZE TABLE t_nested_vertical FINAL;

-- After vertical merge, Nested arrays must not be corrupted
SELECT id, `n1.nums`, `n1.numsplus`, `n2.nums`, `n2.numsplus` FROM t_nested_vertical ORDER BY id;

-- The new columns should return correct defaults (empty arrays matching Nested dimensions)
SELECT id, `n1.urls`, `n1.domains` FROM t_nested_vertical ORDER BY id;

DROP TABLE t_nested_vertical;
