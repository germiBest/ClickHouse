-- Tags: no-fasttest

-- Test that the AST fuzzer oracle checks (TLP WHERE and NoREC) work correctly
-- on valid queries. The oracles run inside the server and should not throw
-- on correct query results.

DROP TABLE IF EXISTS oracle_test;

CREATE TABLE oracle_test
(
    x UInt32,
    y Nullable(String),
    z Float64
)
ENGINE = MergeTree
ORDER BY x;

INSERT INTO oracle_test VALUES (1, 'a', 0.5), (2, 'b', 1.5), (3, NULL, 2.5), (0, 'd', -1.0), (5, 'e', 3.0), (10, NULL, 0.0);

-- Simple WHERE with comparison
SELECT * FROM oracle_test WHERE x > 2 SETTINGS ast_fuzzer_runs = 1, ast_fuzzer_oracle = 1;

-- WHERE with IS NOT NULL
SELECT x, y FROM oracle_test WHERE y IS NOT NULL SETTINGS ast_fuzzer_runs = 1, ast_fuzzer_oracle = 1;

-- WHERE with arithmetic expression
SELECT x, z FROM oracle_test WHERE z > 0 SETTINGS ast_fuzzer_runs = 1, ast_fuzzer_oracle = 1;

-- WHERE with AND
SELECT * FROM oracle_test WHERE x > 1 AND y IS NOT NULL SETTINGS ast_fuzzer_runs = 1, ast_fuzzer_oracle = 1;

-- WHERE with OR
SELECT x FROM oracle_test WHERE x = 0 OR x = 5 SETTINGS ast_fuzzer_runs = 1, ast_fuzzer_oracle = 1;

-- Empty result set (oracles should still work)
SELECT * FROM oracle_test WHERE x > 100 SETTINGS ast_fuzzer_runs = 1, ast_fuzzer_oracle = 1;

DROP TABLE oracle_test;
