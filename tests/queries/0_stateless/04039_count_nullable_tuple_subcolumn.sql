-- Tags: no-ordinary-database

-- Regression test for LOGICAL_ERROR: Bad cast from ColumnNullable to ColumnVector<char8_t>
-- The FunctionToSubcolumnsPass replaced count(nullable_arg) with sum(not(nullable_arg.null)),
-- hardcoding the .null subcolumn type as UInt8. But for Nullable(Tuple(... Nullable(T) ...)),
-- the .null subcolumn in storage is Nullable(UInt8), causing a type mismatch in AggregateFunctionSum.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?REF=master&sha=1550dbafd9751ce927a525d5201f36ad9985a3cd&name_0=MasterCI&name_1=AST%20fuzzer%20%28amd_tsan%29

SET allow_experimental_nullable_tuple_type = 1;

DROP TABLE IF EXISTS t_nullable_tuple;

CREATE TABLE t_nullable_tuple
(
    `tup` Nullable(Tuple(u UInt64, s Nullable(String)))
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0, nullable_serialization_version = 'allow_sparse', min_bytes_for_wide_part = 0;

INSERT INTO t_nullable_tuple SELECT if((number % 5) = 0, (number, toString(number)), NULL) FROM numbers(1000);

-- This used to cause LOGICAL_ERROR: Bad cast from ColumnNullable to ColumnVector<char8_t>
SELECT count(tup.s) FROM t_nullable_tuple SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count(tup.u) FROM t_nullable_tuple SETTINGS optimize_functions_to_subcolumns = 1;

-- Also test with DISTINCT as in the original fuzzer query
SELECT DISTINCT count(tup.s) FROM t_nullable_tuple SETTINGS optimize_functions_to_subcolumns = 1;

DROP TABLE t_nullable_tuple;
