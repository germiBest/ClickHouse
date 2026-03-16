-- Test that CREATE TABLE/VIEW with EMPTY and COMMENT accepts both orderings
-- and formats consistently for AST roundtrip.
-- The formatter outputs COMMENT after AS SELECT (with parentheses) for backward compatibility.

-- Table: COMMENT then EMPTY (original parser order)
SELECT formatQuery('CREATE TABLE t (c Int8) ENGINE = MergeTree ORDER BY c COMMENT \'test\' EMPTY AS SELECT 1');

-- Table: EMPTY then COMMENT (alternative parser order)
SELECT formatQuery('CREATE TABLE t (c Int8) ENGINE = MergeTree ORDER BY c EMPTY COMMENT \'test\' AS SELECT 1');

-- Table: EMPTY without COMMENT
SELECT formatQuery('CREATE TABLE t (c Int8) ENGINE = MergeTree ORDER BY c EMPTY AS SELECT 1');

-- Table: COMMENT without EMPTY
SELECT formatQuery('CREATE TABLE t (c Int8) ENGINE = MergeTree ORDER BY c COMMENT \'test\' AS SELECT 1');

-- Materialized view: EMPTY then COMMENT
SELECT formatQuery('CREATE MATERIALIZED VIEW v (c Int8) ENGINE = MergeTree ORDER BY c EMPTY COMMENT \'test\' AS SELECT 1');

-- Materialized view: COMMENT then EMPTY
SELECT formatQuery('CREATE MATERIALIZED VIEW v (c Int8) ENGINE = MergeTree ORDER BY c COMMENT \'test\' EMPTY AS SELECT 1');

-- View: COMMENT before AS SELECT (new syntax, must be accepted by parser)
SELECT formatQuery('CREATE VIEW v COMMENT \'test\' AS SELECT 1');

-- View: COMMENT after AS (SELECT) (old syntax with required parentheses)
SELECT formatQuery('CREATE VIEW v AS (SELECT 1) COMMENT \'test\'');
