-- Tags: no-random-merge-tree-settings
-- Verify that `MergeTreeQueue` does not support merging modes.

select 'SummingMergeTreeQueue';
CREATE TABLE mtq_summing(a UInt64) ENGINE = SummingMergeTreeQueue ORDER BY a; -- { serverError UNKNOWN_STORAGE }

select 'ReplacingMergeTreeQueue';
CREATE TABLE mtq_replacing(a UInt64) ENGINE = ReplacingMergeTreeQueue ORDER BY a; -- { serverError UNKNOWN_STORAGE }

select 'CollapsingMergeTreeQueue';
CREATE TABLE mtq_collapsing(a UInt64, s Int8) ENGINE = CollapsingMergeTreeQueue(s) ORDER BY a; -- { serverError UNKNOWN_STORAGE }

select 'AggregatingMergeTreeQueue';
CREATE TABLE mtq_aggregating(a UInt64) ENGINE = AggregatingMergeTreeQueue ORDER BY a; -- { serverError UNKNOWN_STORAGE }
