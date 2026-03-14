-- Tags: no-random-merge-tree-settings, no-parallel-replicas, replica
-- Test `ReplicatedMergeTreeQueue`: commit order preserved through merge.

set enable_analyzer = 1;
set insert_keeper_fault_injection_probability = 0;

drop table if exists rmtq sync;

CREATE TABLE rmtq(a UInt64)
ENGINE = ReplicatedMergeTreeQueue('/clickhouse/tables/{database}/rmtq', '1')
settings index_granularity=1;

-- Insert several parts
insert into rmtq values (30) (10) (20);

detach table rmtq;
attach table rmtq;

insert into rmtq values (60) (40) (50);

detach table rmtq;
attach table rmtq;

insert into rmtq values (90) (70) (80);

-- Level-0 parts: data is sorted by commit order without explicit ORDER BY
select 'level-0 data';
select a, _block_number, _block_offset from rmtq settings max_threads=1;

-- Verify primary index has correct `_block_number` values via `mergeTreeIndex`
select '';
select 'level-0 primary index';
select part_name, _block_number, _block_offset
from mergeTreeIndex(currentDatabase(), 'rmtq')
order by part_name, mark_number;

-- Verify sorting key includes virtual columns
select '';
select 'sorting key';
select sorting_key from system.tables where database = currentDatabase() and name = 'rmtq';

-- Index lookup on level-0 parts
select '';
select 'level-0 index lookup';
select a from rmtq where (_block_number, _block_offset) = (1, 1);

select '';
select 'level-0 index lookup explain';
explain indexes=1 select a from rmtq where (_block_number, _block_offset) = (1, 1);

-- Merge and verify commit order is preserved
optimize table rmtq final;

select '';
select 'after merge';
select a, _block_number, _block_offset from rmtq settings max_threads=1;

-- Index lookup after merge
select '';
select 'after merge index lookup';
select a from rmtq where (_block_number, _block_offset) = (1, 1);

select '';
select 'after merge index lookup explain';
explain indexes=1 select a from rmtq where (_block_number, _block_offset) = (1, 1);

drop table rmtq sync;
