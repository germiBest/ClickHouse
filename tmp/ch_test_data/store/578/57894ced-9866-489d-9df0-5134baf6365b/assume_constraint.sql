ATTACH TABLE _ UUID '4ebe34e2-5fdd-422e-aff4-b894345d2444'
(
    `c0` Int32,
    CONSTRAINT c0 ASSUME (
        SELECT 1
    )
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192
