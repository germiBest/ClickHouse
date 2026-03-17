ATTACH TABLE _ UUID '4f649890-ef5b-40c5-a354-86b359add85d'
(
    `a` UInt32,
    `b` UInt32,
    PROJECTION proj_sum
    (
        SELECT
            a,
            sum(b)
        GROUP BY a
    )
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 8192
