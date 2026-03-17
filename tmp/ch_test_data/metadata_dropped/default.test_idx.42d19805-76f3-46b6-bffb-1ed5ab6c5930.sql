ATTACH TABLE _ UUID '42d19805-76f3-46b6-bffb-1ed5ab6c5930'
(
    `a` String,
    `b` String,
    INDEX idx_a a TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 8192
