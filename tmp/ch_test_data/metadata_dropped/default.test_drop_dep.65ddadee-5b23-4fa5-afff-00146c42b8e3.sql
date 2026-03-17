ATTACH TABLE _ UUID '65ddadee-5b23-4fa5-afff-00146c42b8e3'
(
    `a` UInt32,
    `b` UInt32 DEFAULT a + 1
)
ENGINE = MergeTree
ORDER BY a
SETTINGS index_granularity = 8192
