ATTACH TABLE _ UUID '8fb8f77c-96fd-42df-a528-c646bea3ea64'
(
    `c0` Int32,
    CONSTRAINT c0 CHECK (
        SELECT 1
    )
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192
