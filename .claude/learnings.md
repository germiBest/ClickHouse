# Learnings

- `numbers_mt` has `storage_has_evenly_distributed_read = true`, so the pre-aggregation resize is skipped for it. To test resize behavior, use a real MergeTree table with enough data to produce multiple read streams.
- Fresh ClickHouse build from scratch takes ~10 minutes on this machine (96 cores, arm64, clang-21). Use `ninja -C build clickhouse` as the target name.
- The cmake cache needs to be cleared if switching from gcc to clang: remove `CMakeCache.txt` and `CMakeFiles/`.
- Build target for the main binary is `clickhouse`, not `clickhouse-server`.
