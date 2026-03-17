# Learnings

- Queries like `SELECT 1` implicitly use `system.one`, which is in the `shouldIgnoreQuotaAndLimits` exemption list. Use `SELECT ... FROM numbers(...)` or other non-exempt tables when testing quota enforcement.
- Access entities (quotas, users, roles) are stored relative to CWD in `access/` directory (not in the `--path` data directory), unless overridden in config.
- The `default` XML quota is always present and may be matched before SQL-created quotas in `chooseQuotaToConsumeFor` (iteration order depends on UUID). Use separate users/roles for quota tests.
- `HashMap::find` returns a raw pointer, so clang-tidy requires `auto *` instead of `auto`.
