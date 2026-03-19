# Learnings

- Queries like `SELECT 1` implicitly use `system.one`, which is in the `shouldIgnoreQuotaAndLimits` exemption list. Use `SELECT ... FROM numbers(...)` or other non-exempt tables when testing quota enforcement.
- Access entities (quotas, users, roles) are stored relative to CWD in `access/` directory (not in the `--path` data directory), unless overridden in config.
- The `default` XML quota is always present and may be matched before SQL-created quotas in `chooseQuotaToConsumeFor` (iteration order depends on UUID). Use separate users/roles for quota tests.
- `HashMap::find` returns a raw pointer, so clang-tidy requires `auto *` instead of `auto`.
- When adding new settings in `Settings.cpp`, you must also add `extern` declarations in every `.cpp` file that uses them via `Setting::name` (e.g., in the `namespace Setting` block in `executeQuery.cpp`, `ClientBase.cpp`).
- Rust crate build targets in ninja have the form `rust/workspace/cargo-build__ch_rust_<name>`, not just `_ch_rust_<name>`.
