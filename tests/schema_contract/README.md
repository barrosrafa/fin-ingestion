# Versioned schema contract for isolated tests

These immutable snapshots preserve the historical fin-db stages 14 and 17 used by
the deployed migration ledger. They let CI exercise real PostgreSQL without checking
out a second repository. `manifest.json` records SHA-256 values for migration files.
The current schema-only DDL is published in fin-db stage 20; its engine definitions
match historical stage 17. Stage 14 retains the historical test seed intentionally.
The legacy market snapshots contain only the original daily/intraday CREATE TABLEs.
