---
name: fin-ingestion-ops
description: Configure, deploy and verify the Fin ingestion scheduler, durable PostgreSQL recovery and optional free Cloudflare watchdog. Use for Fin ingestion operations and delayed or missing financial jobs.
---

# Fin ingestion operations

Work from the user's existing authorization. Deployment permission persists
through validation, push, workflow dispatch and release promotion. Preserve a
free-infrastructure constraint; do not activate paid hosting as an automatic fix.

## Locate the runtime

The default local repositories are `~/fin-ingestion`, `~/fin` (fin-db authority)
and `~/fin-infra`. A second fin-db checkout under `~/fin/fin-db` is not the place
to apply competing edits. Discover the actual repositories and current branch
before changing them. Preserve unrelated working-tree edits.

Read `fin-ingestion/docs/DEPLOY_ENGINE.md` and
`fin-infra/ingestion/README.md` for the live entry points and rollout state.
The code's `operations/reliable_runtime.py` owns the BRT schedule. GitHub cron and
Cloudflare only wake the runtime; PostgreSQL owns cursors, jobs and leases.

## Diagnose or deploy

- Prefer authenticated GitHub CLI. If `gh` is absent from PATH on this machine,
  look for `~/fin-run/tools/github-cli/gh` or its `gh-auth` wrapper. Check auth
  status without printing tokens. Use browser settings when CLI cannot perform
  a needed action; do not infer that the connector's read-only scope means the
  browser or local CLI also lacks access.
- Inspect workflow state, exact release SHA, run creation/job start timestamps,
  job receipts and `ingestion.v_runtime_health`. A green skipped job is not proof
  of collection. Distinguish late trigger delivery from database startup errors.
- Neon resumes idle compute on connection. Use the bounded readiness helper;
  never replay an uncertain arbitrary transaction or disable suspension by
  switching to a paid plan without authorization.
- Validate with Python 3.12 and isolated PostgreSQL/Redis. The homologation script
  supports a local Redis executable; no live source flags are needed for tests.
  Compare the bundled runtime DDL with fin-db and check migration replay.
- Push reviewed changes, require CI success for that exact SHA, run
  `Deploy scheduled engine`, inspect its receipt, then promote
  `ENGINE_RELEASE_SHA` and dispatch `profile=recover`. Check the resulting jobs,
  rows, Gold tests, pending/failed counts and next schedule cycle. Do not label
  a pushed commit as deployed before the pinned release is promoted.

## Optional free external scheduler

Use the Cloudflare Worker under `fin-infra/ingestion/cloudflare` and its runbook.
It needs an existing free account, a GitHub fine-grained token restricted to this
repository with Actions read/write, and a separate endpoint health secret.
Keep all secrets outside Git, command arguments, screenshots and diagnostic logs.
Do not copy the local broad administrator token to the Worker.

If a CAPTCHA or human verification appears, pause that browser branch and ask
the user to complete it. A skill, plugin or MCP does not bypass it. Continue
independent code/testing work while waiting. Report a configured-but-undeployed
Worker separately from an observed external dispatch.

## Verify recovery and stop

Prove duplicate triggers do not duplicate jobs, expired leases can be reclaimed,
an old token cannot confirm a job, failed work remains durable, and stale intraday
slots expire without inventing historical prices. Use isolated tests for crashes;
do not kill production jobs or suspend production compute solely for a test.

After a transient failure, allow at most three manual reruns while diagnosing
the cause; leave remaining recovery to the bounded durable retry policy. For
permanent errors, retain failure evidence and stop that failing path. Report
commit/deploy links, observed verification and any unresolved provider access.
