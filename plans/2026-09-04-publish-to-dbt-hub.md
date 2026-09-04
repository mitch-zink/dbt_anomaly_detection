# Publish `dbt_anomaly_detection` to dbt Hub

Repo: `mitch-zink/dbt_anomaly_detection` (public, MIT). Consumed today by `prefect-ec2/prefect/dbt_tests/packages.yml` via `git` + `revision: main` (floating, unpinned).

Sources for requirements (fetched 2026-09-04):
- https://docs.getdbt.com/guides/building-packages — official "build a package" guide
- https://docs.getdbt.com/guides/dbt-package-compat — Fusion compatibility upgrade guide
- https://github.com/dbt-labs/hubcap — the bot that actually ingests `hub.json` and publishes to hub.getdbt.com (README + `package-best-practices.md` + `hubcap/version.py`)

---

## Decisions needed from Mitch before starting

### 1. What ships in this release — LOCKED
`main` is 2 commits ahead of the last tag (`v0.1.0`): day-of-week seasonality support (`e1adffa`, merged via PR #1). That's additive and safe to ship as `v0.2.0`.

Separately, there's an **unmerged** branch `feature/robust-prep-final-anomaly-models` (6 commits) that adds a whole second detection architecture (median/MAD prep+final models, `README_v2`). This is a real design decision, not a mechanical merge — it changes how anomalies get computed.

**Decision (confirmed 2026-09-04): NOT merging it for this release.** `v0.2.0` ships seasonality-only, off current `main`. `feature/robust-prep-final-anomaly-models` stays parked for its own separate review later.

Correction from the original draft: that branch's commit `9055f19` was assumed to be a clean, cherry-pickable `tests:`→`data_tests:` mechanical rename. Verified against the actual local tooling (`uvx dbt-autofix deprecations --dry-run`, see Phase 1.2) — this install is **dbt-fusion 2.0.0-preview**, not dbt-core, and its autofix output doesn't do a simple key rename at all. It flags moving `where` under `config`, moving custom test arguments under `arguments`, and flipping a behavior flag (`require_generic_test_arguments_property`). That's real behavior-affecting surgery across 33 test definitions, not a safe cherry-pick. Not doing it blind — see Phase 1.2 for the actual path.

### 2. `prefect-ec2` floats on `main`, unpinned
Once you push new commits to fix things below, `prefect-ec2`'s next dbt run picks them up immediately — there's no version pin protecting it. Plan below pins it to the new tag once cut (Phase 5), but the window between "fix commits land on main" and "tag cut" is live-fire for that pipeline. Fine given these are compatibility/cleanup fixes only, not logic changes — flagging so it's a conscious choice, not a surprise.

---

## Current-state audit

| Requirement (dbt Hub / hubcap) | Status |
|---|---|
| Hosted on GitHub, public | ✅ |
| `dbt_project.yml` has `name:` matching repo | ✅ `dbt_anomaly_detection` |
| Detectable LICENSE (GitHub-recognized) | ✅ MIT |
| `packages.yml` at repo root, hub-sourced deps (not raw git) | ✅ `dbt-labs/dbt_utils` via hub range |
| No hardcoded table refs (must use `ref`/`source`) | ✅ spot-checked, none found |
| `require-dbt-version` declared | ✅ but excludes Fusion (`<2.0.0`) — see Phase 1 |
| README: install instructions point at a real org/version | ❌ literally says `YOUR_ORG` and a stale `revision: 0.2.0` placeholder |
| Git tag is semver and matches shipped code | ⚠️ `v0.1.0` tag exists and is pushed, but `main` has unreleased commits past it |
| GitHub Release (with notes) | ❌ none published — tag exists but no Release object |
| `integration_tests/` project | ❌ absent (optional per guide, but it's how you validate before real users hit it, and how you'd Fusion-test) |
| Deprecated `tests:` key vs current `data_tests:` | ❌ used everywhere (`models/*/schema.yml`, `snapshots/schema.yml`) — dbt-autofix will rewrite this |
| CI (lint/build on PR) | ❌ none (optional) |
| `.github/` PR/issue templates | ❌ none (optional) |
| Repo topics for discoverability | ❌ none set |
| "Used in real production" (hubcap best-practice) | ✅ live in `prefect-ec2` since before `v0.1.0`, multi-commit history, not a one-shot |

Nothing here blocks publishing outright except the README placeholder and cutting a real tag/release — everything else is "should fix" per dbt Hub's own best-practices doc, called out below as MUST vs SHOULD.

---

## Phase 1 — Code readiness (on `main`, before tagging)

1. **Fix README placeholders** (`README.md`) — DONE:
   - `git: "https://github.com/YOUR_ORG/dbt_anomaly_detection.git"` → `https://github.com/mitch-zink/dbt_anomaly_detection.git`
   - `revision: 0.2.0` — left as-is, now correct since that's the tag Phase 3 cuts.

2. **`tests:` → `data_tests:` / other schema deprecations** — HOLDING, not applying.
   Local `dbt` resolves to **dbt-fusion 2.0.0-preview**, not dbt-core. `uvx dbt-autofix deprecations --dry-run --path .` against this project found:
   - `snapshots/schema.yml`: 11 test definitions need `where` moved under `config`, plus 2 custom-test args (`values`, `expression`) moved under `arguments`
   - `dbt_project.yml`: flip behavior flag `require_generic_test_arguments_property` to `True`, and drop the deprecated `target-path` key
   No `models/*/schema.yml` changes were flagged by this run — worth re-checking with `--select models` once decided, dry run above covered the whole project in one pass and only printed the snapshot + project-level diffs.
   This is real parsing-behavior surgery (how test args get interpreted), not cosmetic — **needs your review of the dry-run output before applying**, not a rubber-stamp. Run `uvx dbt-autofix deprecations --dry-run --path .` yourself to see the full diff, then `uvx dbt-autofix deprecations --path .` (drop `--dry-run`) to apply once you're good with it.

3. **`require-dbt-version` / Fusion badge** — HOLDING, not bumping.
   Bumping to include `2.0.0` before actually building the project end-to-end with Fusion would be an unverified claim. Real blocker: `dbt_project.yml` declares `profile: 'dbt_anomaly_detection'`, and there's no such profile in `~/.dbt/profiles.yml` (Snowflake creds needed — this package is Snowflake-only). Ship `v0.2.0` without the Fusion claim; revisit as `v0.2.1` once Phase 1.2's autofix changes are applied AND there's a real Snowflake target to `dbt parse`/`dbt build` against (the `integration_tests/` project from Phase 2.1 is the right place for that target, not a one-off local hack).

4. **Model naming convention (optional, flagging not fixing)** — dbt Hub's modeling-package convention says prefix models with the package name (e.g. `mailchimp_campaigns` not `campaigns`) to avoid name collisions in a consumer's project. This package ships `stg_monitored_tables`, `volume_metrics_history`, `freshness_metrics_history` unprefixed. `prefect-ec2` is already live on these names — renaming is a breaking change for zero functional gain right now. **Recommendation: don't rename for this release.** Only reconsider if a hub user reports an actual name collision.

---

## Phase 2 — Package hygiene (optional but cheap, do these)

1. **`integration_tests/` project** — copy the pattern from `dbt-labs/dbt-codegen`'s `integration_tests/` (empty `models/`, `macros/`, `tests/`, a `packages.yml` with `- local: ../`, a seed or two of anonymized sample rows). This is what the dbt Hub best-practices doc calls "the way you demonstrate the package actually works," and it's also the prerequisite for automated Fusion testing later. Given the package needs Snowflake `information_schema` + `generator()`, integration tests need a real Snowflake target — reuse a scratch schema, not prod.
2. **Repo topics** (GitHub → About → topics): `dbt`, `dbt-package`, `snowflake`, `data-quality`, `anomaly-detection`, `data-observability`. Pure discoverability, zero cost.
3. **GitHub Actions CI** (optional, skip for v1 unless you want it) — a workflow that runs `dbt deps && dbt parse` on PRs at minimum; full `dbt build` needs a live Snowflake connection via repo secrets.
4. **LICENSE copyright year** — says `2025`, cosmetic only, not required by hubcap. Skip unless touching the file anyway.

---

## Phase 3 — Version and release

1. ~~Bump `dbt_project.yml` `version:` from `'0.1.0'` to `'0.2.0'`~~ — DONE (minor bump, additive seasonality feature since last tag, per semver guidance in the hub best-practices doc).
2. **NOT YET DONE — needs your go-ahead**: commit the README fix + version bump to `main` (no AI attribution, per standing rule), push, then:
3. Tag: `git tag v0.2.0 && git push origin v0.2.0`.
4. Publish an actual GitHub Release (`gh release create v0.2.0 --title "v0.2.0" --notes "..."`) summarizing: seasonality support, README fix. (Skip the `data_tests:` migration / Fusion mention until Phase 1.2/1.3 are actually applied — don't claim work that isn't done.) hubcap itself only reads git tags, but a Release with real notes is what the building-packages guide asks for and what a human reviewer on the hub PR (Phase 4) will actually read.

Push/tag/release are the first externally-visible, hard-to-reverse steps in this plan — holding here until you say go.

---

## Phase 4 — Submit to dbt Hub (the hubcap PR)

dbt Hub (hub.getdbt.com) is generated by the `hubcap` bot from `dbt-labs/hubcap`'s `hub.json`. There's no dashboard/form — it's a PR against that file.

1. Fork `dbt-labs/hubcap` (or branch if you're later granted write, but default to fork+PR).
2. Add an entry to `hub.json`, alphabetically near the other `mi*`/`M*` entries:
   ```json
   "mitch-zink": [
       "dbt_anomaly_detection"
   ]
   ```
3. Open the PR against `dbt-labs/hubcap`. Reviewed by a dbt Labs team member, "typically within one business day" per hubcap's own README. Their review is the cursory pass against `package-best-practices.md` — everything in Phase 1/2 is prep for that review, not busywork.
4. Once merged, hubcap's hourly job picks up `v0.2.0` from the pushed tag and opens its own PR against `hub.getdbt.com` to render the listing — nothing further needed from us.

---

## Phase 5 — Post-publish cleanup

1. **Pin `prefect-ec2`** off the floating `main` git dependency onto the real release:
   ```yaml
   # prefect-ec2/prefect/dbt_tests/packages.yml
   - package: mitch-zink/dbt_anomaly_detection
     version: [">=0.2.0", "<0.3.0"]
   ```
   (Once it's on the hub — hub-sourced deps resolve faster and dedupe better than `git`, and it's what the building-packages guide recommends for your *own* consumers too.) This is a `prefect-ec2` change — separate PR, own worktree, per the Job Agent workflow convention.
2. Re-run `dbt deps` in `prefect-ec2` to confirm the pinned hub package resolves and the dbt_tests project still builds clean.
3. (Optional) Post in dbt Slack `#package-ecosystem` — explicitly optional per the guide, skip unless you want the visibility.
