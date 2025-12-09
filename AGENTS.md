# AGENTS.md

## Project overview (goal)
Build a **day solar generation predictor** in Python that estimates energy (kWh) and power (W) per timestep for:
- **1..N PV arrays** per site
- **1..N locations/sites**
- With **explainable debug output** at every stage (JSONL), so results can be audited or cross-checked.

Core idea: forecast weather + irradiance → sun position → plane-of-array irradiance → cell temperature → DC power → AC power → losses → aggregation.

## Non-negotiables (quality gates)
- Modular design: each module has a clean API and minimal responsibilities.
- Every module must have:
  - **syntax tests** (e.g., `python -m compileall`)
  - **logic tests** (unit tests with deterministic fixtures)
- All code paths support **debug emission** (structured, deterministic JSON).

## Repo workflow (MANDATORY per task)
Every task is one module end-to-end: design → code → tests → docs (as needed) → PR.

### Task checklist (do these in order)
1) **Understand the overall goal**
   - Read this file (AGENTS.md) and the project README (if present).
2) **Understand the current state of the project**
   - `git status`
   - skim `src/` + `tests/`
   - run tests to see baseline (even if failing early tasks): `pytest -q`
3) **Understand the goal for the current task**
   - Read the task description (see “Task map” below).
   - Define “done” in concrete terms (public API + tests + debug).
4) **Branch out in git**
   - Start from updated default branch:
     - `git checkout main`
     - `git pull --ff-only`
   - Create a task branch:
     - `git checkout -b task/<NN>-<short-slug>`
5) **Implement fully**
   - Build the module from scratch (or extend it) with clear interfaces.
   - Add/update fixtures and unit tests.
   - Wire debug events (see “Debug contract” below).
6) **Test**
   - Run syntax check: `python -m compileall src`
   - Run unit tests: `pytest -q`
   - Fix failures until green.
7) **Push + create a PR (required)**
   - Push branch:
     - `git push -u origin task/<NN>-<short-slug>`
   - Create PR (prefer GitHub CLI):
     - `gh pr create --base main --head task/<NN>-<short-slug> --title "Task <NN>: <title>" --body "<PR template below>"`
   - If `gh` isn’t available, push the branch and include PR-ready title/body in the final output/log so a human can paste it into the GitHub UI.

> Notes on PRs:
> - One PR per task. Do not mix tasks.
> - The PR must describe what changed, what tests ran, and include a small debug excerpt.

## Testing policy update

- It is acceptable for tests to hit the network when necessary. Prefer fixtures for determinism and speed, but don’t block legitimate network use.

GitHub PR creation via `gh pr create` and the `--base/--head` flags are documented by GitHub. :contentReference[oaicite:1]{index=1}

## PR template (paste into PR body)
### What
- Implements Task <NN>: <short summary>

### Why
- <why this module exists / what it unlocks>

### How
- Public API:
  - `...`
- Key decisions:
  - `...`

### Tests run
- `python -m compileall src`
- `pytest -q`

### Debug sample (JSONL)
```json
{"stage":"...","site":"...","array":"...","ts":"...","data":{...}}
