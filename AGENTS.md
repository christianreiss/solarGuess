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
Every task is one module end-to-end: design → code → tests → docs (as needed) → commit (no PRs).

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
4) **Branch (optional) or stay on main**
   - Default: keep history linear on `main`.
   - If you need isolation, create `task/<NN>-<short-slug>` from updated `main` and fast-forward merge when done.
5) **Implement fully**
   - Build the module from scratch (or extend it) with clear interfaces.
   - Add/update fixtures and unit tests.
   - Wire debug events (see “Debug contract” below).
6) **Test**
   - Run syntax check: `python -m compileall src`
   - Run unit tests: `pytest -q`
   - Fix failures until green.
7) **Commit (no PR step)**
   - Use descriptive commit messages: `git commit -am "Task <NN>: <title>"` (or multiple small commits if it aids review).
   - Push to `main` (or your task branch, then fast-forward `main`). Keep commits clean; avoid merge commits if possible.

## Testing policy update

- It is acceptable for tests to hit the network when necessary. Prefer fixtures for determinism and speed, but don’t block legitimate network use.

GitHub PR creation via `gh pr create` and the `--base/--head` flags are documented by GitHub. :contentReference[oaicite:1]{index=1}

## Commit note (replace PR template)
Include in commit message or accompanying note:
- What: Implements Task <NN> with a short summary.
- Why: What this module unlocks.
- How: Public API + key decisions.
- Tests run: list commands (e.g., `python -m compileall src`, `pytest -q`).
- Debug sample: one JSONL line showing emitted structure.
