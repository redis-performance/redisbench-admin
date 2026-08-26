# Cross-cutting nitpick taxonomy — redisbench-admin, real precedent only

Grounded in actual GitHub review history, actual merged bugfix commits, and
this repo's own `AGENTS.md`/`CONTRIBUTING.md` on
`redis-performance/redisbench-admin` (~250 PRs surveyed, 2023–2026). Unlike a
project with a long, dense review history, several of these categories are
evidenced by a *single* real occurrence — that is being honest about the
actual size of the record, not a weakness to paper over. Where a category is
genuinely thin, this file says so; do not treat a single citation as if it
were a settled, oft-repeated maintainer doctrine.

1. **An opt-in "don't fail the build" flag must actually not fail the
   build.** The single sharpest real bug catch in this repo's mined review
   history: PR#541 added `--continue-on-redistimeseries-export-error` to make
   a transient-error path non-fatal, but `return_code |= 1` was set *before*
   the new `if/else`, so the flag suppressed the exception yet the process
   still exited non-zero — silently defeating the whole point of the flag
   (kei-nan, real review comment, with exact line-level reasoning). Any PR
   that adds an opt-in "continue on error" / "best-effort" flag should be
   checked for exactly this: does every code path affected by the flag
   (exceptions raised, `return_code`/exit-code accumulation, log level)
   actually change, or does only the most visible one?

2. **Retry/backoff policies need their worst-case wall-clock reasoned about,
   not just their existence.** Same PR#541 review: kei-nan computed the
   actual worst-case delay from `ExponentialBackoff(cap=10, base=1),
   retries=5` (~25s per call) and multiplied it out against how many calls a
   single benchmark's metrics export issues, concluding a fully-down endpoint
   could stall a run for minutes. This is real, on-point precedent for doing
   the arithmetic on any new retry/backoff/timeout constant rather than
   accepting "it retries now" as sufficient — this project's benchmarks run
   in CI where a multi-minute stall has a real, direct cost.

3. **A new "should this be opt-in or the default?" flag deserves an explicit
   answer in the PR description, not just the choice.** kei-nan, PR#541:
   *"Conservative default-off is fine for a library, but every known caller
   of this code path... would set the flag... Worth a sentence... about why
   opt-in was preferred over flipping the default."* This is a real,
   evidenced ask specifically about *why*, not a demand to change the
   default — check whether a new backward-compat-motivated flag's rationale
   for its default is stated anywhere, and if it plausibly always gets
   turned on by every real caller, it's fair to ask about it the way kei-nan
   did.

4. **Test coverage is real, explicit written doctrine here — but not a
   literally enforced hard gate in practice.** `CONTRIBUTING.md` states
   plainly: "All new behaviour must be covered by tests... Coverage should
   not decrease," and Codecov posts an automatic patch-coverage percentage on
   every PR. Be accurate, though: PRs have been merged in this repo's real
   history with patch coverage well under 100% (25% on PR#538, 85.6% on
   PR#549) — so cite the written rule as real, and a low number as worth
   surfacing on a non-trivial diff, but don't claim or imply the number
   itself blocks merge; the record shows it doesn't, mechanically.

5. **Operator-precedence and truthiness bugs in metrics/section filtering
   have actually shipped and been fixed here.** PR#533's own title says it
   plainly: "metrics: fix section_filter no-op on scalars (operator
   precedence)." This is this codebase's own precedent (a merged bugfix, not
   a reviewer's comment) that filter/condition expressions over metrics
   sections are a real, recurring source of silent no-ops — worth a close,
   manual read on any PR touching `redisbench_admin/utils/*` filtering logic
   or the `compare`/`metrics` modules' conditionals, since this exact class
   of bug has evaded review here before.

6. **Crash-on-missing/non-numeric metric values is a real, recurring failure
   mode.** PR#534: "metrics: skip None / non-numeric metric values instead of
   crashing." Another self-authored, merged fix rather than a reviewer catch,
   but real precedent that this codebase's metric-processing paths have
   crashed on absent/malformed data before — check that new metric-handling
   code degrades (skip, log, default) rather than raising on a `None` or a
   non-numeric value from a real-world result file.

7. **Mutually-exclusive CLI flag validation has had real false-positive
   bugs.** PR#535: "compare: fix mutually-exclusive false-positive on
   `--baseline-tag` + `--comparison-branch`." Precedent (again, a
   self-authored fix, not a reviewer comment) that argparse-level
   mutual-exclusion / validation logic across this project's many `compare`
   and `run` flags is easy to get subtly wrong — worth tracing through by
   hand on any PR that adds a new flag interacting with existing
   baseline/comparison/architecture flags, rather than trusting that argparse
   or a quick manual test caught every combination.

8. **CodeQL catches uninitialized locals, import cycles, and unclosed
   files — reason about the same classes yourself, don't just wait for it.**
   Real alerts from this repo's own CodeQL workflow: an uninitialized local
   variable used across a branch that didn't define it (PR#549), a
   module-level cyclic import (PR#405, and a second one fixed in PR#549 by
   moving `merge_measurements_into_results` out of `run.common` into
   `utils/results.py`), and a file opened but never closed (PR#405). CodeQL
   only sees what's already in the diff; when reviewing a PR that restructures
   imports across `redisbench_admin/run*` and `redisbench_admin/utils*`
   (a project with a known history of accidental cross-module cycles),
   manually check for a new cycle even if CodeQL hasn't run yet, and check
   any new `open()` has a matching `close()` or context manager.

9. **Version-bump and release PRs have a real, codified admin-merge
   caveat.** `AGENTS.md`, verbatim: "Branch protection on `master` requires
   an approving review. GitHub forbids approving your own PR, so a
   version-bump PR authored by the releaser must either be reviewed by
   someone else or merged with `gh pr merge --admin`." This is written
   institutional doctrine (not mined from a comment thread) — worth
   surfacing only for a PR that is itself a version bump / release PR, as
   context for why a self-authored bump might legitimately show an
   admin-merge rather than a normal approval, not as something to demand
   review process changes over.

10. **Scope discipline here is author-initiated, evidenced by one real
    example, and not yet reviewer-tested.** PR#549's own description
    documents the author abandoning an earlier, more general ~300-line
    `dbconfig.wait_for` YAML mechanism once a much smaller approach was found
    to do the same job. This is a good, real example of the practice — but
    be honest about its provenance: it is the author's own account, written
    before any human reviewed the PR, so it demonstrates a norm this
    codebase's own engineers hold themselves to, not a reviewer-enforced
    standard with real back-and-forth behind it. Recognize it positively when
    a PR does this (a description that says "I tried X, realized Y was
    redundant, simplified to Z" deserves credit, not a demand to re-litigate
    the abandoned design), but don't claim reviewers here have a track record
    of demanding splits or simplifications — the evidence for that isn't
    there.

## What this taxonomy is honestly thin or silent on

Categories a project like memtier_benchmark has real precedent for, but
where this repo's surveyed history has **no equivalent evidenced example** —
say so plainly rather than inventing one:

- **Backward-compatible output/CLI-format changes.** No mined review comment
  in this repo explicitly weighs a breaking vs. non-breaking design choice
  the way memtier's oranagra/yossigo precedent does. PR#541's flag is
  additive and opt-in by the author's own design choice, and PR#527's PR
  comment header change is additive, but neither drew an explicit
  maintainer comment on compatibility trade-offs specifically. If a PR under
  review changes an existing CLI flag's meaning or an existing JSON output
  key, say plainly that this project's own history doesn't give you a
  citable maintainer precedent to lean on here, and reason about the
  tradeoff on its own merits instead of fabricating one.
- **Stray/accidental committed files, dead code call-outs.** Real, well
  evidenced in memtier's history (paulorsousa there flags backup files and
  unused functions repeatedly); no comparable example turned up in this
  repo's surveyed sample. `CONTRIBUTING.md`'s "No dead code, no
  commented-out blocks" is written doctrine, so it's fine to apply, but there
  is no real reviewer quote from this repo to cite alongside it.
- **Buffer sizing / memory-safety nitpicks.** Not applicable — this is a
  pure-Python codebase (145 `.py` files, zero C/C++ at time of writing); do
  not import memtier's C-string/`snprintf` category here.
