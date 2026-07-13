# Fair-dispatch: sort the pending queue once per process tick

- **Status:** Proposed (design-record-first; no implementation exists yet)
- **Domain:** platform (server-side pipeline queue / scheduler)
- **Tracker:** SEA-1180 (follow-up to SEA-938 fair-dispatch, PR #5)
- **Baseline:** sealed-fork PR #5 head `8e73b4d6280d11fc6d688ea3021687684110eeab`

## Problem / Intent

The fair-dispatch change (PR #5) made the FIFO queue dispatch pending tasks in
creation order by sorting the pending set inside `assignToWorker()`. Because the
per-tick dispatch loop calls `assignToWorker()` once per dispatched task, that
sort runs once **per dispatch** instead of once **per tick**: `N` dispatches in a
tick re-allocate and re-sort the whole pending set `N` times — `O(N · M log M)`
where `M` is the pending depth, when `O(M log M)` per tick suffices.

CodeRabbit flagged this on PR #5 as a `🔵 Trivial` / `⚡ Quick win` nitpick. It is
negligible at realistic queue depth (tens to low-hundreds pending, 100ms tick)
and was deliberately left out of PR #5 to keep the fairness diff minimal and
obviously correct. This record designs the optimization as its own reviewed
change so the approach fork gets a decision before any code lands.

## Evidence (current control flow)

All citations are `server/queue/fifo.go` at baseline `8e73b4d6`, read in-session.

**`process()` — the per-tick dispatch loop calls `assignToWorker()` per dispatch** (lines 257-287):

```go
257: func (q *fifo) process() {
258: 	for {
259: 		select {
260: 		case <-time.After(processTimeInterval):
261: 		case <-q.ctx.Done():
262: 			return
263: 		}
264:
265: 		q.Lock()
266: 		if q.paused {
267: 			q.Unlock()
268: 			continue
269: 		}
270:
271: 		q.resubmitExpiredPipelines()
272: 		q.filterWaiting()
273: 		for pending, worker := q.assignToWorker(); pending != nil && worker != nil; pending, worker = q.assignToWorker() {
274: 			task, _ := pending.Value.(*model.Task)
275: 			task.AgentID = worker.agentID
276: 			delete(q.workers, worker)
277: 			q.pending.Remove(pending)
278: 			q.running[task.ID] = &entry{
279: 				item:     task,
280: 				done:     make(chan bool),
281: 				deadline: time.Now().Add(q.extension),
282: 			}
283: 			worker.channel <- task
284: 		}
285: 		q.Unlock()
286: 	}
287: }
```

**`assignToWorker()` — re-sorts on every call via `pendingByCreation()`** (lines 314-343):

```go
314: func (q *fifo) assignToWorker() (*list.Element, *worker) {
315: 	var bestWorker *worker
316: 	var bestScore int
317:
318: 	for _, element := range q.pendingByCreation() {
319: 		task, _ := element.Value.(*model.Task)
...
324: 		if !q.canRunConcurrent(task) {
...
326: 			continue
327: 		}
328:
329: 		for worker := range q.workers {
330: 			matched, score := worker.filter(task)
331: 			if matched && score > bestScore {
332: 				bestWorker = worker
333: 				bestScore = score
334: 			}
335: 		}
336: 		if bestWorker != nil {
...
338: 			return element, bestWorker
339: 		}
340: 	}
341: 	return nil, nil
342: }
```

**`pendingByCreation()` — fresh slice alloc + full stable sort every call** (lines 356-374):

```go
356: func (q *fifo) pendingByCreation() []*list.Element {
357: 	elements := make([]*list.Element, 0, q.pending.Len())
358: 	for element := q.pending.Front(); element != nil; element = element.Next() {
359: 		elements = append(elements, element)
360: 	}
361: 	slices.SortStableFunc(elements, func(a, b *list.Element) int {
362: 		taskA, _ := a.Value.(*model.Task)
363: 		taskB, _ := b.Value.(*model.Task)
364: 		switch {
365: 		case taskOrderLess(taskA, taskB):
366: 			return -1
367: 		case taskOrderLess(taskB, taskA):
368: 			return 1
369: 		default:
370: 			return 0
371: 		}
372: 	})
373: 	return elements
374: }
```

## Key invariant (why a single pass per tick is safe)

The whole tick runs under one lock — `q.Lock()` at line 265, `q.Unlock()` at 285.
Within that critical section:

- **`canRunConcurrent(task)` (call site line 324) only ever tightens during a
  tick.** The early gate `if running >= task.ConcurrencyLimit` (fifo.go:409-411)
  is monotone: `q.running` only grows (line 278 adds; completion is handled off
  the `done` channel elsewhere, never inside this locked loop). The main verdict
  is `running + ahead < limit` (fifo.go:438), where `ahead` counts earlier
  same-group members from **both** `q.pending` and `q.waitingOnDeps`
  (fifo.go:429-436) — so `ahead` *shrinks* as pending drains, and "running grows"
  alone does not prove the gate tightens. The real guarantee is **conservation**:
  every member counted in `ahead` is by construction same-group (fifo.go:418), so
  when it dispatches it leaves `q.pending` (`ahead--`, `Remove` line 277) and
  enters `q.running` (`running++`, line 278) in lockstep — `running + ahead` is
  conserved. Same-pipeline members are excluded from `ahead` (fifo.go:422-424),
  so their dispatch raises only `running` (strictly tighter). `q.waitingOnDeps`
  is fixed for the loop (`filterWaiting` ran once at line 272, before the loop).
  Hence `running + ahead` never decreases: the gate never loosens.
- **`q.workers` only shrinks** (line 276 `delete`; no worker is added mid-loop).
- **`q.pending` only shrinks** (line 277 `Remove`), and `filterWaiting()` (line
  272) has already run once before the loop, so pending membership is fixed for
  the loop's duration except for the elements the loop itself removes.

**Corollary:** once an element is passed over in a tick — deferred by
`!canRunConcurrent` or lacking a matching worker — it cannot become dispatchable
later in the **same** tick (concurrency only tighter, workers only fewer). So
restarting the scan from the front on each `assignToWorker()` call re-examines
elements that are provably still non-dispatchable. A single forward pass over one
sorted snapshot — skipping removed/deferred/unmatched, dispatching matches
inline — yields the same dispatch set and the same order as today.

**Scoring caveat (must preserve):** `bestWorker`/`bestScore` are declared at
lines 315-316 outside the element loop, but the function returns the instant
`bestWorker != nil` after the first matching task's worker scan (lines 336-339),
so they are **effectively per-task** today — scoring ranks workers *within* one
task, never across tasks. Any restructure that keeps one running pass MUST reset
`bestWorker`/`bestScore` per element to preserve this. (This is the same property
the Greptile P2 on PR #5 raised and `seal-agent` confirmed: dispatch commits to
the first eligible task in order; per-task worker selection is unchanged.)

## Approach

Three approaches that all "sort once per tick" (A/B/C), plus two lower-risk
alternatives surfaced by the critic pass (D sorted-insert, B′ slice+cursor — see
`## Alternatives considered`). The original recommendation is **Approach C**, but
the adversarial pass reframed the decision criterion (see Open Questions): the
final choice is deferred to Matt.

### Approach A — cache + invalidate on the `fifo` struct

Memoize the creation-ordered snapshot on the `fifo` struct; invalidate it on
every `q.pending` mutation (all `PushBack`/`PushFront`/`Remove` sites, including
`filterWaiting` and the resubmit/enqueue paths).

- **Pro:** `assignToWorker()`'s signature is untouched; smallest call-graph change.
- **Con:** largest **correctness** surface. Every current and *future* `q.pending`
  mutation site must remember to invalidate; a single missed site yields a stale
  order — a silent fairness bug, exactly the class this fork is meant to avoid.
  Adds mutable cache state to a struct that today derives order purely.
- **Verdict:** rejected-leaning. Most invalidation surface for the least benefit.

### Approach B — hoist the sort into `process()`

Sort once at the top of the `process()` tick, pass the ordered `[]*list.Element`
into `assignToWorker()` as a parameter; `assignToWorker` walks the passed slice.

- **Pro:** sort runs once per tick; keeps `assignToWorker`'s "find the next
  dispatchable" shape.
- **Con:** the dispatch loop removes elements from `q.pending` mid-iteration
  (line 277). A removed `*list.Element` still sits in the hoisted slice, so
  `assignToWorker` must guard "is this element still in the list" on each call —
  and `container/list` exposes no clean membership test, forcing a hacky sentinel
  (e.g. checking `element.Next()/Prev()/list` internals). Awkward and fragile.
- **Verdict:** viable but the mid-loop-removal guard is ugly.

### Approach C — single-pass restructure of the dispatch loop (recommended)

Sort once per tick, then walk the sorted snapshot in **one forward pass**:
skip removed/deferred/unmatched elements, and when an element matches a worker,
dispatch it inline (assign agent, consume the worker, move the task to
`running`, remove from `pending`). Replaces the
`process()` ⇄ `assignToWorker()` call-per-dispatch protocol with one pass.
Relies on the tick invariant above.

- **Pro:** cleanest result — `O(M log M)` per tick, no cache state, no
  invalidation surface, no membership guard. The removal is a natural part of the
  single pass (we hold the element we're dispatching). Directly reflects the
  invariant: a skipped element is never revisited within the tick.
- **Con:** it restructures the `process()`/`assignToWorker()` control flow (a
  control-flow change → design approval per `AGENTS.md`), and MUST carefully
  preserve the per-task `bestWorker`/`bestScore` reset (see scoring caveat) so
  worker selection stays byte-for-byte identical to PR #5.
- **Verdict:** recommended *for the single-pass shape*, but its "only clean
  option" claim is contested — the adversarial critic pass (see
  `## Alternatives considered`) surfaced a lower-risk alternative (D,
  sorted-insert) that also removes the redundant work with no cache/membership
  surface *and* no control-flow change. C's equivalence is provable from the
  invariant; the open question is whether its larger blast radius is warranted
  (see Open Questions).

## Alternatives considered

Surfaced by the adversarial design-critic pass (SEA-1188); folded here because
they were absent from the original A/B/C set and change the risk calculus.

### D — sorted-insert: keep `q.pending` ordered, drop the sort entirely

`taskOrderLess` keys only on `Created` and `Name` (fifo.go:445-450), both
**immutable** task fields — so a task's sort position never changes over its
lifetime. Insert each task at its ordered position instead of `PushBack` +
sort-on-read (mutation sites: `PushAtOnce` line 80, `filterWaiting` re-push line
293, `resubmitExpiredPipelines` `PushFront` line 457), and `pendingByCreation()`
degenerates to a plain `O(M)` snapshot with **no sort at all**.

- **Pro:** eliminates the sort rather than hoisting it; no cache state, no
  membership guard, and — crucially — **no `process()`/`assignToWorker()`
  control-flow restructure**, so it may not even trip the `AGENTS.md`
  design-approval gate C incurs. Lowest blast radius of any option.
- **Con / load-bearing catch:** `resubmitExpiredPipelines` deliberately
  `PushFront`s an expired task (fifo.go:457) to re-dispatch it ahead of the
  queue. Sorted-insert would relocate it to creation order — a **behavioral
  change** unless resubmit is special-cased. This seam is what makes D a genuine
  fork, not a free win: it must be verified that no path depends on
  `PushFront`/`PushBack` insertion order diverging from `taskOrderLess` order.
  - **Review-swarm check (SEA-1180, unratified — for Matt):** this catch appears
    **inverted**. See Open Questions: at baseline `8e73b4d6` every `q.pending` reader
    already derives order from `taskOrderLess`, so `PushFront`'s physical placement is
    already overridden — D would *match* current dispatch behavior, not diverge from it.
- **Verdict:** lower-risk than C for the same result; contingent on the
  resubmit-ordering check. Surfaced to the Open Question.

### B′ — slice + resume-cursor (membership-free variant of B)

B was found viable-but-ugly (lines 188-193) because a removed `*list.Element` lingers in the
hoisted slice and `container/list` has no clean membership test. But the tick
invariant guarantees the walk only ever moves **forward** — a skipped element is
never revisited in the tick — so `assignToWorker()` needs no membership test:
sort once in `process()`, pass the sorted slice **and a resume cursor**, and have
`assignToWorker` resume from the cursor, returning `(element, worker, nextCursor)`.
This keeps the `process()`/`assignToWorker()` call protocol (smaller diff than
C's full inline dispatch) while still sorting once per tick.

- **Pro:** removes B's stated con (the membership hack was a strawman given the
  invariant); smaller control-flow change than C.
- **Con:** still threads a cursor through the call signature; marginally more
  surface than D.
- **Verdict:** viable lower-risk middle ground; dominated by D on blast radius,
  retained for completeness.

## Global Constraints

- **Language floor:** Go `1.26.0` (`go.mod`). Use `slices.SortStableFunc` (already
  in use at line 361) — the sort MUST stay **stable** so equal-`taskOrderLess`
  elements keep insertion order (the property `pendingByCreation`'s doc-comment
  guarantees, lines 349-350).
- **Behavioral equivalence is the contract:** dispatch set, dispatch order, and
  per-task worker scoring MUST be byte-for-byte identical to PR #5 baseline
  `8e73b4d6`. This change is a pure internal optimization — zero observable
  behavior change. PR #5's tests are the oracle.
- **Depends on PR #5 (fair-dispatch, SEA-938):** this optimization stacks on the
  fair-dispatch change. `TestFifoFairDispatch` and the creation-order dispatch
  behavior land in PR #5, **not** in this PR's base (`sealed-fork` at
  `81d343dbc`). Implementation of SEA-1180 MUST begin only after PR #5 has merged
  to `sealed-fork`; all `fifo.go`/`fifo_test.go` line numbers and the
  `TestFifoFairDispatch` oracle cited below are **as of PR #5's head `8e73b4d6`**,
  the post-merge state the executor will branch from.
- **Locking unchanged:** all work stays within the existing single `q.Lock()`/
  `q.Unlock()` per-tick critical section (lines 265/285). No new goroutines, no
  lock-scope changes.
- **No public API change:** `Queue` interface untouched; `assignToWorker` and
  `pendingByCreation` are unexported — refactoring their shape is internal.
- **Fork CI reality:** sealed-fork PRs run no woodpecker pipeline — only the AI
  review bots + Graphite mergeability. Verification is local `go test` (stated in
  the PR body) plus the bots.
- **Upstream sync:** SEA-938 fair-dispatch is slated to go upstream
  (woodpecker-ci/woodpecker); keep this change rebase-clean against that so the
  optimization can follow the fairness fix upstream.

## Plan

Right-sized tasks, each carrying its own test cycle. Executes only after the
approach Open Question is decided (assume **C** unless Matt says otherwise).

### Task 1 — Lock behavioral equivalence with a red test

Add a test that dispatches **multiple tasks in a single tick** and asserts (a)
dispatch order = creation order, (b) a concurrency-deferred task stays deferred
while later-eligible tasks in the same tick dispatch, and (c) **per-task worker
scoring is reset** — in one tick, dispatch an earlier task to its highest-scoring
worker, then assert a *later* same-tick task is dispatched to a worker whose
score is **strictly lower** than the earlier winner's. This test must pass on the
PR #5 baseline (it encodes current behavior) — it guards the refactor, so it is
written first and stays green throughout.

Assertion (c) is load-bearing and NOT covered by the existing suite: the closest
guard, `TestFifoLabelBasedScoring` (`8e73b4d6:fifo_test.go:1180-1249`), catches a
`bestScore` carried across tasks only by coincidence — both its matching filters
score exactly `20` (fifo_test.go:1195,1203), so a non-reset `bestScore` fails the
`score > bestScore` check (fifo.go:331) and the later task never dispatches. If a
later task's best score were strictly *higher* than the earlier winner's, a
missing reset would still dispatch and the cross-task scoring regression — the #1
hazard of Approach C (see scoring caveat) — would pass green. (c) closes that gap.

- **Interfaces:** consumes `setupTestQueue(t)` (defined at server/queue/fifo_test.go:41);
  produces a new `TestFifo…` subtest colocated with
  `TestFifoFairDispatch` (at fifo_test.go:1315 **once PR #5 has merged** — that
  test and line number do not exist on this PR's `sealed-fork` base, see Global
  Constraints). Uses `model.Task{Created, ...}` and the existing
  worker-registration test helpers.

### Task 2 — Implement the chosen approach (assume C)

Restructure the dispatch loop to sort once per tick and dispatch in a single
forward pass, preserving the per-task `bestWorker`/`bestScore` reset. Remove the
now-redundant per-call sort. Keep `taskOrderLess`/stable-sort semantics.

- **Interfaces:**
  - `process()` (fifo.go:257) — dispatch loop restructured to obtain the sorted
    snapshot once (via `pendingByCreation()` at 356, called once per tick) and
    drive the single pass.
  - `assignToWorker() (*list.Element, *worker)` (fifo.go:314) — folded into the
    single pass or reshaped to consume the pre-sorted snapshot; per-task scoring
    reset preserved.
  - `pendingByCreation() []*list.Element` (fifo.go:356) — unchanged; now called
    once per tick.
  - No change to `canRunConcurrent` (fifo.go:395), `worker.filter`, or
    `taskOrderLess`.

### Task 3 — Regression + perf guard

Run the full queue suite (line numbers as of PR #5's head `8e73b4d6`, the
post-merge state this work branches from); all must stay green unchanged:
`TestFifoFairDispatch` (1315, added by PR #5), `TestFifoConcurrency` (674),
`TestFifoDependencies` (362), `TestFifoBasicOperations` (53),
`TestFifoLeaseManagement` (896), `TestFifoLabelBasedScoring` (1180). Optionally
add a micro-benchmark asserting one sort per tick (guards against regression to
per-call sort).

- **Interfaces:** `go test ./server/queue/...` (local; no fork CI). Optional
  `func BenchmarkFifoDispatchTick(b *testing.B)` in fifo_test.go.

## Tasks

- [ ] Task 1 — red/equivalence test: order + deferred-stays-deferred + per-task scoring reset (strictly-lower later task)
- [ ] Task 2 — implement chosen approach (assume C: single-pass restructure), preserve per-task scoring reset
- [ ] Task 3 — full queue suite stays green; optional per-tick sort benchmark

## Open Questions

- **[LOAD-BEARING] Do this at all — and if so, by which criterion?** The
  adversarial critic pass (SEA-1188) raised that the Problem section admits the
  gain is "negligible at realistic queue depth" (line 18) and CodeRabbit tagged
  it Trivial (line 17) — so the rational decision criterion is **lowest risk**,
  not lowest big-O. Under that criterion the options reorder:
  - **WONTFIX — accept the per-call sort.** Never weighed in the original draft.
    Zero correctness surface, zero design-approval cost; for a non-observable
    gain, doing nothing is the safest option and a legitimate answer.
  - **D (sorted-insert)** or **B′ (slice+cursor)** — remove/hoist the sort with
    materially less blast radius than C (D adds no control-flow change; see
    `## Alternatives considered`), D contingent on its resubmit-ordering check.
  - **C (single-pass restructure)** — the original recommendation: cleanest
    single-pass shape, equivalence provable from the tick invariant, but the
    **largest** blast radius (control-flow restructure → design-approval gate at
    lines 208-209; must preserve the per-task scoring reset, the #1 regression
    hazard). On a lowest-risk criterion, C is the *weakest* pick.
  - **A (cache+invalidate)** — most correctness surface; rejected-leaning.

  I did **not** flip the recommendation unilaterally — that is Matt's
  ratification. My read: if the gain is truly negligible, **WONTFIX or D**
  dominate C. This blocks the merge-freeze: the executor needs the decided shape.
  *mercator relays to Matt.*
- **[LOAD-BEARING — review-swarm finding, SEA-1180, UNRATIFIED] D's "load-bearing
  catch" is inverted; D is lower-risk than the `## Alternatives considered` Con
  states.** The mandated review-swarm (advisory) grounded the D resubmit-ordering
  catch (lines 238-243) against baseline `8e73b4d6` this session. Result: all eight
  `q.pending` readers are order-independent or re-derive order via `taskOrderLess` —
  dispatch walks the sorted `pendingByCreation()` (fifo.go:318); the concurrency-group
  `ahead` scan iterates physically but counts via `taskOrderLess(other, task)`
  (fifo.go:425-429); `depsInQueue`, `updateDepStatusInQueue`,
  `removeFromPendingAndWaiting` match by task ID; `stats` reads `Len()`;
  `filterWaiting` rebuilds a set. `resubmitExpiredPipelines` `PushFront`s (fifo.go:457)
  but does **not** mutate `Created`, so the sort-on-read overrides the physical front
  every tick. **Net: the resubmitted task already dispatches in creation order at
  baseline — D's sorted-insert *matches* current behavior; it is the
  `PushFront`-*preserving* special-case that would diverge.** The check the D verdict
  defers here comes back clean, which *removes* D's one contingency and *strengthens*
  the "D dominates C" read — bearing on the "assume C" Plan default (lines 297/326),
  which assumes the option this OQ ranks weakest. **Not flipping the recommendation —
  Matt ratifies.** Secondary observation for Matt: this means PR #5's sort already
  defeats `resubmitExpiredPipelines`'s intent to re-dispatch an expired task *ahead*
  of the queue — a latent PR #5 behavior question, his call.
- **[NON-LOAD-BEARING] Fold into PR #5, or ship as its own follow-up PR?**
  Recommend a **separate follow-up PR** (keeps PR #5's fairness diff minimal and
  already-reviewed; this is a pure optimization). Deferrable — doesn't change the
  design, only its delivery. Matt may prefer folding it in since it's small.
- **[NON-LOAD-BEARING] Micro-benchmark?** Recommend adding one (Task 3) to guard
  against silent regression to per-call sorting. Optional; the change is correct
  without it.
