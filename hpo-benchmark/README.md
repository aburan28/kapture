# HPO Benchmark Harness

A skeleton for benchmarking hyperparameter-optimization (HPO) algorithms —
Optuna samplers, SMAC, Ax, random search, etc. — on equal footing.

## Design principles

The design follows directly from how these algorithms work (and fail):

1. **The harness owns the loop.** Optimizers are wrapped behind an
   **ask/tell** interface (`Optimizer.ask()` proposes a config,
   `Optimizer.tell()` reports the result). The harness — not the framework —
   runs the evaluation loop, so budget accounting is uniform and an adapter
   cannot quietly evaluate extra configs or stop early. Optuna
   (`study.ask()`/`study.tell()`), SMAC, and Ax all support this mode.

2. **Budget is accounted in three currencies, simultaneously.**
   - `trials`: number of completed evaluations,
   - `cost`: simulated objective cost (e.g. epochs trained, or surrogate
     "runtime") reported by the *problem*,
   - `wall`: real wall-clock spent inside the optimizer (sampler overhead,
     e.g. GP fitting), measured separately.
   Rankings differ by currency — pruning methods shine on `cost`, not on
   `trials` — so every observation is logged with all three cumulative
   counters and analysis can slice by any of them.

3. **Anytime performance is the primary artifact.** The runner logs every
   observation as a JSONL row; analysis builds best-so-far curves
   interpolated onto a common budget grid and aggregates over repeats with
   median + interquartile bands. Final-score-only comparisons are
   misleading (e.g. TPE is pure random search for its first
   `n_startup_trials`).

4. **Problems expose fidelity.** `Problem.evaluate(config, fidelity, seed)`
   takes an optional fidelity (epochs, subset size). This lets
   multi-fidelity methods (Hyperband/ASHA/pruners) participate honestly:
   a pruned trial is charged only the cost it consumed. Tabular/surrogate
   problems (HPOBench, LCBench-style lookup tables) return a *simulated*
   cost so thousands of runs can be replayed cheaply, including simulated
   wall-clock.

5. **Search space is declared once, neutrally.** `space.py` defines a
   framework-agnostic space (log floats, ints, categoricals, conditionals);
   each adapter translates it to its framework's native form. This keeps
   "the problem" identical across optimizers.

6. **Repeats are first-class.** A benchmark run is the cross product
   (optimizer × problem × seed). Both the optimizer seed and the objective
   noise seed are controlled; aim for ≥10 repeats and report quantiles —
   HPO result distributions are skewed.

7. **Random search is always in the lineup** (and ideally at 2× budget) as
   the floor any method must clear.

## Layout

```
harness/
  space.py        # neutral search-space definition
  problem.py      # Problem interface + Observation; synthetic example
  optimizer.py    # ask/tell Optimizer interface, Trial bookkeeping
  adapters/
    random_search.py
    optuna_adapter.py   # wraps any Optuna sampler (+ optional pruner)
  runner.py       # the loop: budget accounting, JSONL recorder
  analysis.py     # anytime curves, normalized regret, rank aggregation
run_example.py    # TPE vs random on a synthetic problem
```

## Running the example

```bash
pip install optuna numpy
python run_example.py   # writes results/*.jsonl and prints a summary
```

## Extending

- New optimizer: subclass `Optimizer`, translate `SearchSpace` in your
  constructor, implement `ask`/`tell`.
- New problem: subclass `Problem`; for surrogate benchmarks return the
  table's recorded cost as `Observation.cost`.
- Parallelism: run `k` concurrent ask/tell workers against one optimizer
  instance and log `k` as a run dimension — sequential and parallel
  variants of the same sampler are different algorithms for ranking
  purposes.
