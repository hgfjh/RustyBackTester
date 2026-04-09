# backtest_engine

`backtest_engine` is a Rust portfolio project for replaying historical limit order book data, reconstructing a local book, running a simple market-making strategy, simulating order lifecycle and fills, and producing run artifacts and summary statistics. The goal of the project is to explore market microstructure and systems-oriented simulation in a way that is realistic enough to be interesting, while still keeping the code understandable and modular.

## Why I Built This

I built this project to go beyond small Rust exercises and work on something that combines data processing, stateful simulation, and performance-aware design. I was especially interested in the mechanics that sit underneath a basic backtest: sequencing, local book reconstruction, queue-aware fills, latency, inventory accounting, and post-run analysis.

This project is intentionally not a production trading system. It is a portfolio piece that emphasizes clarity, correctness, and realistic exchange-style behavior over premature optimization.

## What It Does

- Replays historical order book messages from a BTCUSDT dataset used to drive the simulation.
- Reconstructs a local book from snapshots and deltas while tracking gaps and other anomalies.
- Computes a compact market state summary, including spread, mid, imbalance, and microprice.
- Runs a simple market-making strategy that quotes around microprice.
- Simulates order placement, cancellation, replacement, queue position, fills, and basic risk limits.
- Tracks position, cash, mark-to-market equity, drawdown, and realized PnL.
- Writes run artifacts and generates a final summary report with PnL, inventory, execution, and data quality metrics.

## Architecture Overview

The codebase is split into a few focused modules.

- `src/main.rs`
  Orchestrates the backtest loop. It loads the dataset, sequences messages, maintains the local book, computes features, invokes the strategy, advances the exchange simulator, writes run outputs, and triggers final reporting.

- `src/strategy.rs`
  Defines the strategy interface and the current `SimpleMarketMaker` implementation. The strategy decides what quotes it wants to have working and reconciles that desired state against the exchange simulator.

- `src/exchange_sim.rs`
  Implements the order state machine and position accounting. This module models placement and cancel latency, queue position, partial and full fills, and basic inventory and leverage checks.

- `src/reporting.rs`
  Builds the final run summary from the generated artifacts. It computes PnL, inventory, execution, and data quality metrics and writes the final `summary.json`.

At a high level, the pipeline is:

1. Read a message from the memory-mapped input file.
2. Sequence it and decide whether to accept, reset, accept with a gap, or drop it.
3. Apply the update to the local book and compute a summary of the current market state.
4. Ask the strategy what quotes it wants.
5. Reconcile that desired quote state with the exchange simulator.
6. Advance the exchange simulator by one book update and emit order events.
7. Mark the position to the current mid and persist outputs.
8. Generate a summary report after the run completes.

## Design Choices

A few technical decisions shape the project.

- Fixed-point arithmetic
  Prices and sizes are stored as scaled integers instead of floats. That keeps accounting and inventory calculations deterministic and avoids the kinds of rounding issues that are easy to introduce in finance-style code.

- Memory-mapped input
  The dataset is read through `memmap2`, which allows the program to stream through a large file without first copying it into a giant owned buffer.

- Explicit sequencing and desync handling
  The code does not assume the input stream is always clean. It tracks snapshots, update numbers, gaps, regressions, and crossed-book conditions, and the exchange simulator can temporarily stand down when its local queue model is no longer trustworthy.

- Modular separation
  Strategy logic, exchange simulation, reporting, and orchestration are separated into different modules. For a portfolio project, that tradeoff felt more valuable than compressing everything into a smaller but harder-to-follow implementation.

- Clarity first, then optimization
  The current implementation is reasonably fast for the size of the dataset, but it is not written as a maximally optimized engine. Some choices, such as CSV output during the run and a straightforward in-memory book representation, favor readability and debuggability.

## How to Run

This project currently runs against a local dataset file in the repository root:

- `2026-02-02_BTCUSDT_ob200.data`

The dataset is not bundled in this repository. If you would like to reproduce the exact run setup, you will need to supply a compatible input file at that path or adapt the hardcoded path in `src/main.rs` to point to your local copy.

The input path is hardcoded in `src/main.rs`, so there is no CLI configuration layer yet. To run the backtest:

```powershell
cargo run --release
```

Run the command from the project root:

```text
backtest_engine/
```

Each execution creates a timestamped directory under `runs/` and writes the output artifacts there.

## Output Artifacts

Each run produces a directory under `runs/<timestamp>/` containing:

- `book_state.csv`
  Per-update book summary values such as best bid, best ask, spread, mid, and top-of-book features.

- `fills.csv`
  Fill and partial-fill events emitted by the exchange simulator.

- `orders.csv`
  Order lifecycle events such as placements, cancellations, and rejections.

- `equity.csv`
  Position and account state over time, including inventory, cash, equity, drawdown, and realized PnL.

- `summary.json`
  A compact summary of run metadata, PnL, inventory behavior, execution metrics, and data quality metrics.

- `log.txt`
  A log of anomalous or dropped raw messages for later inspection when sequencing or reconstruction issues occur.

## Performance

The reference dataset used during development is approximately 596 MB, and the default run writes multiple CSV artifacts plus a JSON summary. This means the runtime includes more than just the simulation loop; it also includes parsing, book maintenance, output serialization, and final report generation.

Using the development benchmark setup, a recent release-mode baseline was:

```text
4.202 s +/- 0.031 s
```

I treat that number as a baseline rather than a claim of extreme optimization. The project is reasonably fast for a portfolio backtester that is doing full input replay and writing run artifacts, but there is still room for deeper optimization if performance becomes the main goal.

## Limitations and Future Improvements

- The current strategy is intentionally simple. It is useful for exercising the engine, but it is not meant to be a sophisticated trading model.
- The input path is currently hardcoded in `src/main.rs`, so the program does not yet support a polished CLI or configuration layer.
- The engine writes detailed outputs during the run, which is helpful for inspection but not ideal for peak benchmark performance.
- There is room to optimize parsing, output writing, and book maintenance if faster end-to-end runtimes become a priority.
- This is a portfolio project, not production trading infrastructure. It is designed to demonstrate systems thinking, simulation design, and Rust implementation skills rather than production deployment readiness.

## Contact

If you have questions, comments, or ideas for improving the project, feel free to reach out at `oio3@cornell.edu`. I would be glad to chat about the implementation, possible extensions, or ways to make the backtester more realistic and robust.
