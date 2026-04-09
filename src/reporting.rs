use std::{fs::File, io::{BufWriter, Write}, path::PathBuf};
use csv;
use crate::{
    ALPHA, 
    AnomalyTracker, 
    CANCEL_LATENCY_MS, 
    FEES_PER_FILL, 
    OrderHistory, 
    PLACE_LATENCY_MS, 
    STARTING_CAPITAL, 
    exchange_sim,
};
use serde_derive::{Deserialize, Serialize};
use sci_rs;
use statrs;

#[derive(Debug, Default, Deserialize, Serialize)]
/// Row shape for reading the equity time series back from disk.
struct EquityRow {
    cts: Option<u64>,
    mid_ticks_2x: i64,
    base_pos_micro: i64,
    cash_quote_micro: i128,
    equity_quote_micro: i128,
    peak_equity_quote_micro: i128,
    drawdown_quote_micro: i128,
    realized_pnl_quote_micro: i128, 
    fees_quote_micro: i128, // negative for fees, postive for rebates
    open_basis_quote_micro: i128,
    n_fills_total: u64,
    desynced: bool,
}

#[derive(Debug, Default, Deserialize, Serialize)]
/// Snapshot of the run configuration that is written into the final report.
struct Config {
    alpha: f64,
    place_latency_ms: u64,
    cancel_latency_ms: u64,
    fees_per_fill_quote_micro: i128,
}

#[derive(Debug, Default, Deserialize, Serialize)]
/// High-level metadata about the run window and its configuration.
struct RunMetadata {
    start_cts_ms: u64,
    end_cts_ms: u64,
    n_rows: usize,
    config: Config
}

#[derive(Debug, Default, Deserialize, Serialize)]
/// Profit-and-loss section of the final summary report.
struct PnL {
    final_equity_quote_micro: i128,
    final_realized_pnl_quote_micro: i128,
    total_fees_paid_quote_micro: i128,
    max_drawdown_quote_micro: i128,
    sharpe_ratio: f64,
    sortino_ratio: f64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
/// Inventory and exposure metrics for the run.
struct Inventory {
    avg_abs_pos_micro: f64,
    max_abs_pos_micro: u64,
    max_base_pos_micro: i64,
    min_base_pos_micro: i64,
    p95_abs_pos_micro: f64,
    pct_time_non_flat: f64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
/// Execution-quality metrics derived from simulated order events.
struct Execution {
    order_level_any_fill_rate: f64,
    quantity_based_fill_ratio: f64,
    cancel_rate: f64,
    realized_spread_proxy: f64,
    effective_spread_proxy: f64,
    adverse_selection_proxy: f64,
    avg_live_lifetime_ms: f64,
    avg_end_to_end_ms: f64,
    avg_time_to_first_fill_ms: f64,

}

#[derive(Debug, Default, Deserialize, Serialize)]
/// Stream-health and synchronization metrics.
struct DataQuality {
    gap_count: u64,
    crossed_book_count: u64,
    pct_desynced_time: f64,
}

#[derive(Debug, Default, Deserialize, Serialize)]
/// Top-level report written to `summary.json`.
struct Summary {
    run_metadata: RunMetadata,
    pnl: PnL,
    inventory: Inventory,
    execution: Execution,
    data_quality: DataQuality, 
}

/// Returns `0.0` instead of propagating `NaN` or panicking on divide-by-zero.
pub fn safe_divide_f64(a: f64, b: f64) -> f64 {
    if b == 0.0 {
        0.0
    } else {
        a / b
    }
}

pub fn generate_report(
    order_history: OrderHistory, 
    ex_sim: exchange_sim::ExchangeSim, 
    anomalies: AnomalyTracker, 
    run_dir: PathBuf
) -> Result<(), Box<dyn std::error::Error>> {
    // Reload the equity time series from disk so the report is generated from
    // the same persisted output a user would inspect later.
    let mut reader = match csv::Reader::from_path(
        run_dir.join("equity.csv")
            .to_str()
            .unwrap()) {
        Ok(rdr) => rdr,
        Err(e) => {
            eprintln!("Error reading file: {e}");
            return Err(Box::new(e));
        }
    };
    let equity_rows: Vec<EquityRow> = reader
        .deserialize::<EquityRow>()
        .map(|r| r.unwrap_or_default())
        .collect();

    // Execution metrics come from simulator counters and aggregated order
    // history captured during the backtest.
    let order_level_any_fill_rate = safe_divide_f64(
        ex_sim.get_total_orders_any_fill() as f64,
        ex_sim.get_total_live_orders() as f64
    );
    let quantity_based_fill_ratio = safe_divide_f64(
        order_history.total_filled_micro as f64,
        order_history.total_posted_micro as f64
    );
    let cancel_rate = safe_divide_f64(
        ex_sim.get_total_cancelled_orders() as f64,
        (ex_sim.get_next_order_id() - 1) as f64
    );
    let avg_live_lifetime_ms = safe_divide_f64(
        order_history.live_lifetime_sum as f64,
        ex_sim.get_total_live_orders() as f64
    );
    let avg_end_to_end_ms = safe_divide_f64(
        order_history.end_to_end_sum as f64,
        (ex_sim.get_next_order_id() - 1) as f64
    );
    let avg_time_to_first_fill_ms = safe_divide_f64(
        order_history.time_to_first_fill_sum as f64,
        ex_sim.get_total_orders_any_fill() as f64
    );
    let realized_spread_proxy = safe_divide_f64(
        order_history.vw_aggregate_realized_spread_2x as f64,
        order_history.aggregate_realized_spread_2x as f64
    );
    let effective_spread_proxy = safe_divide_f64(
        order_history.vw_aggregate_effective_spread_2x as f64,
        order_history.aggregate_effective_spread_2x as f64
    );
    let adverse_selection_proxy = safe_divide_f64(
        order_history.vw_aggregate_adverse_selection_2x as f64,
        order_history.aggregate_adverse_selection_2x as f64
    );
    
    // Final account values are the last observations in the equity series.
    let equity_quote_micro_iter = equity_rows
        .iter()
        .map(|r| r.equity_quote_micro);
    let final_equity_quote_micro = match equity_quote_micro_iter.last() {
        Some(num) => num,
        None => {
            eprintln!("Error parsing last row of equity_quote_micro.");
            return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Error parsing last row of equity_quote_micro.",
                ).into());
        }
    };

    let realized_pnl_quote_micro_iter = equity_rows
        .iter()
        .map(|r| r.realized_pnl_quote_micro);
    let final_realized_pnl_quote_micro = match realized_pnl_quote_micro_iter.last() {
        Some(num) => num,
        None => {
            eprintln!("Error parsing last row of equity_quote_micro.");
            return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Error parsing last row of equity_quote_micro.",
                ).into());
        }
    };

    let total_fees_paid_quote_micro = ex_sim.get_total_fees_paid_quote_micro();

    // Drawdown is stored as a negative distance from the running peak, so the
    // report flips the sign to present a positive magnitude.
    let drawdown_quote_micro_iter = equity_rows
        .iter()
        .map(|r| r.drawdown_quote_micro);
    let max_drawdown_quote_micro = -match drawdown_quote_micro_iter.min() {
        Some(num) => num,
        None => {
            eprintln!("Empty drawdown_quote_micro column!");
            return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Empty drawdown_quote_micro column!",
                ).into());
        }
    };

    // Build 1-second close series: keep the last equity seen in each second bucket.
    let mut second_close_equity: Vec<i128> = Vec::new();
    let mut current_sec: Option<u64> = None;
    let mut last_eq_in_sec: i128 = 0;

    for row in &equity_rows {
        let Some(cts_ms) = row.cts else { continue };
        let sec = cts_ms / 1000;

        match current_sec {
            None => {
                current_sec = Some(sec);
                last_eq_in_sec = row.equity_quote_micro;
            }
            Some(s) if s == sec => {
                last_eq_in_sec = row.equity_quote_micro;
            }
            Some(_) => {
                second_close_equity.push(last_eq_in_sec);
                current_sec = Some(sec);
                last_eq_in_sec = row.equity_quote_micro;
            }
        }
    }

    // Push final bucket close.
    if current_sec.is_some() {
        second_close_equity.push(last_eq_in_sec);
    }

    // Normalization: delta equity over starting capital.
    let percent_returns: Vec<f64> = second_close_equity
        .windows(2)
        .map(|w| (w[1] - w[0]) as f64 / STARTING_CAPITAL as f64)
        .collect();

    if percent_returns.is_empty() {
        eprintln!("Not enough 1-second buckets to compute return stats.");
        return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Not enough 1-second buckets to compute return stats.",
                ).into());
    }

    let (mean_percent_return, _) = sci_rs::stats::mean(percent_returns.iter().copied());
    let (percent_return_stdev, _) = sci_rs::stats::stdev(percent_returns.iter().copied());
    let (downside_percent_return_stdev, _) = sci_rs::stats::stdev(
        percent_returns.iter().copied().map(|r| r.min(0.0))
    );

    let annualization = 31_536_000f64.sqrt();
    let sharpe_ratio = safe_divide_f64(mean_percent_return, percent_return_stdev) * annualization;
    let sortino_ratio = safe_divide_f64(mean_percent_return, downside_percent_return_stdev) * annualization;

    // Compute a time-weighted average absolute inventory by weighting each
    // position by the amount of time it remained in force.
    let avg_abs_pos_numerator: u64 = equity_rows.iter()
        .map(|r| r.cts.unwrap())
        .scan(None::<u64>, |prev_cts, cts| {
            let out = prev_cts.map(|p| cts - p);
            *prev_cts = Some(cts);
            Some(out)
        })
        .flatten()
        .zip(equity_rows.iter().map(|r| r.base_pos_micro).skip(1))
        .map(|(dt, base_pos_micro)| base_pos_micro.unsigned_abs() * dt )
        .sum();
    
    let avg_abs_pos_denominator: u64 = equity_rows.iter()
        .map(|r| r.cts.unwrap())
        .scan(None::<u64>, |prev_cts, cts| {
            let out = prev_cts.map(|p| cts - p);
            *prev_cts = Some(cts);
            Some(out)
        })
        .flatten()
        .sum();
    
    let avg_abs_pos_micro: f64 = safe_divide_f64(
        avg_abs_pos_numerator as f64, 
        avg_abs_pos_denominator as f64
    );

    // Point-in-time inventory extrema are included for a quick risk view.
    let max_abs_pos_micro = match equity_rows.iter()
        .map(|r| r.base_pos_micro.unsigned_abs())
        .max() {
            Some(num) => num,
            None => {
                eprintln!("Empty base_pos_micro column!");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Empty base_pos_micro column!",
                ).into());
            }
        };
    let max_base_pos_micro = match equity_rows.iter()
        .map(|r| r.base_pos_micro)
        .max() {
            Some(num) => num,
            None => {
                eprintln!("Empty base_pos_micro column!");
               return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Empty base_pos_micro column!",
                ).into());
            }
        };
    
    let min_base_pos_micro = match equity_rows.iter()
        .map(|r| r.base_pos_micro)
        .min() {
            Some(num) => num,
            None => {
                eprintln!("Empty base_pos_micro column!");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Empty base_pos_micro column!",
                ).into());
            }
        };
    
    let abs_pos_values: Vec<f64> = equity_rows.iter()
        .map(|r| r.base_pos_micro.unsigned_abs() as f64)
        .collect();
    let p95_abs_pos_micro = statrs::statistics::OrderStatistics::quantile(
        &mut statrs::statistics::Data::new(abs_pos_values), 
        0.95
    );

    // Percentage of runtime spent carrying any non-zero inventory.
    let percent_time_non_flat_numerator: u64 = equity_rows.iter()
        .map(|r| r.cts.unwrap())
        .scan(None::<u64>, |prev_cts, cts| {
            let out = prev_cts.map(|p| cts - p);
            *prev_cts = Some(cts);
            Some(out)
        })
        .flatten()
        .zip(equity_rows.iter().map(|r| r.base_pos_micro).skip(1))
        .filter_map(|(dt, base_pos_micro)| (base_pos_micro != 0).then_some(dt))
        .sum();

    let pct_time_non_flat = safe_divide_f64(
        percent_time_non_flat_numerator as f64, 
        avg_abs_pos_denominator as f64
        ) * 100.0;

    let gap_count = anomalies.num_gap;
    let crossed_book_count = anomalies.num_crossed_book;
    
    // Percentage of runtime during which the simulator considered its queue
    // position model unreliable.
    let percent_desynced_numerator: u64 = equity_rows.iter()
        .map(|r| r.cts.unwrap())
        .scan(None::<u64>, |prev_cts, cts| {
            let out = prev_cts.map(|p| cts - p);
            *prev_cts = Some(cts);
            Some(out)
        })
        .flatten()
        .zip(equity_rows.iter().map(|r| r.desynced).skip(1))
        .filter_map(|(dt, desynced)| desynced.then_some(dt) )
        .sum();

    let pct_desynced_time = safe_divide_f64(
        percent_desynced_numerator as f64, 
        avg_abs_pos_denominator as f64
        ) * 100.0;

    let n_rows = equity_rows.len();

    // The first and last timestamps define the report window.
    let start_cts_ms = match equity_rows.iter()
        .map(|r| r.cts.unwrap())
        .next() {
            Some(num) => num,
            None => {
                eprintln!("Empty cts column!");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Empty cts column!",
                ).into());
            }
        };     
    let end_cts_ms = match equity_rows.iter()
        .map(|r| r.cts.unwrap())
        .last() {
            Some(num) => num,
            None => {
                eprintln!("Empty cts column!");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Empty cts column!",
                ).into());
            }
        };
    
    // Assemble the final report in sections so the generated JSON stays easy
    // to read and inspect.
    let config = Config { 
        alpha: ALPHA, 
        place_latency_ms: PLACE_LATENCY_MS, 
        cancel_latency_ms: CANCEL_LATENCY_MS, 
        fees_per_fill_quote_micro: FEES_PER_FILL, 
    };
    let run_metadata = RunMetadata {
        start_cts_ms, 
        end_cts_ms, 
        n_rows, 
        config 
    };
    let pnl = PnL {
        final_equity_quote_micro, 
        final_realized_pnl_quote_micro, 
        total_fees_paid_quote_micro,
        max_drawdown_quote_micro,
        sharpe_ratio,
        sortino_ratio 
    };
    let inventory = Inventory {
        avg_abs_pos_micro,
        max_abs_pos_micro,
        max_base_pos_micro,
        min_base_pos_micro,
        p95_abs_pos_micro,
        pct_time_non_flat,
    };
    let execution = Execution {
        order_level_any_fill_rate,
        quantity_based_fill_ratio,
        cancel_rate,
        realized_spread_proxy,
        effective_spread_proxy,
        adverse_selection_proxy,
        avg_live_lifetime_ms,
        avg_end_to_end_ms,
        avg_time_to_first_fill_ms,
    };
    let data_quality = DataQuality {
        gap_count,
        crossed_book_count,
        pct_desynced_time,
    };

    let summary = Summary {
        run_metadata,
        pnl,
        inventory,
        execution,
        data_quality
    };

    // Persist the report in a human-readable JSON format.
    let summary_file = File::create(run_dir.join("summary.json").to_str().unwrap())?;
    let mut summary_writer = BufWriter::new(summary_file);
    serde_json::to_writer_pretty(&mut summary_writer, &summary)?;
    summary_writer.flush()?;

    Ok(())
}
