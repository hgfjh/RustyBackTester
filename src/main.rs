use std::{
    collections::BTreeMap,
    collections::HashMap,
    collections::VecDeque,
    fs,
    fs::File,
    fs::OpenOptions,
    path::PathBuf,
};
use std::io::{self, BufWriter, Write};
use sonic_rs;
use memmap2::Mmap;
use serde_derive::{Deserialize, Serialize};
use csv;
use atoi::atoi;
use derivative::Derivative;
use chrono::Local;

use crate::strategy::Strategy;

mod exchange_sim;
mod reporting;
mod strategy;

pub const MAX_DEPTH: u32 = 200;
pub const PRICE_SCALE: u32 = 10;
pub const SIZE_SCALE: u32 = 1_000_000;
pub const ALPHA: f64 = 0.5;
pub const PLACE_LATENCY_MS: u64 = 200; // data is approximately every 200ms, so this gives a delay to the next tick
pub const CANCEL_LATENCY_MS: u64 = 200; // similar or slightly higher
pub const STARTING_CAPITAL: i128 = 1_000_000_000_000;
pub const FEES_PER_FILL: i128 = 1_000;

/// Creates a buffered CSV writer with headers enabled.
fn open_writer(path: &str) -> io::Result<csv::Writer<BufWriter<File>>> {
    let file = File::create(path)?;
    Ok(csv::WriterBuilder::new()
        .has_headers(true)
        .from_writer(BufWriter::new(file)))
}

/// Parses decimal strings into the fixed-point units used by the simulator.
///
/// Prices are stored in `PRICE_SCALE` ticks and sizes are stored in
/// `SIZE_SCALE` micro-units.
fn fixed_point_parse(decimal_str: &str, data_type: &str) -> i64 {
    let bytes = decimal_str.as_bytes();
    let mut int_str: &[u8] = bytes;
    let mut frac_str: &[u8] = &[];
    let len = bytes.len();
    let mut i = 0;
    for byte in bytes {
        if byte == &b'.' {
            int_str = &bytes[..i];
            frac_str = if i < len {&bytes[i+1..]} else {&[]};
            break;
        }
        i += 1;
    }

    let padding: u32 = 6 - frac_str.len() as u32;
    let int_part = if let Some(x) = atoi::<i64>(int_str) {x} else {0 as i64};
    let frac_part = if let Some(x) = atoi::<i64>(frac_str) {x} else {0 as i64};
    
    if data_type == "p" {
        (int_part * PRICE_SCALE as i64) + frac_part
    } else {
        (int_part * SIZE_SCALE as i64 ) + (frac_part * (10i64.pow(padding)))   
    }
}

/// Computes the top-of-book microprice.
///
/// This is a size-weighted fair value estimate that leans toward the thinner
/// side of the book. If both top levels have zero size, it falls back to the
/// arithmetic mid.
pub fn compute_microprice(
    best_bid_price: i64, 
    best_bid_size: i64, 
    best_ask_price: i64,
    best_ask_size: i64, 
) -> i64 {
    let denominator = best_bid_size + best_ask_size;
    if denominator == 0 {
        (best_bid_price + best_ask_price) / 2
    } else {
        let numerator = (best_ask_price as i128 * best_bid_size as i128)
            + (best_bid_price as i128 * best_ask_size as i128);
        (numerator / denominator as i128) as i64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum Side {
    Bid,
    Ask
}

/// Normalized CSV representation of the order events we want to persist.
#[derive(Debug, Serialize, Deserialize)]
struct OrderCsvRow {
    event_type: &'static str,
    side: Side,
    price_ticks: i64,
    fill_micro: i64,
    cts: u64,
    order_id: u64,
}

impl OrderCsvRow {
    /// Keeps only fill events for `fills.csv`.
    fn fills_from_event(event: &exchange_sim::OrderEvent) -> Option<Self> {
        match *event {
            exchange_sim::OrderEvent::Filled(
                side, 
                price_ticks, 
                fill_micro, 
                cts,
                order_id,
            ) => Some(Self {
                event_type: "fill",
                side,
                price_ticks,
                fill_micro,
                cts,
                order_id,
            }),
            exchange_sim::OrderEvent::PartialFilled(
                side, 
                price_ticks, 
                fill_micro, 
                cts,
                order_id,
            ) => Some(Self {
                event_type: "partial_fill",
                side,
                price_ticks,
                fill_micro,
                cts,
                order_id,
            }),
            _ => None, // ignore non-fill events for fills.csv
        }
    }

    /// Keeps only order lifecycle events for `orders.csv`.
    fn orders_from_event(event: &exchange_sim::OrderEvent) -> Option<Self> {
        match *event {
            exchange_sim::OrderEvent::Placed(
                side, 
                price_ticks, 
                fill_micro, 
                cts,
                order_id,
            ) => Some( Self {
                event_type: "placed_order",
                side,
                price_ticks,
                fill_micro,
                cts,
                order_id,
            }),
            exchange_sim::OrderEvent::Cancelled(
                side, 
                price_ticks, 
                fill_micro, 
                cts,
                order_id,
            ) => Some(Self {
                event_type: "cancelled_order",
                side,
                price_ticks,
                fill_micro,
                cts,
                order_id,
            }),
            exchange_sim::OrderEvent::Rejected(
                side, 
                price_ticks,
                fill_micro, 
                cts,
                order_id,
                reason, 
            ) => Some(Self {
                event_type: reason,
                side,
                price_ticks,
                fill_micro,
                cts,
                order_id,
            }),
            _ => None, // ignore events that are not placing or cancelling an order for orders.csv
        }
    }
}

/// Writes fill-related events and returns the number of rows emitted.
fn write_fill_events<W: Write>(
    wtr: &mut csv::Writer<W>,
    events: &[exchange_sim::OrderEvent],
) -> csv::Result<usize> {
    let mut written = 0usize;
    for event in events {
        if let Some(row) = OrderCsvRow::fills_from_event(event) {
            wtr.serialize(row)?;
            written += 1;
        }
    }
    Ok(written)
}

/// Writes order lifecycle events and returns the number of rows emitted.
fn write_order_events<W: Write>(
    wtr: &mut csv::Writer<W>,
    events: &[exchange_sim::OrderEvent],
) -> csv::Result<usize> {
    let mut written = 0usize;
    for event in events {
        if let Some(row) = OrderCsvRow::orders_from_event(event) {
            wtr.serialize(row)?;
            written += 1;
        }
    }
    Ok(written)
}

/// Raw line format from the order book data file.
#[derive(Deserialize, Serialize, Debug)]
struct RawMsg<'a> {
    #[serde(borrow)]
    topic: &'a str,
    ts: u64,
    #[serde(rename = "type", borrow)]
    msg_type: &'a str,
    #[serde(borrow)]
    data: OrderBookData<'a>,
    cts: Option<u64>,
}

/// Exchange payload carrying bid and ask deltas.
#[derive(Deserialize, Serialize, Debug)]
struct OrderBookData<'a> {
    #[serde(borrow)]
    s: &'a str,
    #[serde(default, borrow)]
    b: Vec<[&'a str; 2]>,
    #[serde(default, borrow)]
    a: Vec<[&'a str; 2]>,
    u: u64,
    seq: u64,
}

/// Memory-mapped reader for the newline-delimited market-data file.
struct Reader {
    mmap: Mmap
}

impl Reader {
    /// Opens and memory-maps the file so the run can stream through it without
    /// copying the entire dataset into owned strings.
    fn mmap_file(file_name: &str) -> io::Result<Mmap> {
        let file = File::open(file_name)?;
        let mmap = unsafe {
            Mmap::map(&file)?
        };
        Ok(mmap)
    }

    fn from_file(file_name: &str) -> Self {
        let mmap = Reader::mmap_file(file_name)
            .expect("Error with memory map of file.");
        Reader { mmap }
    }

    /// Produces one parsed message per line, preserving the original line
    /// number for error reporting.
    fn data_stream(&self) -> impl Iterator<Item = (usize, Result<RawMsg<'_>, sonic_rs::Error>)> + '_ {
        self.mmap[..]
            .split(|b| *b == b'\n')
            .filter(|line| !line.is_empty())
            .enumerate()
            .map(|(i, line)| {
                let parsed: Result<RawMsg<'_>, sonic_rs::Error> = sonic_rs::from_slice(line);
                (i, parsed)
            })
    }
}

/// Sequencer decision for each incoming message.
enum Action<'a> {
    Accept(RawMsg<'a>),
    AcceptGap(RawMsg<'a>, &'static str),
    AcceptReset(RawMsg<'a>),
    Drop(RawMsg<'a>, &'static str),
}

#[derive(Debug, Default)]
/// Tracks enough state to decide whether a message should be accepted,
/// treated as a reset, accepted with a gap warning, or dropped outright.
struct Sequencer {
    have_snapshot: bool,
    last_u: Option<u64>,
    last_seq: Option<u64>,
}

impl Sequencer {
    /// Applies the stream-ordering rules required to maintain a coherent book.
    fn process_item<'a>(&mut self, msg: RawMsg<'a>) -> Action<'a> {
        if ! self.have_snapshot {
            // The book cannot be trusted until the first snapshot arrives.
            if msg.msg_type != "snapshot" {
                Action::Drop(msg, "delta_before_snapshot")
            } else {
                self.have_snapshot = true;
                self.last_seq = Some(msg.data.seq);
                self.last_u = Some(msg.data.u);
                Action::AcceptReset(msg)
            }
        } else if msg.msg_type == "snapshot" {
            // A fresh snapshot resets local state, unless it is clearly stale.
            if msg.data.u !=1 {
                if let Some(last_u) = self.last_u  {
                    if msg.data.u <= last_u {
                        Action::Drop(msg, "stale_snapshot")
                    } else {
                        self.last_seq = Some(msg.data.seq);
                        self.last_u = Some(msg.data.u);
                        Action::AcceptReset(msg)     
                    }     
                } else {
                    Action::Drop(msg, "invariant_broken")  
                }
            } else {
                self.last_seq = Some(msg.data.seq);
                self.last_u = Some(msg.data.u);
                Action::AcceptReset(msg)
            }
        } else {
            // Delta handling is based on the update number `u`.
            if let Some(last_u) = self.last_u {
                if msg.data.u <= last_u {
                    Action::Drop(msg, "regression_or_dup")
                } else if msg.data.u > last_u.checked_add(1).unwrap() {
                    // The update is still applied, but later components treat
                    // the book as temporarily desynchronized.
                    self.last_seq = Some(msg.data.seq);
                    self.last_u = Some(msg.data.u);
                    Action::AcceptGap(msg, "gap")
                } else {
                    self.last_seq = Some(msg.data.seq);
                    self.last_u = Some(msg.data.u);
                    Action::Accept(msg)    
                }
            } else {
                Action::Drop(msg, "invariant_broken")
            }
        }
    }
}

/// Minimal view of the current book used by the strategy and exchange simulator.
#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
struct BookStateSummary {
    cts: Option<u64>,
    ts: u64,
    seq: u64,
    u: u64,
    best_bid_price_ticks: i64,
    best_bid_size_micro: i64,
    best_ask_price_ticks: i64,
    best_ask_size_micro: i64,
    spread_ticks: i64,
    mid_ticks_2x: i64,
    top_k_imbalance: f64,
    top_k_notional_bid: i128,
    top_k_notional_ask: i128,          
}

#[derive(Debug)]
/// Owns the in-memory order book and can emit summaries after each update.
struct BookBuilder {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    max_depth: u32,
    k: u32,
}

impl BookBuilder {
    /// Creates an empty book with the configured storage depth and feature depth.
    fn new (max_depth: u32, k:u32) -> BookBuilder {
        let bids: BTreeMap<i64, i64> = BTreeMap::new();
        let asks: BTreeMap<i64, i64> = BTreeMap::new();
        BookBuilder { bids, asks, max_depth, k }
    }

    /// Applies exchange deltas to the local book and records the before/after
    /// visible size at each touched level.
    fn apply_deltas(
        &mut self,
        bids: &[[&str; 2]],
        asks: &[[&str; 2]],
        level_changes: &mut Vec<LevelChange>,
    ) {
        for [price, size] in bids {
            // Bids are stored with negated keys so the best bid can be read
            // with `first_key_value`, just like asks.
            let price_ticks = -1 * fixed_point_parse(price, "p");
            let size_micro = fixed_point_parse(size, "s");
            let old = if size_micro == 0 {
                self.bids.remove(&price_ticks).unwrap_or(0)
            } else {
                self.bids.insert(price_ticks, size_micro).unwrap_or(0)
            };
            level_changes.push(LevelChange {
                side: Side::Bid,
                price_ticks: -1 * price_ticks,
                old_visible_micro: old,
                new_visible_micro: size_micro,
            });
        }
        for [price, size] in asks {
            let price_ticks = fixed_point_parse(price, "p");
            let size_micro = fixed_point_parse(size, "s");
            let old = if size_micro == 0 {
                self.asks.remove(&price_ticks).unwrap_or(0)
            } else {
                self.asks.insert(price_ticks, size_micro).unwrap_or(0)
            };
            level_changes.push(LevelChange {
                side: Side::Ask,
                price_ticks,
                old_visible_micro: old,
                new_visible_micro: size_micro,
            });
        }
    }

    /// Enforces the configured maximum depth and records when trimming was needed.
    fn trim_depth(&mut self, anomalies: &mut AnomalyTracker) {
        let needs_trim = self.bids.len() > self.max_depth as usize 
            || self.asks.len() > self.max_depth as usize;
        if needs_trim {
            anomalies.update_trim_count();
        }
        while self.bids.len() > self.max_depth as usize {
            self.bids.pop_last();
        }
        while self.asks.len() > self.max_depth as usize {
            self.asks.pop_last();
        }
    }

    /// Derives the strategy-facing summary from the current in-memory book.
    fn compute_summary(&self, msg: &RawMsg<'_>, anomalies: &mut AnomalyTracker, line: u32) -> Option<BookStateSummary> {
        if self.bids.is_empty() || self.asks.is_empty() {
            eprintln!("{line}: Spread/Mid are not defined.");
            AnomalyTracker::update_count(anomalies, "other");
            let _ = anomalies.log(msg);
            return None;
        }
        let best_bid_price_ticks = -1 * self.bids.first_key_value().map(|(k, _)| *k).unwrap_or(0);
        let best_bid_size_micro = self.bids[&(-1 * best_bid_price_ticks)];
        let best_ask_price_ticks = self.asks.first_key_value().map(|(k, _)| *k).unwrap_or(0);
        let best_ask_size_micro = self.asks[&best_ask_price_ticks];
        if best_bid_price_ticks >= best_ask_price_ticks {
            // A crossed book usually signals a sequencing problem or an
            // inconsistent local reconstruction.
            eprintln!("{line}: Potential sequencing gap, missed snapshot, or inconsistent trimming");
            AnomalyTracker::update_count(anomalies, "crossed_book");
            let _ = anomalies.log(msg);
        }
        let spread_ticks = best_ask_price_ticks - best_bid_price_ticks;
        let mid_ticks_2x = best_bid_price_ticks + best_ask_price_ticks;
        let mut k_bids_sum: i64 = 0;
        let mut k_asks_sum: i64 = 0;
        let mut top_k_notional_bid: i128 = 0;
        let mut top_k_notional_ask: i128 = 0;
        for ((bid_price, bid_size), (ask_price, ask_size)) in self.bids.iter()
            .take(self.k as usize)
            .zip(self.asks.iter().take(self.k as usize))
        {
            k_bids_sum += bid_size;
            k_asks_sum += ask_size;
            top_k_notional_bid += ((-1 * bid_price) as i128) * (*bid_size as i128);
            top_k_notional_ask += (*ask_price as i128) * (*ask_size as i128);
        }
        let top_k_imbalance = reporting::safe_divide_f64(
            (k_bids_sum - k_asks_sum) as f64, 
            (k_bids_sum + k_asks_sum) as f64
        );
        Some(Emitter::emit_summary(
            msg.cts,
            msg.ts,
            msg.data.seq,
            msg.data.u,
            best_bid_price_ticks,
            best_bid_size_micro,
            best_ask_price_ticks,
            best_ask_size_micro,
            spread_ticks,
            mid_ticks_2x,
            top_k_imbalance,
            top_k_notional_bid,
            top_k_notional_ask,
        ))
    }

    /// Routes each sequencer action through the appropriate book update path.
    fn action_handler<'a, 'b>(
        &mut self,
        action: &Action<'b>,
        anomalies: &mut AnomalyTracker,
        line: u32,
        level_changes: &'a mut Vec<LevelChange>,
    ) -> (Option<BookStateSummary>, &'a mut Vec<LevelChange>) {
        level_changes.clear();
        let ret = match action {
            Action::Accept(msg) => {
                AnomalyTracker::update_accepted(anomalies);
                self.apply_deltas(&msg.data.b, &msg.data.a, level_changes);
                self.trim_depth(anomalies);
                self.compute_summary(msg, anomalies, line)
            }
            Action::AcceptGap(msg, why) => {
                eprintln!("{line}: {why}");
                anomalies.update_count(why);
                self.apply_deltas(&msg.data.b, &msg.data.a, level_changes);
                self.trim_depth(anomalies);
                self.compute_summary(msg, anomalies, line)
            }
            Action::AcceptReset(msg) => {
                self.bids.clear();
                self.asks.clear();
                self.apply_deltas(&msg.data.b, &msg.data.a, level_changes);
                self.trim_depth(anomalies);
                self.compute_summary(msg, anomalies, line)
            }
            Action::Drop(msg, why) => {
                eprintln!("{line}: {why}");
                let _ = anomalies.log(msg);
                anomalies.update_count(why);
                None
            }
        };
        (ret, level_changes)
    }
}

#[derive(Debug)]
/// Small helper used to centralize summary construction.
struct Emitter {

}

impl Emitter {
    /// Packages a set of computed fields into a `BookStateSummary`.
    fn emit_summary(
        cts: Option<u64>,
        ts: u64,
        seq: u64,
        u: u64,
        best_bid_price_ticks: i64,
        best_bid_size_micro: i64,
        best_ask_price_ticks: i64,
        best_ask_size_micro: i64,
        spread_ticks: i64,
        mid_ticks_2x: i64,
        top_k_imbalance: f64,
        top_k_notional_bid: i128,
        top_k_notional_ask: i128,
    ) -> BookStateSummary {
        BookStateSummary { 
            cts, 
            ts, 
            seq, 
            u, 
            best_bid_price_ticks, 
            best_bid_size_micro, 
            best_ask_price_ticks, 
            best_ask_size_micro, 
            spread_ticks, 
            mid_ticks_2x, 
            top_k_imbalance, 
            top_k_notional_bid,
            top_k_notional_ask,
         }
    }
}

#[derive(Debug, Default)]
/// Collects stream-health counters and writes problematic raw messages to disk
/// for later inspection.
struct AnomalyTracker {
    accepted: u64,
    num_delta_before_snapshot: u64,
    num_seq_regression_or_dup: u64,
    num_crossed_book: u64,
    num_gap: u64,
    trim_count: u64,
    num_other: u64,
    log_writer: Option<BufWriter<File>>,
    run_dir: PathBuf,
}

impl AnomalyTracker {
    /// Creates a tracker scoped to the current run directory.
    fn new(run_dir: PathBuf) -> Self {
        Self { run_dir, ..Default::default() }
    }

    /// Lazily opens the anomaly log and appends the raw offending message.
    fn log(&mut self, msg: &RawMsg<'_>) -> io::Result<()> {
        if self.log_writer.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.run_dir.join("log.txt"))?;
            self.log_writer = Some(BufWriter::new(file));
        }
        if let Some(w) = self.log_writer.as_mut() {
            let encoded = sonic_rs::to_vec(msg)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            w.write_all(&encoded)?;
            w.write_all(b"\n")?;
        }
        Ok(())
    }

    /// Flushes the anomaly log if one was ever opened.
    fn flush_log(&mut self) -> io::Result<()> {
        if let Some(w) = self.log_writer.as_mut() {
            w.flush()?;
        }
        Ok(())
    }

    /// Increments the counter associated with a specific anomaly label.
    fn update_count(&mut self, why: &str) {
        if why == "regression_or_dup" {
            self.num_seq_regression_or_dup += 1;
        } else if why == "delta_before_snapshot" {
            self.num_delta_before_snapshot +=1;
        } else if why == "gap" {
            self.num_gap += 1;
        } else if why == "crossed_book" {
            self.num_crossed_book += 1;
        } else {
            self.num_other += 1;
        }
    }

    /// Counts cleanly accepted messages.
    fn update_accepted(&mut self) {
        self.accepted +=1;
    }

    /// Counts book updates that required depth trimming.
    fn update_trim_count(&mut self) {
        self.trim_count += 1;
    }
}

#[derive(Debug, Serialize, Deserialize)]
/// Before/after view of a price level touched by the latest book update.
struct LevelChange {
    side: Side,
    price_ticks: i64,
    old_visible_micro: i64,
    new_visible_micro: i64,
}

#[derive(Debug, Serialize, Deserialize)]
/// Fill that has occurred already but is still waiting for its forward-looking
/// realized-spread horizon to elapse.
struct PendingFill {
    target_cts: u64,
    fill_cts: u64,
    side: Side,
    fill_price_ticks_2x: i64,
    fill_micro: i64,
    mid_at_fill_2x: i64,
    order_id: u64,
}


#[derive(Derivative, Serialize, Deserialize)]
#[derivative(Debug, Default)]
/// Aggregates order-level timing and execution-quality statistics throughout
/// the run.
struct OrderHistory {
    order_time_data: HashMap<u64, [Option<u64>; 4]>,
    // &order_id -> [placed_cts, live_ack_cts, terminal_cts, first_filled_cts] 
    live_lifetime_sum: u64,
    end_to_end_sum: u64,
    time_to_first_fill_sum: u64,
    total_posted_micro: i64,
    total_filled_micro: i64,
    pending: VecDeque<PendingFill>,
    #[derivative(Default(value = "1000"))]
    horizon_ms: u64,
    aggregate_effective_spread_2x: i64,
    vw_aggregate_effective_spread_2x: i128,
    aggregate_realized_spread_2x: i64,
    vw_aggregate_realized_spread_2x: i128,
    aggregate_adverse_selection_2x: i64,
    vw_aggregate_adverse_selection_2x: i128,

}

impl OrderHistory {
    /// Updates latency and volume statistics from the latest batch of order events.
    fn time_data_from_events(&mut self, events: &Vec<exchange_sim::OrderEvent>) {
        for event in events {
            match *event {
                exchange_sim::OrderEvent::Placed(
                    _side, 
                    _price_ticks, 
                    fill_micro, 
                    cts,
                    order_id, 
                ) => {
                    self.order_time_data.insert(order_id, [Some(cts), None, None, None]);
                    self.total_posted_micro += fill_micro;
                }
                exchange_sim::OrderEvent::LiveAck(
                    _side, 
                    _price_ticks, 
                    _fill_micro, 
                    cts,
                    order_id, 
                ) => {
                    if let Some(times) = self.order_time_data.get_mut(&order_id) {
                        times[1] = Some(cts);
                    }
                }
                exchange_sim::OrderEvent::PartialFilled(
                    _side, 
                    _price_ticks, 
                    fill_micro, 
                    cts,
                    order_id, 
                ) => {
                    if let Some(times) = self.order_time_data.get_mut(&order_id) {
                        if times[3] == None {
                            times[3] = Some(cts);
                            self.time_to_first_fill_sum += times[3].unwrap() - times[0].unwrap(); 
                        }
                    }
                    self.total_filled_micro += fill_micro;
                }
                exchange_sim::OrderEvent::Filled(
                    _side, 
                    _price_ticks, 
                    fill_micro, 
                    cts,
                    order_id, 
                ) => {
                    if let Some(times) = self.order_time_data.get_mut(&order_id) {
                        if times[3] == None {
                            times[3] = Some(cts);
                        }
                        times[2] = Some(cts);
                        self.live_lifetime_sum += times[2].unwrap() - times[1].unwrap();
                        self.end_to_end_sum += times[2].unwrap() - times[0].unwrap();
                        self.order_time_data.remove(&order_id);
                    }
                    self.total_filled_micro += fill_micro;
                }
                exchange_sim::OrderEvent::Cancelled(
                    _side, 
                    _price_ticks, 
                    _fill_micro, 
                    cts,
                    order_id, 
                ) => {
                    if let Some(times) = self.order_time_data.get_mut(&order_id) {
                        times[2] = Some(cts);
                        self.end_to_end_sum += times[2].unwrap() - times[0].unwrap();
                        if let Some(live_ack_cts) = times[1] {
                            self.live_lifetime_sum += times[2].unwrap() - live_ack_cts;
                        }
                        self.order_time_data.remove(&order_id);
                    }
                }
                exchange_sim::OrderEvent::Rejected(
                    _side, 
                    _price_ticks , 
                    _fill_micro,
                    cts,
                    order_id,
                    _reason, 
                ) => {
                    if let Some(times) = self.order_time_data.get_mut(&order_id) {
                        times[2] = Some(cts);
                        self.end_to_end_sum += times[2].unwrap() - times[0].unwrap();
                        self.order_time_data.remove(&order_id);
                    }
                }
            }
        }
    }

    /// Queues fills so realized-spread style metrics can be computed after a
    /// fixed forward horizon.
    fn pending_fills_from_events(
        &mut self, 
        events: &Vec<exchange_sim::OrderEvent>, 
        summary: &BookStateSummary
    ) {
        for event in events {
            match *event {
                exchange_sim::OrderEvent::PartialFilled(
                        side, 
                        price_ticks, 
                        fill_micro, 
                        cts,
                        order_id, 
                    ) => {
                        self.pending
                            .push_front(
                                PendingFill { 
                                    target_cts: cts + self.horizon_ms, 
                                    fill_cts: cts, 
                                    side, 
                                    fill_price_ticks_2x: 2 * price_ticks, 
                                    fill_micro, 
                                    mid_at_fill_2x: summary.mid_ticks_2x,
                                    order_id
                                }
                            );

                    }
                    exchange_sim::OrderEvent::Filled(
                        side, 
                        price_ticks, 
                        fill_micro, 
                        cts,
                        order_id, 
                    ) => {
                        self.pending
                            .push_front(
                                PendingFill { 
                                    target_cts: cts + self.horizon_ms, 
                                    fill_cts: cts, 
                                    side, 
                                    fill_price_ticks_2x: 2 * price_ticks, 
                                    fill_micro, 
                                    mid_at_fill_2x: summary.mid_ticks_2x,
                                    order_id
                                }
                            );    
                    }
                _ => {} // ignore non-fill events
            }
        }
    }
}

/// Runs the full backtest:
/// 1. stream market data,
/// 2. maintain the local book,
/// 3. generate strategy quotes,
/// 4. simulate exchange behavior,
/// 5. write outputs and summary statistics.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reader = Reader::from_file("2026-02-02_BTCUSDT_ob200.data");
    let iter = Reader::data_stream(&reader);
    let run_id = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let run_dir = PathBuf::from("runs").join(run_id);
    fs::create_dir_all(&run_dir)?;
    let mut sequencer = Sequencer::default();
    let mut anomalies = AnomalyTracker::new(run_dir.clone());
    let mut book_builder = BookBuilder::new(MAX_DEPTH, 10);
    let mut ex_sim = exchange_sim::ExchangeSim::default();
    let mut order_history = OrderHistory::default();
    let mut level_changes: Vec<LevelChange> = Vec::with_capacity(1024);
    let mut order_events: Vec<exchange_sim::OrderEvent> = Vec::with_capacity(8);
    let mut position = exchange_sim::Position::default();
    let simple_market_maker = strategy::SimpleMarketMaker::default();
    let mut book_state_writer = open_writer(run_dir.join("book_state.csv").to_str().unwrap())?;
    let mut fill_writer = open_writer(run_dir.join("fills.csv").to_str().unwrap())?;
    let mut order_writer = open_writer(run_dir.join("orders.csv").to_str().unwrap())?;
    let mut position_writer = open_writer(run_dir.join("equity.csv").to_str().unwrap())?;
    let mut rng = rand::rng();

    for (line, msg) in iter {
        // Parse each raw line into the structured message expected by the
        // sequencer and book builder.
        let message = match msg {
            Ok(m) => m,
            Err(e) => {
                eprintln!("Error retrieving message: {e}");
                continue;
            },
        };
        // Decide how this update should affect the local reconstructed book.
        let action = sequencer.process_item(message);
        let (book_state,
            level_change) = book_builder
            .action_handler(
                &action, 
                &mut anomalies, 
                line as u32, 
                &mut level_changes
            );
        if let Some(summary) = book_state {
            // Persist the market snapshot before strategy and simulation logic.
            book_state_writer.serialize(&summary)?;
            let micro_price = compute_microprice(
                summary.best_bid_price_ticks, 
                summary.best_bid_size_micro, 
                summary.best_ask_price_ticks, 
                summary.best_ask_size_micro,
            );
            // Build the feature vector consumed by the strategy.
            let features = strategy::Features {
                spread_ticks: summary.spread_ticks,
                mid_ticks_2x: summary.mid_ticks_2x,
                top_k_imbalance: summary.top_k_imbalance,
                top_k_notional_bid: summary.top_k_notional_bid,
                top_k_notional_ask: summary.top_k_notional_ask,
                micro_price: micro_price,
            }; 
            // Ask the strategy what end-state it wants for this update.
            let desired_quote_state = simple_market_maker
                .desired_quote_state(
                    &summary, 
                    &features, 
                    &position
                );
            // Turn the desired state into place, cancel, or replace requests.
            simple_market_maker.quote_reconciler(
                desired_quote_state, 
                &mut rng, 
                &mut ex_sim
            );
            // Advance the exchange simulator one step using the latest book update.
            let events = exchange_sim::ExchangeSim::on_book_update(
                &mut ex_sim, 
                summary, 
                level_change, 
                &action, 
                &book_builder,
                &mut order_events,
                &mut position,
                &mut rng
            );
            // Revalue the account on the latest mid after processing fills.
            position.mark_to_mid(&summary);
            if position.get_equity_quote_micro() <= 0 {
                println!("Liquidated!");
                break;
            }
            // Update order-level analytics from the events produced this step.
            order_history.time_data_from_events(&events);
            order_history.pending_fills_from_events(&events, &summary);
            while order_history
                .pending
                .front()
                .is_some_and(|pending_fill| pending_fill.target_cts <= summary.cts.unwrap()) {
                    let pending_fill = order_history.pending.pop_front().unwrap();
                    let mid_after_horizon_2x = summary.mid_ticks_2x;
                    match pending_fill.side {
                        Side::Bid => {
                            // For bid fills, a higher future mid is favorable.
                            let side_sign = 1i64; 
                            order_history.aggregate_effective_spread_2x += 
                                side_sign * (pending_fill.mid_at_fill_2x 
                                    - pending_fill.fill_price_ticks_2x);
                            order_history.aggregate_realized_spread_2x +=
                                side_sign * (mid_after_horizon_2x 
                                    - pending_fill.fill_price_ticks_2x);
                            order_history.aggregate_adverse_selection_2x +=
                                side_sign * (mid_after_horizon_2x 
                                    - pending_fill.mid_at_fill_2x);
                            order_history.vw_aggregate_effective_spread_2x += 
                                (side_sign * (pending_fill.mid_at_fill_2x 
                                    - pending_fill.fill_price_ticks_2x)) as i128
                                    * pending_fill.fill_micro as i128;
                            order_history.vw_aggregate_realized_spread_2x +=
                                (side_sign * (mid_after_horizon_2x 
                                    - pending_fill.fill_price_ticks_2x)) as i128
                                    * pending_fill.fill_micro as i128;
                            order_history.vw_aggregate_adverse_selection_2x +=
                                (side_sign * (mid_after_horizon_2x 
                                    - pending_fill.mid_at_fill_2x)) as i128
                                    * pending_fill.fill_micro as i128;
                                    
                        }
                        Side::Ask => {
                            // For ask fills, the sign is inverted so favorable
                            // outcomes still contribute positively.
                            let side_sign = -1i64;
                            order_history.aggregate_effective_spread_2x += 
                                side_sign * (pending_fill.mid_at_fill_2x 
                                    - pending_fill.fill_price_ticks_2x);
                            order_history.aggregate_realized_spread_2x +=
                                side_sign * (mid_after_horizon_2x 
                                    - pending_fill.fill_price_ticks_2x);
                            order_history.aggregate_adverse_selection_2x +=
                                side_sign * (mid_after_horizon_2x 
                                    - pending_fill.mid_at_fill_2x);
                            order_history.vw_aggregate_effective_spread_2x += 
                                (side_sign * (pending_fill.mid_at_fill_2x 
                                    - pending_fill.fill_price_ticks_2x)) as i128
                                    * pending_fill.fill_micro as i128;
                            order_history.vw_aggregate_realized_spread_2x +=
                                (side_sign * (mid_after_horizon_2x 
                                    - pending_fill.fill_price_ticks_2x)) as i128
                                    * pending_fill.fill_micro as i128;
                            order_history.vw_aggregate_adverse_selection_2x +=
                                (side_sign * (mid_after_horizon_2x 
                                    - pending_fill.mid_at_fill_2x)) as i128
                                    * pending_fill.fill_micro as i128;
                        }
                    }
                }
            // Persist event and position outputs for later inspection.
            write_fill_events(&mut fill_writer, &events)?;
            write_order_events(&mut order_writer, &events)?;
            position_writer.serialize(&position)?;
        }
    }
    // Ensure all run artifacts are written before building the summary report.
    book_state_writer.flush()?;
    fill_writer.flush()?;
    position_writer.flush()?;
    anomalies.flush_log()?;
    reporting::generate_report(order_history, ex_sim, anomalies, run_dir)?;

    Ok(())
}
