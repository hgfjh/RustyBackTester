use std::cmp::{min, max};
use rand::{RngExt, rngs::ThreadRng};
use serde_derive::{Deserialize, Serialize};
use derivative::Derivative;
use crate::{
    ALPHA, 
    Action,
    BookBuilder, 
    BookStateSummary,
    CANCEL_LATENCY_MS, 
    FEES_PER_FILL,
    LevelChange, 
    PLACE_LATENCY_MS,
    PRICE_SCALE, 
    Side,
    STARTING_CAPITAL
};

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
/// Order request as seen by the exchange simulator.
pub struct Order {
    pub side: Side,
    pub price_ticks: i64,
    pub size_micro: i64,
    pub order_state: OrderState,
    pub order_id: u64
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
/// Lifecycle states for a single logical order.
pub enum OrderState {
    New,
    LivePending(u64),
    Live,
    Cancelled,
    Filled,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
/// Additional state maintained once an order is assumed to be resting live.
pub struct LiveOrder {
    pub order: Order,
    pub remaining_micro: i64,
    pub queue_ahead_micro: i64,
    pub last_visible_micro: i64,
    pub live_since_cts: u64,
    pub had_any_fill: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
/// Internal state machine for each side of the book.
///
/// The simulator keeps only one working order per side, but that order may be
/// in the middle of a cancel or replace transition.
pub enum OrderType {
    Standard(Order),
    Live(LiveOrder),
    StandardCancel(Order, u64),
    LiveCancel(LiveOrder, u64),
    StandardReplace(Order, Order, u64),
    LiveReplace(LiveOrder, Order, u64),
}

#[derive(Debug, Serialize, Deserialize)]
/// Public order events emitted by the simulator after each book update.
pub enum OrderEvent {
    Placed(Side, i64, i64, u64, u64),
    Cancelled(Side, i64, i64, u64, u64),
    LiveAck(Side, i64, i64, u64, u64),
    Filled(Side, i64, i64, u64, u64),
    PartialFilled(Side, i64, i64, u64, u64),
    Rejected(Side, i64, i64, u64, u64, &'static str)
}

#[derive(Derivative, Serialize, Deserialize)]
#[derivative(Debug, Default)]
/// Tracks account state, realized PnL, and inventory through time.
pub struct Position {
    pub cts: Option<u64>,
    pub mid_ticks_2x: i64,
    pub base_pos_micro: i64,
    #[derivative(Default(value = "STARTING_CAPITAL"))]
    pub cash_quote_micro: i128,
    #[derivative(Default(value = "STARTING_CAPITAL"))]
    pub equity_quote_micro: i128,
    pub peak_equity_quote_micro: i128,
    pub drawdown_quote_micro: i128,
    pub realized_pnl_quote_micro: i128,
    #[derivative(Default(value = "FEES_PER_FILL"))] 
    pub fees_quote_micro: i128, // negative for fees, postive for rebates
    pub open_basis_quote_micro: i128,
    pub n_fills_total: u64,
    pub desynced: bool,
}

impl Position {
    /// Applies the accounting impact of a fill to cash, inventory, cost basis,
    /// and realized PnL.
    fn on_fill(
        &mut self, 
        side: &Side, 
        price_ticks: i64, 
        fill_amount_micro: i64, 
        book: &BookStateSummary, 
        desynced: &bool
    ) {
        let trade_value_quote_micro = (price_ticks as i128 * fill_amount_micro as i128) / PRICE_SCALE as i128;
        self.mid_ticks_2x = book.mid_ticks_2x;
        self.desynced = *desynced;
        match side {
            Side::Bid => {
                // Buying increases base inventory and decreases cash.
                let delta_pos = fill_amount_micro;
                self.cash_quote_micro -= trade_value_quote_micro - self.fees_quote_micro;
                if self.base_pos_micro == 0 {
                    // Opening a fresh long from a flat position.
                    let closed_basis = 0;
                    let closed_value = 0;
                    let open_value = trade_value_quote_micro - closed_value;
                    self.open_basis_quote_micro -= closed_basis - open_value;
                    self.base_pos_micro += delta_pos;
                    if self.base_pos_micro == 0 {
                        self.open_basis_quote_micro = 0;
                    }

                } else if ! (self.base_pos_micro.is_positive() ^ delta_pos.is_positive()) {
                    // Adding to an existing position on the same side does not
                    // realize PnL; it only adjusts open basis.
                    let close_qty: i128 = 0;
                    let closed_basis = self.open_basis_quote_micro * close_qty / self.base_pos_micro.abs() as i128;
                    let closed_value = trade_value_quote_micro * close_qty / fill_amount_micro as i128;
                    let open_value = trade_value_quote_micro - closed_value;
                    self.open_basis_quote_micro -= closed_basis - open_value;
                    self.base_pos_micro += delta_pos;
                    if self.base_pos_micro == 0 {
                        self.open_basis_quote_micro = 0;
                    }     
                } else {
                    // This trade offsets some or all of the existing position,
                    // so part of the fill realizes PnL.
                    let close_qty: i128 = min(self.base_pos_micro.abs() as i128, fill_amount_micro as i128);
                    let closed_basis = self.open_basis_quote_micro * close_qty / self.base_pos_micro.abs() as i128;
                    let closed_value = trade_value_quote_micro * close_qty / fill_amount_micro as i128;
                    let open_value = trade_value_quote_micro - closed_value;
                    self.open_basis_quote_micro -= closed_basis - open_value;
                    if self.base_pos_micro.is_positive() {
                        self.realized_pnl_quote_micro += closed_value - closed_basis;
                    }
                    if self.base_pos_micro.is_negative() {
                        self.realized_pnl_quote_micro += closed_basis - closed_value;
                    }
                    self.base_pos_micro += delta_pos;
                    if self.base_pos_micro == 0 {
                        self.open_basis_quote_micro = 0;
                    }   
                }
                self.n_fills_total += 1;
            }
            Side::Ask => {
                // Selling decreases base inventory and increases cash.
                let delta_pos = -1 * fill_amount_micro;
                self.cash_quote_micro += trade_value_quote_micro + self.fees_quote_micro;
                if self.base_pos_micro == 0 {
                    // Opening a fresh short from a flat position.
                    let closed_basis = 0;
                    let closed_value = 0;
                    let open_value = trade_value_quote_micro - closed_value;
                    self.open_basis_quote_micro -= closed_basis - open_value;
                    self.base_pos_micro += delta_pos;
                    if self.base_pos_micro == 0 {
                        self.open_basis_quote_micro = 0;
                    }

                } else if ! (self.base_pos_micro.is_positive() ^ delta_pos.is_positive()) {
                    // Adding to an existing short extends exposure without
                    // realizing PnL.
                    let close_qty: i128 = 0;
                    let closed_basis = self.open_basis_quote_micro * close_qty / self.base_pos_micro.abs() as i128;
                    let closed_value = trade_value_quote_micro * close_qty / fill_amount_micro as i128;
                    let open_value = trade_value_quote_micro - closed_value;
                    self.open_basis_quote_micro -= closed_basis - open_value;
                    self.base_pos_micro += delta_pos;
                    if self.base_pos_micro == 0 {
                        self.open_basis_quote_micro = 0;
                    }     
                } else {
                    // Selling against a long position closes inventory and
                    // realizes PnL on the closed portion.
                    let close_qty: i128 = min(self.base_pos_micro.abs() as i128, fill_amount_micro as i128);
                    let closed_basis = self.open_basis_quote_micro * close_qty / self.base_pos_micro.abs() as i128;
                    let closed_value = trade_value_quote_micro * close_qty / fill_amount_micro as i128;
                    let open_value = trade_value_quote_micro - closed_value;
                    self.open_basis_quote_micro -= closed_basis - open_value;
                    if self.base_pos_micro.is_positive() {
                        self.realized_pnl_quote_micro += closed_value - closed_basis;
                    }
                    if self.base_pos_micro.is_negative() {
                        self.realized_pnl_quote_micro += closed_basis - closed_value;
                    }
                    self.base_pos_micro += delta_pos;
                    if self.base_pos_micro == 0 {
                        self.open_basis_quote_micro = 0;
                    }   
                }
                self.n_fills_total += 1;
            } 
        }
    }

    /// Marks the account to the current mid and updates drawdown statistics.
    pub fn mark_to_mid(&mut self, book: &BookStateSummary) {
        self.cts = book.cts;
        self.mid_ticks_2x = book.mid_ticks_2x;
        let inventory_value_quote_micro = self.base_pos_micro as i128 * self.mid_ticks_2x as i128 
            / (2 * PRICE_SCALE) as i128;
        self.equity_quote_micro = self.cash_quote_micro + inventory_value_quote_micro;
        self.peak_equity_quote_micro = max(self.peak_equity_quote_micro, self.equity_quote_micro);
        self.drawdown_quote_micro = self.equity_quote_micro - self.peak_equity_quote_micro;
    }

    /// Returns the current marked-to-market equity.
    pub fn get_equity_quote_micro(&self) -> i128 {
        self.equity_quote_micro
    }

    /// Returns whether the simulator currently distrusts its queue-position model.
    pub fn get_desynced(&self) -> bool {
        self.desynced
    }

    /// Allows the fee model to be updated externally.
    pub fn update_fee(&mut self, fees_quote_micro: i128) {
        self.fees_quote_micro = fees_quote_micro;
    }
}

#[derive(Derivative, Serialize, Deserialize, Clone, Copy)]
#[derivative(Debug, Default)]
/// Exchange simulator that manages one working bid and one working ask,
/// including latency, queue modeling, and simple risk checks.
pub struct ExchangeSim {
    working_bid: Option<OrderType>,
    working_ask: Option<OrderType>,
    #[derivative(Default(value = "1"))]
    next_order_id: u64,
    desynced: bool,
    clean_updates_after_desync: u64,
    #[derivative(Default(value = "10_000_000"))]
    max_abs_inventory_micro: u64,
    #[derivative(Default(value = "5.0"))]
    max_leverage: f64,
    total_live_orders: u64,
    total_orders_any_fill: u64,
    total_cancelled_orders: u64,
    total_rejected_orders: u64,
    total_fees_paid_quote_micro: i128,
}

impl ExchangeSim {
    /// Advances the exchange simulator by one market-data update and emits the
    /// order events produced during that step.
    pub fn on_book_update<'a>(
        &mut self, 
        summary: BookStateSummary, 
        level_change: &mut Vec<LevelChange>, 
        action: &Action,
        book: &BookBuilder,
        order_events: &'a mut Vec<OrderEvent>,
        position: &mut Position,
        rng: &mut ThreadRng
    ) -> &'a Vec<OrderEvent> {
        order_events.clear();
        // Process the bid side first, then the ask side below.
        if let Some(order_type) = self.working_bid.take() {
            match order_type {
                OrderType::Standard(mut bid_order) => {
                    match bid_order.order_state {
                        OrderState::New => {
                            // New orders first incur placement latency before
                            // they can become visible in the simulated book.
                            let latency = PLACE_LATENCY_MS + rng.random_range(100..=200u64);
                            let current_cts = summary.cts.unwrap();
                            let activate_at_cts = current_cts + latency;
                            let new_open_basis_micro = bid_order.size_micro + position.base_pos_micro;
                            let new_leverage = (new_open_basis_micro.unsigned_abs() * summary.mid_ticks_2x as u64) as f64 
                                / (2 * PRICE_SCALE as i128 * position.equity_quote_micro) as f64;
                            if bid_order.price_ticks >= summary.best_ask_price_ticks {
                                // Crossing orders are rejected rather than
                                // modeled as immediate marketable executions.
                                self.total_cancelled_orders += 1;
                                self.total_rejected_orders += 1;
                                self.working_bid = None;
                                order_events.push(
                                    OrderEvent::Rejected(
                                        Side::Bid, 
                                        bid_order.price_ticks, 
                                        bid_order.size_micro, 
                                        current_cts,
                                        bid_order.order_id,
                                        "Proposed bid >= best ask."
                                    )
                                );
                            } else if new_open_basis_micro.unsigned_abs() > self.max_abs_inventory_micro 
                                || new_leverage > self.max_leverage 
                                || position.equity_quote_micro <= 0 {
                                // Basic risk checks prevent inventory or
                                // leverage from moving beyond configured limits.
                                self.total_cancelled_orders += 1;
                                self.total_rejected_orders += 1;
                                self.working_bid = None;
                                order_events.push(
                                    OrderEvent::Rejected(
                                        Side::Bid, 
                                        bid_order.price_ticks, 
                                        bid_order.size_micro, 
                                        current_cts,
                                        bid_order.order_id,
                                        "Exchange risk limits violated."
                                    )
                                );                                
                            } else {    
                                // The order is accepted and will become live
                                // once placement latency has elapsed.
                                bid_order.order_state = OrderState::LivePending(activate_at_cts);
                                order_events.push(
                                    OrderEvent::Placed(
                                        Side::Bid, 
                                        bid_order.price_ticks, 
                                        bid_order.size_micro, 
                                        current_cts,
                                        bid_order.order_id,
                                    )
                                );
                                self.working_bid = Some(OrderType::Standard(bid_order));
                            }
                        }
                        OrderState::LivePending(activate_at_cts) => {
                            let current_cts = summary.cts.unwrap();
                            if current_cts >= activate_at_cts {
                                // When the order goes live, queue position is
                                // initialized from the visible size at that
                                // price level.
                                let visible = book.bids
                                    .get(&(-1 * bid_order.price_ticks))
                                    .unwrap_or(&0);
                                bid_order.order_state = OrderState::Live; 
                                self.working_bid = Some(
                                    OrderType::Live(
                                        LiveOrder { 
                                            order: bid_order,
                                            remaining_micro: bid_order.size_micro, 
                                            queue_ahead_micro: *visible, 
                                            last_visible_micro: *visible, 
                                            live_since_cts: current_cts,
                                            had_any_fill: false,
                                        }
                                    )
                                );
                                self.total_live_orders += 1;
                                order_events.push(
                                   OrderEvent::LiveAck(
                                    Side::Bid, 
                                    bid_order.price_ticks, 
                                    bid_order.size_micro, 
                                    current_cts,
                                    bid_order.order_id,
                                    ) 
                                );
                            } else {
                                self.working_bid = Some(OrderType::Standard(bid_order));
                            }
                        }
                        _ => {
                            eprintln!("Error in order management system!");
                        }
                    }     
                }
                OrderType::Live(mut live_bid_order) => {
                    match live_bid_order.order.order_state {
                        OrderState::Live => {
                            if matches!(action, Action::AcceptGap(_, _)) || matches!(action, Action::AcceptReset(_)) {
                                // After a gap or reset, queue estimates are no
                                // longer trustworthy. The order remains live,
                                // but the simulator flags the state as desynced.
                                self.desynced = true;
                                let visible = book.bids
                                    .get(&(-1 * live_bid_order.order.price_ticks))
                                    .unwrap_or(&0);
                                let live_since_cts = live_bid_order.live_since_cts; 
                                self.working_bid = Some(
                                    OrderType::Live(
                                        LiveOrder { 
                                            order: live_bid_order.order,
                                            remaining_micro: live_bid_order.remaining_micro, 
                                            queue_ahead_micro: *visible, 
                                            last_visible_micro: *visible, 
                                            live_since_cts: live_since_cts,
                                            had_any_fill: live_bid_order.had_any_fill,
                                        }
                                    )
                                );
                            } else {
                                if self.desynced == true && matches!(action, Action::Accept(_)) {
                                    // A long enough run of clean updates clears
                                    // the desync flag.
                                    if self.clean_updates_after_desync == 100 {
                                        self.desynced = false;
                                        self.clean_updates_after_desync = 0;
                                    }
                                    self.clean_updates_after_desync += 1;
                                }
                                let book_crossed = summary.best_bid_price_ticks >= summary.best_ask_price_ticks;
                                if ! book_crossed {
                                    let level_change = level_change.iter()
                                        .find(|&level_change| 
                                            level_change.price_ticks == live_bid_order.order.price_ticks 
                                            && level_change.side == Side::Bid
                                        );
                                    if let Some(level_change) = level_change {
                                        if level_change.new_visible_micro >= level_change.old_visible_micro {
                                            // More visible size is treated as
                                            // queue replenishment ahead of or
                                            // alongside our order.
                                            live_bid_order.last_visible_micro = level_change.new_visible_micro;
                                        } else {
                                            // A reduction in visible size is
                                            // split between queue that was ahead
                                            // of us and simulated trading
                                            // activity that may fill us.
                                            let consumed = level_change.old_visible_micro - level_change.new_visible_micro;
                                            let ahead_reduction = min(consumed, live_bid_order.queue_ahead_micro);
                                            live_bid_order.queue_ahead_micro -= ahead_reduction;
                                            let trade_consumed = (ALPHA * consumed as f64) as i64;
                                            let trade_used_for_ahead = min(trade_consumed, ahead_reduction);
                                            let leftover = trade_consumed - trade_used_for_ahead;
                                            let fill = min(leftover, live_bid_order.remaining_micro);
                                            live_bid_order.remaining_micro -= fill;
                                            if live_bid_order.remaining_micro == 0 {
                                                // The order fully filled on
                                                // this update.
                                                if ! live_bid_order.had_any_fill {
                                                    self.total_orders_any_fill += 1;
                                                    live_bid_order.had_any_fill = true;
                                                }
                                                self.total_fees_paid_quote_micro -= position.fees_quote_micro;
                                                order_events.push(
                                                    OrderEvent::Filled(
                                                        Side::Bid, 
                                                        live_bid_order.order.price_ticks, 
                                                        fill, 
                                                        summary.cts.unwrap(),
                                                        live_bid_order.order.order_id,
                                                    )
                                                );
                                                position.on_fill(
                                                    &Side::Bid, 
                                                    live_bid_order.order.price_ticks, 
                                                    fill, 
                                                    &summary, 
                                                    &self.desynced
                                                );
                                                live_bid_order.order.order_state = OrderState::Filled;
                                            } else if fill > 0 {
                                                // Only part of the order
                                                // filled, so the remainder
                                                // stays live.
                                                if ! live_bid_order.had_any_fill {
                                                    self.total_orders_any_fill += 1;
                                                    live_bid_order.had_any_fill = true;
                                                }
                                                self.total_fees_paid_quote_micro -= position.fees_quote_micro;
                                                order_events.push(
                                                    OrderEvent::PartialFilled(
                                                        Side::Bid, 
                                                        live_bid_order.order.price_ticks, 
                                                        fill, 
                                                        summary.cts.unwrap(),
                                                        live_bid_order.order.order_id,
                                                    )        
                                                );
                                                position.on_fill(
                                                    &Side::Bid, 
                                                    live_bid_order.order.price_ticks, 
                                                    fill, 
                                                    &summary, 
                                                    &self.desynced
                                                );
                                            }
                                        }
                                    }
                                }
                                // Keep the order live if quantity remains.
                                if live_bid_order.order.order_state != OrderState::Filled {
                                    self.working_bid = Some(OrderType::Live(live_bid_order));
                                }
                            }
                        }
                        OrderState::Filled => {
                            self.working_bid = None;
                        }
                        _ => {
                            eprintln!("Error in order management system!");
                        }
                    }    
                }
                OrderType::StandardCancel(mut bid_order, cancel_effective_cts) => {
                    match bid_order.order_state {
                        OrderState::LivePending(activate_at_cts) => {
                            // A pending order can either be cancelled before it
                            // goes live or become live first and continue as a
                            // live-cancel state.
                            let current_cts = summary.cts.unwrap();
                            if current_cts >= cancel_effective_cts {
                                self.total_cancelled_orders += 1;
                                bid_order.order_state = OrderState::Cancelled;
                                self.working_bid = Some(OrderType::StandardCancel(bid_order, cancel_effective_cts));
                            } else if current_cts >= activate_at_cts {
                                let visible = book.bids
                                    .get(&(-1 * bid_order.price_ticks))
                                    .unwrap_or(&0);
                                bid_order.order_state = OrderState::Live; 
                                self.working_bid = Some(
                                    OrderType::LiveCancel(
                                        LiveOrder { 
                                            order: bid_order,
                                            remaining_micro: bid_order.size_micro, 
                                            queue_ahead_micro: *visible, 
                                            last_visible_micro: *visible, 
                                            live_since_cts: current_cts,
                                            had_any_fill: false,
                                        },
                                        cancel_effective_cts
                                    )
                                );
                                self.total_live_orders += 1;
                                order_events.push(
                                   OrderEvent::LiveAck(
                                    Side::Bid, 
                                    bid_order.price_ticks, 
                                    bid_order.size_micro, 
                                    current_cts,
                                    bid_order.order_id,
                                    ) 
                                );
                            } else {
                                self.working_bid = Some(OrderType::StandardCancel(bid_order, cancel_effective_cts))
                            }
                        }
                        OrderState::Cancelled => {
                            self.working_bid = None;
                            order_events.push(
                                    OrderEvent::Cancelled(
                                        Side::Bid, 
                                        bid_order.price_ticks, 
                                        bid_order.size_micro, 
                                        summary.cts.unwrap(),
                                        bid_order.order_id,
                                    )
                            );
                        }
                        _ => {
                            eprintln!("Error in order management system!");
                        }
                    }
                }
                OrderType::LiveCancel(mut live_bid_order, cancel_effective_cts) => {
                    match live_bid_order.order.order_state {
                        OrderState::Live => {
                            // A live order can still trade while its cancel is
                            // in flight, so fills continue until the cancel
                            // becomes effective.
                            let current_cts = summary.cts.unwrap();
                            if current_cts >= cancel_effective_cts {
                                self.total_cancelled_orders += 1;
                                live_bid_order.order.order_state = OrderState::Cancelled;
                                self.working_bid = Some(OrderType::LiveCancel(live_bid_order, cancel_effective_cts));    
                            } else {
                                if matches!(action, Action::AcceptGap(_, _)) || matches!(action, Action::AcceptReset(_)) {
                                    self.desynced = true;
                                    let visible = book.bids
                                        .get(&(-1 * live_bid_order.order.price_ticks))
                                        .unwrap_or(&0);
                                    let live_since_cts = live_bid_order.live_since_cts; 
                                    self.working_bid = Some(
                                        OrderType::LiveCancel(
                                            LiveOrder { 
                                                order: live_bid_order.order,
                                                remaining_micro: live_bid_order.remaining_micro, 
                                                queue_ahead_micro: *visible, 
                                                last_visible_micro: *visible, 
                                                live_since_cts: live_since_cts,
                                                had_any_fill: live_bid_order.had_any_fill,
                                            },
                                            cancel_effective_cts
                                        )
                                    );
                                } else {
                                    if self.desynced == true && matches!(action, Action::Accept(_)) {
                                        if self.clean_updates_after_desync == 100 {
                                            self.desynced = false;
                                            self.clean_updates_after_desync = 0;
                                        }
                                        self.clean_updates_after_desync += 1;
                                    }
                                    let book_crossed = summary.best_bid_price_ticks >= summary.best_ask_price_ticks;
                                    if ! book_crossed {
                                        let level_change = level_change.iter()
                                            .find(|&level_change| 
                                                level_change.price_ticks == live_bid_order.order.price_ticks 
                                                && level_change.side == Side::Bid
                                            );
                                        if let Some(level_change) = level_change {
                                            if level_change.new_visible_micro >= level_change.old_visible_micro {
                                                // Visible size increased, so
                                                // we only refresh our view of
                                                // the book at that level.
                                                live_bid_order.last_visible_micro = level_change.new_visible_micro;
                                            } else {
                                                // Visible size decreased, so
                                                // some of that change may have
                                                // traded into our resting order.
                                                let consumed = level_change.old_visible_micro - level_change.new_visible_micro;
                                                let ahead_reduction = min(consumed, live_bid_order.queue_ahead_micro);
                                                live_bid_order.queue_ahead_micro -= ahead_reduction;
                                                let trade_consumed = (ALPHA * consumed as f64) as i64;
                                                let trade_used_for_ahead = min(trade_consumed, ahead_reduction);
                                                let leftover = trade_consumed - trade_used_for_ahead;
                                                let fill = min(leftover, live_bid_order.remaining_micro);
                                                live_bid_order.remaining_micro -= fill;
                                                if live_bid_order.remaining_micro == 0 {
                                                    // The order fully filled
                                                    // before the cancel landed.
                                                    if ! live_bid_order.had_any_fill {
                                                        self.total_orders_any_fill += 1;
                                                        live_bid_order.had_any_fill = true;
                                                    }
                                                    self.total_fees_paid_quote_micro -= position.fees_quote_micro;
                                                    order_events.push(
                                                        OrderEvent::Filled(
                                                            Side::Bid, 
                                                            live_bid_order.order.price_ticks, 
                                                            fill, 
                                                            current_cts,
                                                            live_bid_order.order.order_id,
                                                        )
                                                    );
                                                    position.on_fill(
                                                        &Side::Bid, 
                                                        live_bid_order.order.price_ticks, 
                                                        fill, 
                                                        &summary, 
                                                        &self.desynced
                                                    );
                                                    live_bid_order.order.order_state = OrderState::Filled;
                                                } else if fill > 0 {
                                                    // Only part of the order
                                                    // filled; the cancel is
                                                    // still pending.
                                                    if ! live_bid_order.had_any_fill {
                                                        self.total_orders_any_fill += 1;
                                                        live_bid_order.had_any_fill = true;
                                                    }
                                                    self.total_fees_paid_quote_micro -= position.fees_quote_micro;
                                                    order_events.push(
                                                        OrderEvent::PartialFilled(
                                                            Side::Bid, 
                                                            live_bid_order.order.price_ticks, 
                                                            fill, 
                                                            current_cts,
                                                            live_bid_order.order.order_id,
                                                        )        
                                                    );
                                                    position.on_fill(
                                                        &Side::Bid, 
                                                        live_bid_order.order.price_ticks, 
                                                        fill, 
                                                        &summary, 
                                                        &self.desynced
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    // Keep waiting for the cancel if quantity
                                    // remains on the order.
                                    if live_bid_order.order.order_state != OrderState::Filled {
                                        self.working_bid = Some(OrderType::LiveCancel(live_bid_order, cancel_effective_cts));
                                    }
                                }
                            } 
                        }
                        OrderState::Filled => {
                            self.working_bid = None;
                        }
                        OrderState::Cancelled => {
                            self.working_bid = None;
                            order_events.push(
                                    OrderEvent::Cancelled(
                                        Side::Bid, 
                                        live_bid_order.order.price_ticks, 
                                        live_bid_order.order.size_micro, 
                                        summary.cts.unwrap(),
                                        live_bid_order.order.order_id,
                                    )
                            );    
                        }
                        _ => {
                            eprintln!("Error in order management system!");
                        }
                    }
                }
                OrderType::StandardReplace(
                    mut old_bid_order, 
                    new_bid_order,  
                    cancel_effective_cts
                ) => {
                    // Replace is modeled as cancel-old-then-place-new once the
                    // old order has fully left the book.
                    match old_bid_order.order_state {
                        OrderState::New => {
                            self.working_bid = Some(OrderType::Standard(new_bid_order));
                            order_events.push(
                                    OrderEvent::Cancelled(
                                        Side::Bid, 
                                        old_bid_order.price_ticks, 
                                        old_bid_order.size_micro, 
                                        summary.cts.unwrap(),
                                        old_bid_order.order_id,
                                    )
                            );    
                        }
                        OrderState::LivePending(activate_at_cts) => {
                            let current_cts = summary.cts.unwrap();
                            if current_cts >= cancel_effective_cts {
                                self.total_cancelled_orders += 1;
                                old_bid_order.order_state = OrderState::Cancelled;
                                self.working_bid = Some(OrderType::StandardReplace(
                                    old_bid_order, 
                                    new_bid_order, 
                                    cancel_effective_cts,
                                ));
                            } else if current_cts >= activate_at_cts {
                                let visible = book.bids
                                    .get(&(-1 * old_bid_order.price_ticks))
                                    .unwrap_or(&0);
                                old_bid_order.order_state = OrderState::Live; 
                                self.working_bid = Some(
                                    OrderType::LiveReplace(
                                        LiveOrder { 
                                            order: old_bid_order,
                                            remaining_micro: old_bid_order.size_micro, 
                                            queue_ahead_micro: *visible, 
                                            last_visible_micro: *visible, 
                                            live_since_cts: current_cts,
                                            had_any_fill: false,
                                        },
                                        new_bid_order,
                                        cancel_effective_cts,
                                    )
                                );
                                self.total_live_orders += 1;
                                order_events.push(
                                   OrderEvent::LiveAck(
                                    Side::Bid, 
                                    old_bid_order.price_ticks, 
                                    old_bid_order.size_micro, 
                                    current_cts,
                                    old_bid_order.order_id,
                                    ) 
                                );
                            } else {
                                self.working_bid = Some(OrderType::StandardReplace(
                                    old_bid_order, 
                                    new_bid_order, 
                                    cancel_effective_cts
                                ))
                            }
                        }
                        OrderState::Cancelled => {
                            self.working_bid = Some(OrderType::Standard(new_bid_order));
                            order_events.push(
                                    OrderEvent::Cancelled(
                                        Side::Bid, 
                                        old_bid_order.price_ticks, 
                                        old_bid_order.size_micro, 
                                        summary.cts.unwrap(),
                                        old_bid_order.order_id,
                                    )
                            );
                        }
                        _ => {
                            eprintln!("Error in order management system!");
                        }    
                    }
                }
                OrderType::LiveReplace(
                    mut old_live_bid_order, 
                    new_bid_order,  
                    cancel_effective_cts
                ) => {
                    // A live replace behaves like a live cancel with the next
                    // desired order already queued behind it.
                    match old_live_bid_order.order.order_state {
                        OrderState::Live => {
                            let current_cts = summary.cts.unwrap();
                            if current_cts >= cancel_effective_cts {
                                self.total_cancelled_orders += 1;
                                old_live_bid_order.order.order_state = OrderState::Cancelled;
                                self.working_bid = Some(OrderType::LiveReplace(
                                    old_live_bid_order, 
                                    new_bid_order,  
                                    cancel_effective_cts,
                                ));    
                            } else {
                                if matches!(action, Action::AcceptGap(_, _)) || matches!(action, Action::AcceptReset(_)) {
                                    self.desynced = true;
                                    let visible = book.bids
                                        .get(&(-1 * old_live_bid_order.order.price_ticks))
                                        .unwrap_or(&0);
                                    let live_since_cts = old_live_bid_order.live_since_cts; 
                                    self.working_bid = Some(
                                        OrderType::LiveReplace(
                                            LiveOrder { 
                                                order: old_live_bid_order.order,
                                                remaining_micro: old_live_bid_order.remaining_micro, 
                                                queue_ahead_micro: *visible, 
                                                last_visible_micro: *visible, 
                                                live_since_cts: live_since_cts,
                                                had_any_fill: old_live_bid_order.had_any_fill,
                                            },
                                            new_bid_order,
                                            cancel_effective_cts,
                                        )
                                    );
                                } else {
                                    if self.desynced == true && matches!(action, Action::Accept(_)) {
                                        if self.clean_updates_after_desync == 100 {
                                            self.desynced = false;
                                            self.clean_updates_after_desync = 0;
                                        }
                                        self.clean_updates_after_desync += 1;
                                    }
                                    let book_crossed = summary.best_bid_price_ticks >= summary.best_ask_price_ticks;
                                    if ! book_crossed {
                                        let level_change = level_change.iter()
                                            .find(|&level_change| 
                                                level_change.price_ticks == old_live_bid_order.order.price_ticks 
                                                && level_change.side == Side::Bid
                                            );
                                        if let Some(level_change) = level_change {
                                            if level_change.new_visible_micro >= level_change.old_visible_micro {
                                                old_live_bid_order.last_visible_micro = level_change.new_visible_micro;
                                            } else {
                                                let consumed = level_change.old_visible_micro - level_change.new_visible_micro;
                                                let ahead_reduction = min(consumed, old_live_bid_order.queue_ahead_micro);
                                                old_live_bid_order.queue_ahead_micro -= ahead_reduction;
                                                let trade_consumed = (ALPHA * consumed as f64) as i64;
                                                let trade_used_for_ahead = min(trade_consumed, ahead_reduction);
                                                let leftover = trade_consumed - trade_used_for_ahead;
                                                let fill = min(leftover, old_live_bid_order.remaining_micro);
                                                old_live_bid_order.remaining_micro -= fill;
                                                if old_live_bid_order.remaining_micro == 0 {
                                                    if ! old_live_bid_order.had_any_fill {
                                                        self.total_orders_any_fill += 1;
                                                        old_live_bid_order.had_any_fill = true;
                                                    }
                                                    self.total_fees_paid_quote_micro -= position.fees_quote_micro;
                                                    order_events.push(
                                                        OrderEvent::Filled(
                                                            Side::Bid, 
                                                            old_live_bid_order.order.price_ticks, 
                                                            fill, 
                                                            current_cts,
                                                            old_live_bid_order.order.order_id,
                                                        )
                                                    );
                                                    position.on_fill(
                                                        &Side::Bid, 
                                                        old_live_bid_order.order.price_ticks, 
                                                        fill, 
                                                        &summary, 
                                                        &self.desynced
                                                    );
                                                    old_live_bid_order.order.order_state = OrderState::Filled;
                                                } else if fill > 0 {
                                                    if ! old_live_bid_order.had_any_fill {
                                                        self.total_orders_any_fill += 1;
                                                        old_live_bid_order.had_any_fill = true;
                                                    }
                                                    self.total_fees_paid_quote_micro -= position.fees_quote_micro;
                                                    order_events.push(
                                                        OrderEvent::PartialFilled(
                                                            Side::Bid, 
                                                            old_live_bid_order.order.price_ticks, 
                                                            fill, 
                                                            current_cts,
                                                            old_live_bid_order.order.order_id,
                                                        )        
                                                    );
                                                    position.on_fill(
                                                        &Side::Bid, 
                                                        old_live_bid_order.order.price_ticks, 
                                                        fill, 
                                                        &summary, 
                                                        &self.desynced
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    if old_live_bid_order.order.order_state != OrderState::Filled {
                                        self.working_bid = Some(OrderType::LiveReplace(
                                            old_live_bid_order, 
                                            new_bid_order, 
                                            cancel_effective_cts
                                        ));
                                    }
                                }
                            } 
                        }
                        OrderState::Filled => {
                            self.working_bid = Some(OrderType::Standard(new_bid_order));
                        }
                        OrderState::Cancelled => {
                            self.working_bid = Some(OrderType::Standard(new_bid_order));
                            order_events.push(
                                    OrderEvent::Cancelled(
                                        Side::Bid, 
                                        old_live_bid_order.order.price_ticks, 
                                        old_live_bid_order.order.size_micro, 
                                        summary.cts.unwrap(),
                                        old_live_bid_order.order.order_id,
                                    )
                            );    
                        }
                        _ => {
                            eprintln!("Error in order management system!");
                        }
                    }    
                }
            }
        }
        if let Some(order_type) = self.working_ask.take() {
            match order_type {
                OrderType::Standard(mut ask_order) => {
                    match ask_order.order_state {
                        OrderState::New => {
                            // Ask-side handling mirrors the bid path, including
                            // latency and risk checks before the order goes live.
                            let latency = PLACE_LATENCY_MS + rng.random_range(100..=200u64);
                            let current_cts = summary.cts.unwrap();
                            let activate_at_cts = current_cts + latency;
                            let new_open_basis_micro = ask_order.size_micro - position.base_pos_micro;
                            let new_leverage = (new_open_basis_micro.unsigned_abs() * summary.mid_ticks_2x as u64) as f64 
                                / (2 * PRICE_SCALE as i128 * position.equity_quote_micro) as f64;
                            if ask_order.price_ticks <= summary.best_bid_price_ticks {
                                self.total_cancelled_orders += 1;
                                self.total_rejected_orders += 1;
                                self.working_ask = None;
                                order_events.push(
                                    OrderEvent::Rejected(
                                        Side::Ask, 
                                        ask_order.price_ticks, 
                                        ask_order.size_micro, 
                                        current_cts,
                                        ask_order.order_id,
                                        "Proposed ask <= best bid."
                                    )
                                );
                            } else if new_open_basis_micro.unsigned_abs() > self.max_abs_inventory_micro 
                                || new_leverage > self.max_leverage
                                || position.equity_quote_micro <= 0 {
                                self.total_cancelled_orders += 1;
                                self.total_rejected_orders += 1;
                                self.working_ask = None;
                                order_events.push(
                                    OrderEvent::Rejected(
                                        Side::Ask, 
                                        ask_order.price_ticks, 
                                        ask_order.size_micro, 
                                        current_cts,
                                        ask_order.order_id,
                                        "Exchange risk limits violated."
                                    )
                                ); 
                            } else {    
                                ask_order.order_state = OrderState::LivePending(activate_at_cts);
                                order_events.push(
                                    OrderEvent::Placed(
                                        Side::Ask, 
                                        ask_order.price_ticks, 
                                        ask_order.size_micro, 
                                        current_cts,
                                        ask_order.order_id,
                                    )
                                );
                                self.working_ask = Some(OrderType::Standard(ask_order));
                            }
                        }
                        OrderState::LivePending(activate_at_cts) => {
                            let current_cts = summary.cts.unwrap();
                            if current_cts >= activate_at_cts {
                                let visible = book.asks
                                    .get(&(ask_order.price_ticks))
                                    .unwrap_or(&0);
                                ask_order.order_state = OrderState::Live; 
                                self.working_ask = Some(
                                    OrderType::Live(
                                        LiveOrder { 
                                            order: ask_order,
                                            remaining_micro: ask_order.size_micro, 
                                            queue_ahead_micro: *visible, 
                                            last_visible_micro: *visible, 
                                            live_since_cts: current_cts,
                                            had_any_fill: false,
                                        }
                                    )
                                );
                                self.total_live_orders += 1;
                                order_events.push(
                                   OrderEvent::LiveAck(
                                    Side::Ask, 
                                    ask_order.price_ticks, 
                                    ask_order.size_micro, 
                                    current_cts,
                                    ask_order.order_id,
                                    ) 
                                );
                            } else {
                                self.working_ask = Some(OrderType::Standard(ask_order));
                            }
                        }
                        _ => {
                            eprintln!("Error in order management system!");
                        }
                    }     
                }
                OrderType::Live(mut live_ask_order) => {
                    match live_ask_order.order.order_state {
                        OrderState::Live => {
                            if matches!(action, Action::AcceptGap(_, _)) || matches!(action, Action::AcceptReset(_)) {
                                self.desynced = true;
                                let visible = book.asks
                                    .get(&live_ask_order.order.price_ticks)
                                    .unwrap_or(&0);
                                let live_since_cts = live_ask_order.live_since_cts; 
                                self.working_ask = Some(
                                    OrderType::Live(
                                        LiveOrder { 
                                            order: live_ask_order.order,
                                            remaining_micro: live_ask_order.remaining_micro, 
                                            queue_ahead_micro: *visible, 
                                            last_visible_micro: *visible, 
                                            live_since_cts: live_since_cts,
                                            had_any_fill: live_ask_order.had_any_fill,
                                        }
                                    )
                                );
                            } else {
                                if self.desynced == true && matches!(action, Action::Accept(_)) {
                                    if self.clean_updates_after_desync == 100 {
                                        self.desynced = false;
                                        self.clean_updates_after_desync = 0;
                                    }
                                    self.clean_updates_after_desync += 1;
                                }
                                let book_crossed = summary.best_bid_price_ticks >= summary.best_ask_price_ticks;
                                if ! book_crossed {
                                    let level_change = level_change.iter()
                                        .find(|&level_change| 
                                            level_change.price_ticks == live_ask_order.order.price_ticks 
                                            && level_change.side == Side::Ask
                                        );
                                    if let Some(level_change) = level_change {
                                        if level_change.new_visible_micro >= level_change.old_visible_micro {
                                            live_ask_order.last_visible_micro = level_change.new_visible_micro;
                                        } else {
                                            let consumed = level_change.old_visible_micro - level_change.new_visible_micro;
                                            let ahead_reduction = min(consumed, live_ask_order.queue_ahead_micro);
                                            live_ask_order.queue_ahead_micro -= ahead_reduction;
                                            let trade_consumed = (ALPHA * consumed as f64) as i64;
                                            let trade_used_for_ahead = min(trade_consumed, ahead_reduction);
                                            let leftover = trade_consumed - trade_used_for_ahead;
                                            let fill = min(leftover, live_ask_order.remaining_micro);
                                            live_ask_order.remaining_micro -= fill;
                                            if live_ask_order.remaining_micro == 0 {
                                                if ! live_ask_order.had_any_fill {
                                                    self.total_orders_any_fill += 1;
                                                    live_ask_order.had_any_fill = true;
                                                }
                                                self.total_fees_paid_quote_micro -= position.fees_quote_micro;
                                                order_events.push(
                                                    OrderEvent::Filled(
                                                        Side::Ask, 
                                                        live_ask_order.order.price_ticks, 
                                                        fill, 
                                                        summary.cts.unwrap(),
                                                        live_ask_order.order.order_id,
                                                    )
                                                );
                                                position.on_fill(
                                                    &Side::Ask, 
                                                    live_ask_order.order.price_ticks, 
                                                    fill, 
                                                    &summary, 
                                                    &self.desynced
                                                );
                                                live_ask_order.order.order_state = OrderState::Filled;
                                            } else if fill > 0 {
                                                if ! live_ask_order.had_any_fill {
                                                    self.total_orders_any_fill += 1;
                                                    live_ask_order.had_any_fill = true;
                                                }
                                                self.total_fees_paid_quote_micro -= position.fees_quote_micro;
                                                order_events.push(
                                                    OrderEvent::PartialFilled(
                                                        Side::Ask, 
                                                        live_ask_order.order.price_ticks, 
                                                        fill, 
                                                        summary.cts.unwrap(),
                                                        live_ask_order.order.order_id,
                                                    )        
                                                );
                                                position.on_fill(
                                                    &Side::Ask, 
                                                    live_ask_order.order.price_ticks, 
                                                    fill, 
                                                    &summary, 
                                                    &self.desynced
                                                );
                                            }
                                        }
                                    }
                                }
                                if live_ask_order.order.order_state != OrderState::Filled {
                                    self.working_ask = Some(OrderType::Live(live_ask_order));
                                }
                            }
                        }
                        OrderState::Filled => {
                            self.working_ask = None;
                        }
                        _ => {
                            eprintln!("Error in order management system!");
                        }
                    }    
                }
                OrderType::StandardCancel(mut ask_order, cancel_effective_cts) => {
                    match ask_order.order_state {
                        OrderState::LivePending(activate_at_cts) => {
                            let current_cts = summary.cts.unwrap();
                            if current_cts >= cancel_effective_cts {
                                self.total_cancelled_orders += 1;
                                ask_order.order_state = OrderState::Cancelled;
                                self.working_ask = Some(OrderType::StandardCancel(ask_order, cancel_effective_cts));
                            } else if current_cts >= activate_at_cts {
                                let visible = book.asks
                                    .get(&(ask_order.price_ticks))
                                    .unwrap_or(&0);
                                ask_order.order_state = OrderState::Live; 
                                self.working_ask = Some(
                                    OrderType::LiveCancel(
                                        LiveOrder { 
                                            order: ask_order,
                                            remaining_micro: ask_order.size_micro, 
                                            queue_ahead_micro: *visible, 
                                            last_visible_micro: *visible, 
                                            live_since_cts: current_cts,
                                            had_any_fill: false,
                                        },
                                        cancel_effective_cts
                                    )
                                );
                                self.total_live_orders += 1;
                                order_events.push(
                                   OrderEvent::LiveAck(
                                    Side::Ask, 
                                    ask_order.price_ticks, 
                                    ask_order.size_micro, 
                                    current_cts,
                                    ask_order.order_id,
                                    ) 
                                );
                            } else {
                                self.working_ask = Some(OrderType::StandardCancel(ask_order, cancel_effective_cts))
                            }
                        }
                        OrderState::Cancelled => {
                            self.working_ask = None;
                            order_events.push(
                                    OrderEvent::Cancelled(
                                        Side::Ask, 
                                        ask_order.price_ticks, 
                                        ask_order.size_micro, 
                                        summary.cts.unwrap(),
                                        ask_order.order_id,
                                    )
                            );
                        }
                        _ => {
                            eprintln!("Error in order management system!");
                        }
                    }    
                }
                OrderType::LiveCancel(mut live_ask_order, cancel_effective_cts) => {
                    match live_ask_order.order.order_state {
                        OrderState::Live => {
                            let current_cts = summary.cts.unwrap();
                            if current_cts >= cancel_effective_cts {
                                self.total_cancelled_orders += 1;
                                live_ask_order.order.order_state = OrderState::Cancelled;
                                self.working_ask = Some(OrderType::LiveCancel(live_ask_order, cancel_effective_cts));    
                            } else {
                                if matches!(action, Action::AcceptGap(_, _)) || matches!(action, Action::AcceptReset(_)) {
                                    self.desynced = true;
                                    let visible = book.asks
                                        .get(&(live_ask_order.order.price_ticks))
                                        .unwrap_or(&0);
                                    let live_since_cts = live_ask_order.live_since_cts; 
                                    self.working_ask = Some(
                                        OrderType::LiveCancel(
                                            LiveOrder { 
                                                order: live_ask_order.order,
                                                remaining_micro: live_ask_order.remaining_micro, 
                                                queue_ahead_micro: *visible, 
                                                last_visible_micro: *visible, 
                                                live_since_cts: live_since_cts,
                                                had_any_fill: live_ask_order.had_any_fill,
                                            },
                                            cancel_effective_cts
                                        )
                                    );
                                } else {
                                    if self.desynced == true && matches!(action, Action::Accept(_)) {
                                        if self.clean_updates_after_desync == 100 {
                                            self.desynced = false;
                                            self.clean_updates_after_desync = 0;
                                        }
                                        self.clean_updates_after_desync += 1;
                                    }
                                    let book_crossed = summary.best_bid_price_ticks >= summary.best_ask_price_ticks;
                                    if ! book_crossed {
                                        let level_change = level_change.iter()
                                            .find(|&level_change| 
                                                level_change.price_ticks == live_ask_order.order.price_ticks 
                                                && level_change.side == Side::Ask
                                            );
                                        if let Some(level_change) = level_change {
                                            if level_change.new_visible_micro >= level_change.old_visible_micro {
                                                live_ask_order.last_visible_micro = level_change.new_visible_micro;
                                            } else {
                                                let consumed = level_change.old_visible_micro - level_change.new_visible_micro;
                                                let ahead_reduction = min(consumed, live_ask_order.queue_ahead_micro);
                                                live_ask_order.queue_ahead_micro -= ahead_reduction;
                                                let trade_consumed = (ALPHA * consumed as f64) as i64;
                                                let trade_used_for_ahead = min(trade_consumed, ahead_reduction);
                                                let leftover = trade_consumed - trade_used_for_ahead;
                                                let fill = min(leftover, live_ask_order.remaining_micro);
                                                live_ask_order.remaining_micro -= fill;
                                                if live_ask_order.remaining_micro == 0 {
                                                    if ! live_ask_order.had_any_fill {
                                                        self.total_orders_any_fill += 1;
                                                        live_ask_order.had_any_fill = true;
                                                    }
                                                    self.total_fees_paid_quote_micro -= position.fees_quote_micro;
                                                    order_events.push(
                                                        OrderEvent::Filled(
                                                            Side::Ask, 
                                                            live_ask_order.order.price_ticks, 
                                                            fill, 
                                                            current_cts,
                                                            live_ask_order.order.order_id,
                                                        )
                                                    );
                                                    position.on_fill(
                                                        &Side::Ask, 
                                                        live_ask_order.order.price_ticks, 
                                                        fill, 
                                                        &summary, 
                                                        &self.desynced
                                                    );
                                                    live_ask_order.order.order_state = OrderState::Filled;
                                                } else if fill > 0 {
                                                    if ! live_ask_order.had_any_fill {
                                                        self.total_orders_any_fill += 1;
                                                        live_ask_order.had_any_fill = true;
                                                    }
                                                    self.total_fees_paid_quote_micro -= position.fees_quote_micro;
                                                    order_events.push(
                                                        OrderEvent::PartialFilled(
                                                            Side::Ask, 
                                                            live_ask_order.order.price_ticks, 
                                                            fill, 
                                                            current_cts,
                                                            live_ask_order.order.order_id,
                                                        )        
                                                    );
                                                    position.on_fill(
                                                        &Side::Ask, 
                                                        live_ask_order.order.price_ticks, 
                                                        fill, 
                                                        &summary, 
                                                        &self.desynced
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    if live_ask_order.order.order_state != OrderState::Filled {
                                        self.working_ask = Some(OrderType::LiveCancel(live_ask_order, cancel_effective_cts));
                                    }
                                }
                            } 
                        }
                        OrderState::Filled => {
                            self.working_ask = None;
                        }
                        OrderState::Cancelled => {
                            self.working_ask = None;
                            order_events.push(
                                    OrderEvent::Cancelled(
                                        Side::Ask, 
                                        live_ask_order.order.price_ticks, 
                                        live_ask_order.order.size_micro, 
                                        summary.cts.unwrap(),
                                        live_ask_order.order.order_id,
                                    )
                            );    
                        }
                        _ => {
                            eprintln!("Error in order management system!");
                        }
                    }    
                }
                OrderType::StandardReplace(
                    mut old_ask_order, 
                    new_ask_order,  
                    cancel_effective_cts
                ) => {
                    match old_ask_order.order_state {
                        OrderState::New => {
                            self.working_ask = Some(OrderType::Standard(new_ask_order));
                            order_events.push(
                                    OrderEvent::Cancelled(
                                        Side::Ask, 
                                        old_ask_order.price_ticks, 
                                        old_ask_order.size_micro, 
                                        summary.cts.unwrap(),
                                        old_ask_order.order_id,
                                    )
                            );    
                        }
                        OrderState::LivePending(activate_at_cts) => {
                            let current_cts = summary.cts.unwrap();
                            if current_cts >= cancel_effective_cts {
                                self.total_cancelled_orders += 1;
                                old_ask_order.order_state = OrderState::Cancelled;
                                self.working_ask = Some(OrderType::StandardReplace(
                                    old_ask_order, 
                                    new_ask_order, 
                                    cancel_effective_cts,
                                ));
                            } else if current_cts >= activate_at_cts {
                                let visible = book.asks
                                    .get(&(old_ask_order.price_ticks))
                                    .unwrap_or(&0);
                                old_ask_order.order_state = OrderState::Live; 
                                self.working_ask = Some(
                                    OrderType::LiveReplace(
                                        LiveOrder { 
                                            order: old_ask_order,
                                            remaining_micro: old_ask_order.size_micro, 
                                            queue_ahead_micro: *visible, 
                                            last_visible_micro: *visible, 
                                            live_since_cts: current_cts,
                                            had_any_fill: false,
                                        },
                                        new_ask_order,
                                        cancel_effective_cts,
                                    )
                                );
                                self.total_live_orders += 1;
                                order_events.push(
                                   OrderEvent::LiveAck(
                                    Side::Ask, 
                                    old_ask_order.price_ticks, 
                                    old_ask_order.size_micro, 
                                    current_cts,
                                    old_ask_order.order_id,
                                    ) 
                                );
                            } else {
                                self.working_ask = Some(OrderType::StandardReplace(
                                    old_ask_order, 
                                    new_ask_order, 
                                    cancel_effective_cts
                                ))
                            }
                        }
                        OrderState::Cancelled => {
                            self.working_ask = Some(OrderType::Standard(new_ask_order));
                            order_events.push(
                                    OrderEvent::Cancelled(
                                        Side::Ask, 
                                        old_ask_order.price_ticks, 
                                        old_ask_order.size_micro, 
                                        summary.cts.unwrap(),
                                        old_ask_order.order_id,
                                    )
                            );
                        }
                        _ => {
                            eprintln!("Error in order management system!");
                        }    
                    }
                }
                OrderType::LiveReplace(
                    mut old_live_ask_order, 
                    new_ask_order, 
                    cancel_effective_cts
                ) => {
                    match old_live_ask_order.order.order_state {
                        OrderState::Live => {
                            let current_cts = summary.cts.unwrap();
                            if current_cts >= cancel_effective_cts {
                                self.total_cancelled_orders += 1;
                                old_live_ask_order.order.order_state = OrderState::Cancelled;
                                self.working_ask = Some(OrderType::LiveReplace(
                                    old_live_ask_order, 
                                    new_ask_order,  
                                    cancel_effective_cts,
                                ));    
                            } else {
                                if matches!(action, Action::AcceptGap(_, _)) || matches!(action, Action::AcceptReset(_)) {
                                    self.desynced = true;
                                    let visible = book.asks
                                        .get(&(old_live_ask_order.order.price_ticks))
                                        .unwrap_or(&0);
                                    let live_since_cts = old_live_ask_order.live_since_cts; 
                                    self.working_ask = Some(
                                        OrderType::LiveReplace(
                                            LiveOrder { 
                                                order: old_live_ask_order.order,
                                                remaining_micro: old_live_ask_order.remaining_micro, 
                                                queue_ahead_micro: *visible, 
                                                last_visible_micro: *visible, 
                                                live_since_cts: live_since_cts,
                                                had_any_fill: old_live_ask_order.had_any_fill,
                                            },
                                            new_ask_order,
                                            cancel_effective_cts,
                                        )
                                    );
                                } else {
                                    if self.desynced == true && matches!(action, Action::Accept(_)) {
                                        if self.clean_updates_after_desync == 100 {
                                            self.desynced = false;
                                            self.clean_updates_after_desync = 0;
                                        }
                                        self.clean_updates_after_desync += 1;
                                    }
                                    let book_crossed = summary.best_bid_price_ticks >= summary.best_ask_price_ticks;
                                    if ! book_crossed {
                                        let level_change = level_change.iter()
                                            .find(|&level_change| 
                                                level_change.price_ticks == old_live_ask_order.order.price_ticks 
                                                && level_change.side == Side::Ask
                                            );
                                        if let Some(level_change) = level_change {
                                            if level_change.new_visible_micro >= level_change.old_visible_micro {
                                                old_live_ask_order.last_visible_micro = level_change.new_visible_micro;
                                            } else {
                                                let consumed = level_change.old_visible_micro - level_change.new_visible_micro;
                                                let ahead_reduction = min(consumed, old_live_ask_order.queue_ahead_micro);
                                                old_live_ask_order.queue_ahead_micro -= ahead_reduction;
                                                let trade_consumed = (ALPHA * consumed as f64) as i64;
                                                let trade_used_for_ahead = min(trade_consumed, ahead_reduction);
                                                let leftover = trade_consumed - trade_used_for_ahead;
                                                let fill = min(leftover, old_live_ask_order.remaining_micro);
                                                old_live_ask_order.remaining_micro -= fill;
                                                if old_live_ask_order.remaining_micro == 0 {
                                                    if ! old_live_ask_order.had_any_fill {
                                                        self.total_orders_any_fill += 1;
                                                        old_live_ask_order.had_any_fill = true;
                                                    }
                                                    self.total_fees_paid_quote_micro -= position.fees_quote_micro;
                                                    order_events.push(
                                                        OrderEvent::Filled(
                                                            Side::Ask, 
                                                            old_live_ask_order.order.price_ticks, 
                                                            fill, 
                                                            current_cts,
                                                            old_live_ask_order.order.order_id,
                                                        )
                                                    );
                                                    position.on_fill(
                                                        &Side::Ask, 
                                                        old_live_ask_order.order.price_ticks, 
                                                        fill, 
                                                        &summary, 
                                                        &self.desynced
                                                    );
                                                    old_live_ask_order.order.order_state = OrderState::Filled;
                                                } else if fill > 0 {
                                                    if ! old_live_ask_order.had_any_fill {
                                                        self.total_orders_any_fill += 1;
                                                        old_live_ask_order.had_any_fill = true;
                                                    }
                                                    self.total_fees_paid_quote_micro -= position.fees_quote_micro;
                                                    order_events.push(
                                                        OrderEvent::PartialFilled(
                                                            Side::Ask, 
                                                            old_live_ask_order.order.price_ticks, 
                                                            fill, 
                                                            current_cts,
                                                            old_live_ask_order.order.order_id,
                                                        )        
                                                    );
                                                    position.on_fill(
                                                        &Side::Ask, 
                                                        old_live_ask_order.order.price_ticks, 
                                                        fill, 
                                                        &summary, 
                                                        &self.desynced
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    if old_live_ask_order.order.order_state != OrderState::Filled {
                                        self.working_ask = Some(OrderType::LiveReplace(
                                            old_live_ask_order, 
                                            new_ask_order, 
                                            cancel_effective_cts
                                        ));
                                    }
                                }
                            } 
                        }
                        OrderState::Filled => {
                            self.working_ask = Some(OrderType::Standard(new_ask_order));
                        }
                        OrderState::Cancelled => {
                            self.working_ask = Some(OrderType::Standard(new_ask_order));
                            order_events.push(
                                    OrderEvent::Cancelled(
                                        Side::Ask, 
                                        old_live_ask_order.order.price_ticks, 
                                        old_live_ask_order.order.size_micro, 
                                        summary.cts.unwrap(),
                                        old_live_ask_order.order.order_id,
                                    )
                            );    
                        }
                        _ => {
                            eprintln!("Error in order management system!");
                        }
                    }    
                }
            }
        }
        &*order_events
    }

    /// Requests a brand new order on one side of the book.
    pub fn place(&mut self, side: Side, price_ticks: i64, size_micro: i64) {
        match side { 
            Side::Bid => {
                self.working_bid = Some( 
                    OrderType::Standard(
                        Order { side, price_ticks, size_micro, order_state: OrderState::New, order_id: self.next_order_id }
                    )
                );
                self.next_order_id += 1;
            } 
            Side::Ask => {
                self.working_ask = Some( 
                    OrderType::Standard(
                        Order { side, price_ticks, size_micro, order_state: OrderState::New, order_id: self.next_order_id }
                    )
                );
                self.next_order_id += 1;
            }
        }
    }

    /// Requests cancellation of the currently working order on a side.
    pub fn cancel(&mut self, side: Side, rng: &mut ThreadRng, cts: u64) {
        let latency = rng.random_range(100..=200u64);
        let cancel_effective_cts = cts + CANCEL_LATENCY_MS + latency;
        match side {
            Side::Bid => {
                if let Some(order_type) = self.working_bid.as_mut() {
                    match order_type  {
                        OrderType::Standard(bid_order) => {
                            self.working_bid = Some(OrderType::StandardCancel(
                                *bid_order, 
                                cancel_effective_cts
                            ));
                        }
                        OrderType::Live(live_bid_order) => {
                            self.working_bid = Some(OrderType::LiveCancel(
                                *live_bid_order,
                                cancel_effective_cts
                            ));
                        }
                        OrderType::StandardReplace(
                            old_bid_order, 
                            _new_bid_order, 
                            cancel_effective_cts
                        ) => {
                            self.working_bid = Some(OrderType::StandardCancel(
                                *old_bid_order, 
                                *cancel_effective_cts
                            ));
                        }
                        OrderType::LiveReplace(
                            old_live_bid_order, 
                            _new_bid_order, 
                            cancel_effective_cts
                        ) => {
                            self.working_bid = Some(OrderType::LiveCancel(
                                *old_live_bid_order, 
                                *cancel_effective_cts
                            ));
                        }
                        _ => {}
                    };
                }
            } 
            Side::Ask => {
                if let Some(order_type) = self.working_ask.as_mut() {
                    match order_type  {
                        OrderType::Standard(ask_order) => {
                            self.working_ask = Some(OrderType::StandardCancel(
                                *ask_order, 
                                cancel_effective_cts
                            ));
                        }
                        OrderType::Live(live_ask_order) => {
                            self.working_ask = Some(OrderType::LiveCancel(
                                *live_ask_order, 
                                cancel_effective_cts
                            ));
                        }
                        OrderType::StandardReplace(
                            old_ask_order, 
                            _new_ask_order, 
                            cancel_effective_cts
                        ) => {
                            self.working_ask = Some(OrderType::StandardCancel(
                                *old_ask_order, 
                                *cancel_effective_cts
                            ));
                        }
                        OrderType::LiveReplace(
                            old_live_ask_order, 
                            _new_ask_order, 
                            cancel_effective_cts
                        ) => {
                            self.working_ask = Some(OrderType::LiveCancel(
                                *old_live_ask_order, 
                                *cancel_effective_cts
                            ));
                        }
                        _ => {}      
                    };
                }
            }
        }
    }

    /// Requests a replace by preserving the old order until cancel latency
    /// elapses and staging a new order behind it.
    pub fn replace(
        &mut self, 
        side: Side, 
        new_price_ticks: i64, 
        new_size_micro: i64, 
        rng: &mut ThreadRng, 
        cts: u64
    ) {
        let latency = rng.random_range(100..=200u64);
        let cancel_effective_cts = cts + CANCEL_LATENCY_MS + latency;
        match side {
            Side::Bid => {
                if let Some(order_type) = self.working_bid.as_mut() {
                    match order_type  {
                        OrderType::Standard(bid_order) => {
                            self.working_bid = Some(OrderType::StandardReplace(
                                *bid_order,
                                Order {
                                    side: side, 
                                    price_ticks: new_price_ticks, 
                                    size_micro: new_size_micro, 
                                    order_state: OrderState::New, 
                                    order_id: self.next_order_id
                                }, 
                                cancel_effective_cts
                            ));
                            self.next_order_id += 1;
                        }
                        OrderType::Live(live_bid_order) => {
                            self.working_bid = Some(OrderType::LiveReplace(
                                *live_bid_order,
                                Order {
                                    side: side, 
                                    price_ticks: new_price_ticks, 
                                    size_micro: new_size_micro, 
                                    order_state: OrderState::New, 
                                    order_id: self.next_order_id
                                }, 
                                cancel_effective_cts
                            ));
                            self.next_order_id += 1;
                        }
                        OrderType::StandardReplace(
                            old_bid_order, 
                            _prev_new_bid_order,
                            prev_cancel_effective_cts) => {
                                self.working_bid = Some(OrderType::StandardReplace(
                                *old_bid_order,
                                Order {
                                    side: side, 
                                    price_ticks: new_price_ticks, 
                                    size_micro: new_size_micro, 
                                    order_state: OrderState::New, 
                                    order_id: self.next_order_id
                                }, 
                                *prev_cancel_effective_cts
                                ));
                                self.next_order_id += 1;
                            }
                        OrderType::LiveReplace(
                            old_live_bid_order, 
                            _prev_new_bid_order,
                            prev_cancel_effective_cts) => {
                                self.working_bid = Some(OrderType::LiveReplace(
                                *old_live_bid_order,
                                Order {
                                    side: side, 
                                    price_ticks: new_price_ticks, 
                                    size_micro: new_size_micro, 
                                    order_state: OrderState::New, 
                                    order_id: self.next_order_id
                                }, 
                                *prev_cancel_effective_cts
                                ));
                                self.next_order_id += 1;
                            }
                        OrderType::StandardCancel(
                            bid_order, 
                            cancel_effective_cts
                        ) => {
                            self.working_bid = Some(OrderType::StandardReplace(
                                *bid_order,
                                Order {
                                    side: side, 
                                    price_ticks: new_price_ticks, 
                                    size_micro: new_size_micro, 
                                    order_state: OrderState::New, 
                                    order_id: self.next_order_id
                                }, 
                                *cancel_effective_cts
                            ));
                            self.next_order_id += 1;
                        }
                        OrderType::LiveCancel(
                            live_bid_order, 
                            cancel_effective_cts
                        ) => {
                            self.working_bid = Some(OrderType::LiveReplace(
                                *live_bid_order,
                                Order {
                                    side: side, 
                                    price_ticks: new_price_ticks, 
                                    size_micro: new_size_micro, 
                                    order_state: OrderState::New, 
                                    order_id: self.next_order_id
                                }, 
                                *cancel_effective_cts
                            ));
                            self.next_order_id += 1;
                        }
                    };
                }
            } 
            Side::Ask => {
                if let Some(order_type) = self.working_ask.as_mut() {
                    match order_type  {
                        OrderType::Standard(ask_order) => {
                            self.working_ask = Some(OrderType::StandardReplace(
                                *ask_order,
                                Order {
                                    side: side, 
                                    price_ticks: new_price_ticks, 
                                    size_micro: new_size_micro, 
                                    order_state: OrderState::New, 
                                    order_id: self.next_order_id
                                }, 
                                cancel_effective_cts
                            ));
                            self.next_order_id += 1;
                        }
                        OrderType::Live(live_ask_order) => {
                            self.working_ask = Some(OrderType::LiveReplace(
                                *live_ask_order,
                                Order {
                                    side: side, 
                                    price_ticks: new_price_ticks, 
                                    size_micro: new_size_micro, 
                                    order_state: OrderState::New, 
                                    order_id: self.next_order_id
                                }, 
                                cancel_effective_cts
                            ));
                            self.next_order_id += 1;
                        }
                        OrderType::StandardReplace(
                            old_ask_order, 
                            _prev_new_ask_order,
                            prev_cancel_effective_cts) => {
                                self.working_ask = Some(OrderType::StandardReplace(
                                *old_ask_order,
                                Order {
                                    side: side, 
                                    price_ticks: new_price_ticks, 
                                    size_micro: new_size_micro, 
                                    order_state: OrderState::New, 
                                    order_id: self.next_order_id
                                }, 
                                *prev_cancel_effective_cts
                                ));
                                self.next_order_id += 1;
                            }
                        OrderType::LiveReplace(
                            old_live_ask_order, 
                            _prev_new_ask_order,
                            prev_cancel_effective_cts) => {
                                self.working_ask = Some(OrderType::LiveReplace(
                                *old_live_ask_order,
                                Order {
                                    side: side, 
                                    price_ticks: new_price_ticks, 
                                    size_micro: new_size_micro, 
                                    order_state: OrderState::New, 
                                    order_id: self.next_order_id
                                }, 
                                *prev_cancel_effective_cts
                                ));
                                self.next_order_id += 1;
                            }
                        OrderType::StandardCancel(
                            ask_order, 
                            cancel_effective_cts
                        ) => {
                            self.working_ask = Some(OrderType::StandardReplace(
                                *ask_order,
                                Order {
                                    side: side, 
                                    price_ticks: new_price_ticks, 
                                    size_micro: new_size_micro, 
                                    order_state: OrderState::New, 
                                    order_id: self.next_order_id
                                }, 
                                *cancel_effective_cts
                            ));
                            self.next_order_id += 1;
                        }
                        OrderType::LiveCancel(
                            live_ask_order, 
                            cancel_effective_cts
                        ) => {
                            self.working_ask = Some(OrderType::LiveReplace(
                                *live_ask_order,
                                Order {
                                    side: side, 
                                    price_ticks: new_price_ticks, 
                                    size_micro: new_size_micro, 
                                    order_state: OrderState::New, 
                                    order_id: self.next_order_id
                                }, 
                                *cancel_effective_cts
                            ));
                            self.next_order_id += 1;
                        }
                    };
                }
            }
        }
    }

    /// Returns the current working ask state, if any.
    pub fn get_working_ask(&self) -> Option<&OrderType> {
        self.working_ask.as_ref()
    }

    /// Returns the current working bid state, if any.
    pub fn get_working_bid(&self) -> Option<&OrderType> {
        self.working_bid.as_ref()
    }

    /// Returns the next order identifier that will be assigned.
    pub fn get_next_order_id(&self) -> u64 {
        self.next_order_id
    }

    /// Returns cumulative fees paid across all fills in quote micro-units.
    pub fn get_total_fees_paid_quote_micro(&self) -> i128 {
        self.total_fees_paid_quote_micro
    }

    /// Returns the number of orders that received at least one fill.
    pub fn get_total_orders_any_fill(&self) -> u64 {
        self.total_orders_any_fill
    }

    /// Returns the number of orders that became fully live in the book.
    pub fn get_total_live_orders(&self) -> u64 {
        self.total_live_orders
    }

    /// Returns the number of orders that ended through cancellation logic.
    pub fn get_total_cancelled_orders(&self) -> u64 {
        self.total_cancelled_orders
    }
}
