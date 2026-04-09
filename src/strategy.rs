use rand::rngs::ThreadRng;

use crate::{
    BookStateSummary, 
    Side, 
    exchange_sim::{
        ExchangeSim, 
        OrderType, 
        Position}
    };

/// Snapshot of the market features that a strategy can use when deciding
/// where and how large to quote.
pub struct Features {
    pub spread_ticks: i64,
    pub mid_ticks_2x: i64,
    pub top_k_imbalance: f64,
    pub top_k_notional_bid: i128,
    pub top_k_notional_ask: i128, 
    pub micro_price: i64,
}

#[derive(Debug, Default)]
/// Desired end-state for the current quoting cycle.
///
/// Each side is represented as an optional `(price_ticks, size_micro)` tuple.
/// `None` means the strategy wants no working order on that side.
pub struct DesiredQuoteState {
    bid: Option<(i64, i64)>,
    ask: Option<(i64, i64)>,
    cts: u64,
}

/// Strategy interface used by the main event loop.
///
/// A strategy is responsible for choosing the quote state it wants and then
/// reconciling that desired state against whatever the exchange simulator is
/// currently working.
pub trait Strategy {
    fn desired_quote_state(
        &self, 
        book_state: &BookStateSummary, 
        features: &Features, 
        position: &Position
    ) -> DesiredQuoteState;

    fn reconciling_engine(
        &self,
        side: Side, 
        desired_quote: Option<(i64, i64)>, 
        current_quote: Option<OrderType>,
        cts: u64,
        rng: &mut ThreadRng, 
        ex_sim: &mut ExchangeSim
    ) { 
        // Reconciliation is expressed as a desired-vs-current state machine.
        match (desired_quote, current_quote) {
            // Nothing is desired and nothing is working.
            (None, None) => {}
            // We want a quote and do not currently have one on this side.
            (Some((desired_price_ticks, desired_size_micro)), None) => {
                ex_sim.place(side, desired_price_ticks, desired_size_micro);
            }
            // We no longer want a quote, but one is still working.
            (None, Some(_)) => {
                ex_sim.cancel(side, rng, cts);
            }
            // A quote exists already, so update only if price or size changed.
            (Some((desired_price_ticks, desired_size_micro)), Some(order_type)) => {
                match order_type {
                    OrderType::Standard(order) => {
                        if !(desired_price_ticks == order.price_ticks 
                            && desired_size_micro == order.size_micro) {
                            ex_sim.replace(
                                side, 
                                desired_price_ticks, 
                                desired_size_micro, 
                                rng, 
                                cts
                            );
                        }
                    }
                    OrderType::Live(live_order) => {
                        if !(desired_price_ticks == live_order.order.price_ticks 
                            && desired_size_micro == live_order.order.size_micro) {
                            ex_sim.replace(
                                side, 
                                desired_price_ticks, 
                                desired_size_micro, 
                                rng, 
                                cts
                            );
                        }        
                    }
                    OrderType::StandardCancel(_, _) => {
                            // If a cancel is already in flight and the target
                            // changed again, convert that into a replace.
                            ex_sim.replace(
                                side, 
                                desired_price_ticks, 
                                desired_size_micro, 
                                rng, 
                                cts
                            ); 
                    }
                    OrderType::LiveCancel(_, _) => {
                            ex_sim.replace(
                                side, 
                                desired_price_ticks, 
                                desired_size_micro, 
                                rng, 
                                cts
                            );  
                    }
                    OrderType::StandardReplace(
                        _old_order, 
                        prev_new_order, 
                        _prev_cancel_effective_cts
                    ) => {
                        // Skip redundant replace requests when the pending new
                        // order already matches the desired quote.
                        if !(desired_price_ticks == prev_new_order.price_ticks 
                            && desired_size_micro == prev_new_order.size_micro) {
                            ex_sim.replace(
                                side, 
                                desired_price_ticks, 
                                desired_size_micro, 
                                rng, 
                                cts,
                            );
                        }     
                    }
                    OrderType::LiveReplace(
                        _old_live_order, 
                        prev_new_order, 
                        _prev_cancel_effective_cts
                    ) => {
                        if !(desired_price_ticks == prev_new_order.price_ticks 
                            && desired_size_micro == prev_new_order.size_micro) {
                            ex_sim.replace(
                                side, 
                                desired_price_ticks, 
                                desired_size_micro, 
                                rng, 
                                cts,
                            );
                        }     
                    }     
                }
            }
        }
    }

    fn quote_reconciler(
        &self,
        desired_quote_state: DesiredQuoteState, 
        rng: &mut ThreadRng, 
        ex_sim: &mut ExchangeSim
    ) {
        // Reconcile both sides independently so one side can change without
        // disturbing the other.
        let desired_bid_quote = desired_quote_state.bid;
        let desired_ask_quote = desired_quote_state.ask;
        let cts = desired_quote_state.cts;
        let current_bid_quote = ex_sim.get_working_bid().copied();
        let current_ask_quote = ex_sim.get_working_ask().copied();
        self.reconciling_engine(
            Side::Bid, 
            desired_bid_quote, 
            current_bid_quote, 
            cts, 
            rng, 
            ex_sim
        );
        self.reconciling_engine(
            Side::Ask, 
            desired_ask_quote, 
            current_ask_quote, 
            cts, 
            rng, 
            ex_sim
        );
    }
}

#[derive(Debug, Default)]
/// A simple two-sided quoting strategy centered around microprice.
pub struct SimpleMarketMaker {}

impl Strategy for SimpleMarketMaker {
    fn desired_quote_state(
        &self, 
        book_state: &BookStateSummary, 
        features: &Features, 
        position: &Position
    ) -> DesiredQuoteState {
        let current_cts = book_state.cts.unwrap();
        if ! position.get_desynced() {
            // Quote symmetrically around the current fair value estimate while
            // the simulator considers its queue model trustworthy.
            let half_spread_ticks = 100i64;
            let fair_price = features.micro_price;
            let bid = fair_price - half_spread_ticks;
            let ask = fair_price + half_spread_ticks;
            DesiredQuoteState { 
                bid: Some((bid, 1_000)), 
                ask: Some((ask, 1_000)), 
                cts: current_cts 
            }
        } else {
            // Stand down while the simulator is recovering from a sequencing gap
            // or book reset.
            DesiredQuoteState { 
                bid: None, 
                ask: None, 
                cts: current_cts 
            }
        }
    }        
}

