use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::Deserialize;

use super::{from_str_to_side, from_str_to_status, from_str_to_tif, from_str_to_type};
use crate::utils::{from_str_to_f64, to_lowercase};

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum Stream<'a> {
    // 借用式零拷贝（QUI-77）：Depth/Trade 的 px/qty 借进原 WS 文本，省 1+2N+2M
    // String 分配；bench 实测 serde_json borrowed 比 owned p50 −44%/p99 −60%。
    #[serde(borrow)]
    EventStream(EventStream<'a>),
    Result(Result),
}

#[derive(Deserialize, Debug)]
pub struct Result {
    pub result: Option<String>,
    pub id: String,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "e")]
pub enum EventStream<'a> {
    // 热路径变体借用（DepthUpdate/Trade）；冷变体（order/account）仍 owned，
    // 'a 由借用变体使用即满足编译要求。
    #[serde(rename = "depthUpdate")]
    #[serde(borrow)]
    DepthUpdate(Depth<'a>),
    #[serde(rename = "trade")]
    Trade(Trade<'a>),
    #[serde(rename = "ORDER_TRADE_UPDATE")]
    OrderTradeUpdate(OrderTradeUpdate),
    #[serde(rename = "TRADE_LITE")]
    TradeLite(TradeLite),
    #[serde(rename = "ACCOUNT_UPDATE")]
    AccountUpdate(AccountUpdate),
    #[serde(rename = "listenKeyExpired")]
    ListenKeyExpired(ListenKeyStream),
    // QUI-86：bookTicker（BBO 事件触发流，博主处方 fair 用它非 depth top）。热路径 → 借用式。
    #[serde(rename = "bookTicker")]
    BookTicker(BookTicker<'a>),
}

// QUI-86 bookTicker JSON：{u,s,b,B,a,A,T,E}。价/量借用式（Binance 数字串不含转义），
// symbol owned（to_lowercase 必分配，同 Depth）。
#[derive(Deserialize, Debug)]
pub struct BookTicker<'a> {
    #[serde(rename = "u")]
    pub update_id: i64,
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "b")]
    #[serde(borrow)]
    pub best_bid: &'a str,
    #[serde(rename = "B")]
    pub best_bid_qty: &'a str,
    #[serde(rename = "a")]
    pub best_ask: &'a str,
    #[serde(rename = "A")]
    pub best_ask_qty: &'a str,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "E")]
    pub event_time: i64,
}

#[derive(Deserialize, Debug)]
pub struct Depth<'a> {
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "E")]
    pub event_time: i64,
    // symbol 仍 owned：to_lowercase 必须分配（无法借出小写副本），且只 1 次分配，
    // 相对 bids/asks 的 2N+2M 可忽略。
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    // for Coin-M futures
    // #[serde(rename = "ps")]
    // pub pair: String,
    #[serde(rename = "U")]
    pub first_update_id: i64,
    #[serde(rename = "u")]
    pub last_update_id: i64,
    #[serde(rename = "pu")]
    pub prev_update_id: i64,
    // 借用式：px/qty 串对借进原文本（Binance 数字串从不含转义 → 总能借用）。
    #[serde(rename = "b")]
    #[serde(borrow)]
    pub bids: Vec<(&'a str, &'a str)>,
    #[serde(rename = "a")]
    pub asks: Vec<(&'a str, &'a str)>,
}

#[derive(Deserialize, Debug)]
pub struct Trade<'a> {
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "t")]
    pub id: i64,
    #[serde(rename = "p")]
    #[serde(borrow)]
    pub price: &'a str,
    #[serde(rename = "q")]
    pub qty: &'a str,
    #[serde(rename = "X")]
    pub type_: &'a str,
    #[serde(rename = "m")]
    pub is_the_buyer_the_market_maker: bool,
}

#[derive(Deserialize, Debug)]
pub struct TradeLite {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "q")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub qty: f64,
    #[serde(rename = "p")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(rename = "m")]
    pub is_this_trade_the_market_maker: bool,
    #[serde(rename = "c")]
    pub client_order_id: String,
    #[serde(rename = "S")]
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(rename = "L")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub last_filled_price: f64,
    #[serde(rename = "l")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub last_filled_qty: f64,
    #[serde(rename = "t")]
    pub trade_id: i64,
    #[serde(rename = "i")]
    pub order_id: i64,
}

#[derive(Deserialize, Debug)]
pub struct AccountUpdate {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "a")]
    pub account: Account,
}

#[derive(Deserialize, Debug)]
pub struct Account {
    #[serde(rename = "m")]
    pub ev_reason: String,
    #[serde(rename = "B")]
    pub balance: Vec<Balance>,
    #[serde(rename = "P")]
    pub position: Vec<Position>,
}

#[derive(Deserialize, Debug)]
pub struct Balance {
    #[serde(rename = "a")]
    pub asset: String,
    #[serde(rename = "wb")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub wallet_balance: f64,
    #[serde(rename = "cw")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub cross_wallet_balance: f64,
    #[serde(rename = "bc")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub balance_change: f64,
}

#[derive(Deserialize, Debug)]
pub struct Position {
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "pa")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub position_amount: f64,
    #[serde(rename = "ep")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub entry_price: f64,
    #[serde(rename = "bep")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub breakeven_price: f64,
    #[serde(rename = "cr")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub prefee_accumulated_realized: f64,
    #[serde(rename = "up")]
    pub unrealized_pnl: String,
    #[serde(rename = "mt")]
    pub margin_type: String,
    #[serde(rename = "iw")]
    pub isolated_wallet: Option<String>,
    #[serde(rename = "ps")]
    pub position_side: String,
}

#[derive(Deserialize, Debug)]
pub struct OrderTradeUpdate {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "T")]
    pub transaction_time: i64,
    #[serde(rename = "o")]
    pub order: Order,
}

#[derive(Deserialize, Debug)]
pub struct Order {
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "c")]
    pub client_order_id: String,
    #[serde(rename = "S")]
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(rename = "o")]
    #[serde(deserialize_with = "from_str_to_type")]
    pub order_type: OrdType,
    #[serde(rename = "f")]
    #[serde(deserialize_with = "from_str_to_tif")]
    pub time_in_force: TimeInForce,
    #[serde(rename = "q")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub original_qty: f64,
    #[serde(rename = "p")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub original_price: f64,
    #[serde(rename = "ap")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub average_price: f64,
    #[serde(rename = "sp")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub stop_price: f64,
    #[serde(rename = "x")]
    pub execution_type: String,
    #[serde(rename = "X")]
    #[serde(deserialize_with = "from_str_to_status")]
    pub order_status: Status,
    #[serde(rename = "i")]
    pub order_id: i64,
    #[serde(rename = "l")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub order_last_filled_qty: f64,
    #[serde(rename = "z")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub order_filled_accumulated_qty: f64,
    #[serde(rename = "L")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub last_filled_price: f64,
    // #[serde(rename = "N")]
    // pub commission_asset: Option<String>,
    // #[serde(rename = "n")]
    // pub commission: Option<String>,
    #[serde(rename = "T")]
    pub order_trade_time: i64,
    #[serde(rename = "t")]
    pub trade_id: i64,
    // #[serde(rename = "b")]
    // pub bid_notional: String,
    // #[serde(rename = "a")]
    // pub ask_notional: String,
    // #[serde(rename = "m")]
    // pub is_maker_side: bool,
    // #[serde(rename = "R")]
    // pub is_reduce_only: bool,
    // #[serde(rename = "wt")]
    // pub stop_price_working_type: String,
    // #[serde(rename = "ot")]
    // pub original_order_type: String,
    // #[serde(rename = "ps")]
    // pub position_side: String,
    // #[serde(rename = "cp")]
    // pub close_all: Option<String>,
    // #[serde(rename = "AP")]
    // pub activation_price: Option<String>,
    // #[serde(rename = "cr")]
    // pub callback_rate: Option<String>,
    // #[serde(rename = "pP")]
    // pub price_protection: bool,
    // #[serde(rename = "si")]
    // pub ignore: i64,
    // #[serde(rename = "ss")]
    // pub ignore: i64,
    // #[serde(rename = "rp")]
    // pub realized_profit: String,
    // #[serde(rename = "V")]
    // pub stp_mode: String,
    // #[serde(rename = "pm")]
    // pub price_match_mode: String,
    // #[serde(rename = "gtd")]
    // pub gtd_auto_cancel_time: i64,
}

#[derive(Deserialize, Debug)]
pub struct ListenKey {
    #[serde(rename = "listenKey")]
    pub listen_key: String,
}

#[derive(Deserialize, Debug)]
pub struct ListenKeyStream {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "listenKey")]
    pub listen_key: String,
}
