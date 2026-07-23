use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::{Deserialize, Serialize};

use super::{from_str_to_side, from_str_to_status, from_str_to_tif, from_str_to_type};
use crate::utils::{from_str_to_f64, to_lowercase};

// QUI-124：热变体(depthUpdate/aggTrade/trade)借用式零拷贝(px/qty 借进 WS 文本);
// 冷变体(kline)仍 owned,`'a` 由借用变体使用即满足。镜像 futures EventStream<'a>(add0eef)。
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "e")]
pub enum MarketEventStream<'a> {
    #[serde(rename = "depthUpdate")]
    #[serde(borrow)]
    DepthUpdate(Depth<'a>),
    #[serde(rename = "aggTrade")]
    AggTrade(AggTrade<'a>),
    #[serde(rename = "trade")]
    Trade(Trade<'a>),
    #[serde(rename = "kline")]
    Kline(KlineEvent),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "e")]
pub enum UserEventStream {
    #[serde(rename = "outboundAccountPosition")]
    OutboundAccountPosition(OutboundAccountPosition),
    #[serde(rename = "balanceUpdate")]
    BalanceUpdate(BalanceUpdate),
    #[serde(rename = "executionReport")]
    ExecutionReport(ExecutionReport),
    #[serde(rename = "listStatus")]
    ListStatus(ListStatus),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Result {
    pub result: Option<String>,
    pub id: String,
}

/// 现货 `<symbol>@bookTicker` 帧（实时 BBO）。⚠️ 与 futures 不同：**无 `e`/`T`/`E` 字段**
/// （keys 仅 u/s/b/B/a/A，2026-07-15 实测抓帧确认）。故不能进 `#[serde(tag="e")]` 的
/// `MarketEventStream`（匹配不中被丢），只能进下面 untagged 的 `MarketStream`。
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BookTicker<'a> {
    #[serde(rename = "u")]
    pub update_id: i64,
    // symbol owned:to_lowercase 必分配 + 跨 publish(1 次/消息)。价/量借用式(Binance 数字串不含转义)。
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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum MarketStream<'a> {
    #[serde(borrow)]
    EventStream(MarketEventStream<'a>),
    // BookTicker 放 Result 前：无 `e` 不误匹配 tagged EventStream；无 `id` 不误匹配 Result；
    // depthUpdate 的 b/a 是数组不误匹配这里的 String 字段。
    BookTicker(BookTicker<'a>),
    Result(Result),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
pub enum UserStream {
    EventStream(UserDataEvent),
    AuthResponse(AuthResponse),
    SubscribeResponse(SubscribeResponse),
}

#[derive(Debug, Deserialize, Clone)]
pub struct UserDataEvent {
    // ⚠️ 实测真实消息是 **wrapper**:{"subscriptionId":0,"event":{"e":"executionReport",...}}
    // (ws-api userDataStream.subscribe 的包裹)。`event` 是**嵌套键**,内层 `{"e":...}` 由
    // UserEventStream 的 #[serde(tag="e")] 匹配;`subscriptionId` 是额外字段被 serde 忽略。
    // **不能加 #[serde(flatten)]**——flatten 会去外层找 "e"(外层只有 subscriptionId+event)→ 全丢。
    pub event: UserEventStream,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AuthResponse {
    pub id: String,
    pub status: i32,
    pub result: Option<SessionLogonResult>,
    pub rate_limits: Option<Vec<RateLimit>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RateLimit {
    pub rate_limit_type: String,
    pub interval: String,
    pub interval_num: u32,
    pub limit: u32,
    pub count: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SessionLogonResult {
    pub api_key: String,
    pub authorized_since: u64,
    pub connected_since: u64,
    pub return_rate_limits: bool,
    pub server_time: u64,
    pub user_data_stream: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeResponse {
    pub id: String,
    pub status: i32,
    pub result: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeRequest {
    pub id: String,
    pub method: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Depth<'a> {
    #[serde(rename = "E")]
    pub event_time: i64,
    // symbol owned:to_lowercase 必分配(1 次/消息,相对 2N+2M 可忽略)。
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    // 币本位
    // #[serde(rename = "ps")]
    // pub pair: String,
    #[serde(rename = "U")]
    pub first_update_id: i64,
    #[serde(rename = "u")]
    pub last_update_id: i64,
    // 借用式:px/qty 串对借进原 WS 文本(Binance 数字串从不含转义 → 总能借用)。
    #[serde(rename = "b")]
    #[serde(borrow)]
    pub bids: Vec<(&'a str, &'a str)>,
    #[serde(rename = "a")]
    pub asks: Vec<(&'a str, &'a str)>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AggTrade<'a> {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "a")]
    pub aggregated_trade_id: i64,
    #[serde(rename = "p")]
    #[serde(borrow)]
    pub price: &'a str,
    #[serde(rename = "q")]
    pub quantity: &'a str,
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    #[serde(rename = "l")]
    pub last_trade_id: i64,
    #[serde(rename = "T")]
    pub filled_time: i64,
    #[serde(rename = "m")]
    pub is_market_maker: bool,
    #[serde(rename = "M")]
    pub ignore: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trade<'a> {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "t")]
    pub trade_id: i64,
    #[serde(rename = "p")]
    #[serde(borrow)]
    pub price: &'a str,
    #[serde(rename = "q")]
    pub quantity: &'a str,
    #[serde(rename = "T")]
    pub trade_time: i64,
    #[serde(rename = "m")]
    pub is_market_maker: bool,
    #[serde(rename = "M")]
    pub ignore: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct KlineEvent {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "k")]
    pub kline: Kline,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Kline {
    #[serde(rename = "t")]
    pub start_time: i64,
    #[serde(rename = "T")]
    pub end_time: i64,
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "i")]
    pub interval: String,
    #[serde(rename = "f")]
    pub first_trade_id: i64,
    #[serde(rename = "L")]
    pub last_trade_id: i64,
    #[serde(rename = "o")]
    pub open_price: String,
    #[serde(rename = "c")]
    pub close_price: String,
    #[serde(rename = "h")]
    pub high_price: String,
    #[serde(rename = "l")]
    pub low_price: String,
    #[serde(rename = "v")]
    pub volume: String,
    #[serde(rename = "n")]
    pub trade_count: i64,
    #[serde(rename = "x")]
    pub is_closed: bool,
    #[serde(rename = "q")]
    pub quote_asset_volume: String,
    #[serde(rename = "V")]
    pub taker_buy_base_asset_volume: String,
    #[serde(rename = "Q")]
    pub taker_buy_quote_asset_volume: String,
    #[serde(rename = "B")]
    pub ignore: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OutboundAccountPosition {
    #[serde(rename = "E")]
    pub event_time: i64,
    #[serde(rename = "u")]
    pub last_update_time: i64,
    #[serde(rename = "B")]
    pub balances: Vec<Balance>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Balance {
    #[serde(rename = "a")]
    #[serde(deserialize_with = "to_lowercase")]
    pub asset: String,
    #[serde(rename = "f")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub free: f64,
    #[serde(rename = "l")]
    pub locked: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExecutionReport {
    #[serde(rename = "E")]
    pub event_time: i64, // 事件时间
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
    pub quantity: f64,
    #[serde(rename = "p")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(rename = "P")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub stop_price: f64,
    #[serde(rename = "F")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub iceberg_quantity: f64,
    #[serde(rename = "g")]
    pub order_list_id: i64,
    #[serde(rename = "C")]
    pub original_client_order_id: Option<String>,
    #[serde(rename = "x")]
    pub execution_type: String,
    #[serde(rename = "X")]
    #[serde(deserialize_with = "from_str_to_status")]
    pub order_status: Status,
    #[serde(rename = "r")]
    pub rejection_reason: String,
    #[serde(rename = "i")]
    pub order_id: u64,
    #[serde(rename = "l")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub order_last_filled_quantity: f64,
    #[serde(rename = "z")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub order_filled_accumulated_quantity: f64,
    #[serde(rename = "L")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub last_filled_price: f64,
    #[serde(rename = "n")]
    pub commission: String,
    #[serde(rename = "N")]
    pub commission_asset: Option<String>,
    #[serde(rename = "T")]
    pub order_trade_time: u64,
    pub t: i64,
    #[serde(rename = "I")]
    pub execution_id: u64,
    #[serde(rename = "w")]
    pub is_on_order_book: bool,
    #[serde(rename = "m")]
    pub is_maker: bool,
    #[serde(rename = "M")]
    pub ignore: bool,
    #[serde(rename = "O")]
    pub order_creation_time: u64,
    #[serde(rename = "Z")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub cumulative_filled_amount: f64,
    #[serde(rename = "Y")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub last_filled_amount: f64,
    #[serde(rename = "Q")]
    #[serde(deserialize_with = "from_str_to_f64")]
    pub quote_order_quantity: f64,
    #[serde(rename = "D")]
    pub trailing_time: Option<i64>,
    #[serde(rename = "d")]
    pub trailing_delta: Option<i64>,
    #[serde(rename = "j")]
    pub strategy_id: Option<i64>,
    #[serde(rename = "J")]
    pub strategy_type: Option<i64>,
    #[serde(rename = "v")]
    pub prevented_match_id: Option<i64>,
    #[serde(rename = "A")]
    pub prevented_quantity: Option<String>,
    #[serde(rename = "B")]
    pub last_prevented_quantity: Option<String>,
    #[serde(rename = "u")]
    pub trade_group_id: Option<i64>,
    #[serde(rename = "U")]
    pub counter_order_id: Option<i64>,
    #[serde(rename = "Cs")]
    pub counter_symbol: Option<String>,
    #[serde(rename = "pl")]
    pub preventedexecution_quantity: Option<String>,
    #[serde(rename = "pL")]
    pub prevented_execution_price: Option<String>,
    #[serde(rename = "pY")]
    pub prevented_execution_quote_qty: Option<String>,
    #[serde(rename = "W")]
    pub working_time: Option<u64>,
    #[serde(rename = "b")]
    pub match_type: Option<String>,
    #[serde(rename = "a")]
    pub allocation_id: Option<i64>,
    #[serde(rename = "k")]
    pub working_floor: Option<String>,
    #[serde(rename = "uS")]
    pub used_sor: Option<bool>,
    #[serde(rename = "V")]
    pub self_trade_prevention_mode: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BalanceUpdate {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "a")]
    #[serde(deserialize_with = "to_lowercase")]
    pub asset: String,
    #[serde(rename = "d")]
    pub balance_delta: String,
    #[serde(rename = "T")]
    pub clear_time: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ListStatus {
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "g")]
    pub order_list_id: u64,
    #[serde(rename = "c")]
    pub contingency_type: String,
    #[serde(rename = "l")]
    pub list_status_type: String,
    #[serde(rename = "L")]
    pub list_order_status: String,
    #[serde(rename = "r")]
    pub rejection_reason: String,
    #[serde(rename = "C")]
    pub list_client_order_id: String,
    #[serde(rename = "T")]
    pub transaction_time: u64,
    #[serde(rename = "O")]
    pub orders: Vec<ListOrder>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ListOrder {
    #[serde(rename = "s")]
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    #[serde(rename = "i")]
    pub order_id: u64,
    #[serde(rename = "c")]
    pub client_order_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SignRequest {
    pub id: String,
    pub method: String,
    pub params: SignParams,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SignParams {
    pub api_key: String,
    pub signature: Option<String>,
    pub timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserDataRequest {
    pub id: String,
    pub method: String,
    pub params: SignParams,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct UserStreamSubscribeRequest {
    pub id: String,
    pub method: String,
}

#[cfg(test)]
mod tests {
    use super::{MarketEventStream, MarketStream};

    // QUI-106 序列化回归守卫：把「实测抓帧」变回归。三类帧各落对臂。
    // 现货 bookTicker 无 "e" 字段 → 必须由 untagged MarketStream::BookTicker 兜住。
    #[test]
    fn bookticker_frame_routes_to_bookticker() {
        // 2026-07-15 实测抓帧形状：keys u/s/b/B/a/A，无 e/T/E。
        let f = r#"{"u":400900217,"s":"BTCFDUSD","b":"64845.00","B":"0.04891","a":"64845.01","A":"0.01023"}"#;
        let m: MarketStream = serde_json::from_str(f).unwrap();
        match m {
            MarketStream::BookTicker(bt) => {
                assert_eq!(bt.update_id, 400900217);
                assert_eq!(bt.symbol, "btcfdusd"); // to_lowercase
                assert_eq!(bt.best_bid, "64845.00");
                assert_eq!(bt.best_ask_qty, "0.01023");
            }
            other => panic!("bookTicker frame mis-routed: {other:?}"),
        }
    }

    #[test]
    fn depthupdate_frame_routes_to_eventstream() {
        let f = r#"{"e":"depthUpdate","E":1720000000000,"s":"BTCFDUSD","U":1,"u":2,"b":[["64845.0","0.1"]],"a":[["64845.01","0.2"]]}"#;
        let m: MarketStream = serde_json::from_str(f).unwrap();
        match m {
            MarketStream::EventStream(MarketEventStream::DepthUpdate(d)) => {
                assert_eq!(d.first_update_id, 1);
                assert_eq!(d.last_update_id, 2);
                // QUI-124:借用式 &str 档位解析正确(零 String 分配,值不变)。
                assert_eq!(d.bids, vec![("64845.0", "0.1")]);
                assert_eq!(d.asks, vec![("64845.01", "0.2")]);
                assert_eq!(d.symbol, "btcfdusd"); // symbol 仍 owned + to_lowercase
            }
            other => panic!("depthUpdate frame mis-routed: {other:?}"),
        }
    }

    #[test]
    fn subscribe_result_frame_routes_to_result() {
        let f = r#"{"result":null,"id":"abc123"}"#;
        let m: MarketStream = serde_json::from_str(f).unwrap();
        match m {
            MarketStream::Result(r) => assert_eq!(r.id, "abc123"),
            other => panic!("Result frame mis-routed: {other:?}"),
        }
    }

    // QUI-106 BUG-B 回归:executionReport(maker fill)必须解成 UserStream::EventStream。
    // 修前 UserDataEvent.event 无 #[serde(flatten)] → serde 找 JSON key "event" → 找不到 → decode 失败
    // → fill 事件丢弃 → farm 永远看不到 Filled → timeout/moved → cancel → 幽灵仓位。
    #[test]
    fn execution_report_fill_routes_to_eventstream() {
        use super::{UserEventStream, UserStream};
        // 真实 Binance Spot executionReport(maker fill,LIMIT_MAKER,FILLED)。
        // 字段: e=executionReport,E=event_time,s=symbol,c=clientOrderId,S=side,o=type,
        //       f=tif,q=qty,p=price,P=stopPrice,F=icebergQty,g=orderListId,C=origClientOrderId,
        //       x=execType,X=orderStatus,r=rejectReason,i=orderId,l=lastFillQty,z=cumFillQty,
        //       L=lastFillPrice,n=commission,N=commissionAsset,T=orderTradeTime,t=tradeId,
        //       I=execId,w=onBook,m=isMaker,M=ignore,O=orderCreateTime,Z=cumQuoteQty,
        //       Y=lastQuoteQty,Q=quoteOrderQty,W=workingTime,V=selfTradePreventionMode.
        // ⚠️ 真实消息是 **wrapper**:{"subscriptionId":N,"event":{...executionReport...}}
        // (2026-07-16 实盘抓帧确认;ws-api userDataStream.subscribe 的包裹)。
        let f = r#"{"subscriptionId":0,"event":{
            "e":"executionReport","E":1784165213944,"s":"BTCFDUSD","c":"m1sABC",
            "S":"BUY","o":"LIMIT_MAKER","f":"GTC","q":"0.00008000","p":"64685.01000000",
            "P":"0.00000000","F":"0.00000000","g":-1,"C":"","x":"TRADE","X":"FILLED",
            "r":"NONE","i":25749045654,"l":"0.00008000","z":"0.00008000",
            "L":"64685.01000000","n":"0.00000000","N":"BNB","T":1784165213943,"t":2222896094,
            "I":53679698169,"w":false,"m":true,"M":true,"O":1784165200899,
            "Z":"5.17480080","Y":"5.17480080","Q":"0.00000000","W":1784165200899,"V":"EXPIRE_MAKER"
        }}"#;
        let u: UserStream = serde_json::from_str(f).expect("executionReport must decode");
        match u {
            UserStream::EventStream(ref ev) => match &ev.event {
                UserEventStream::ExecutionReport(report) => {
                    assert_eq!(report.order_id, 25749045654);
                    // order_last_filled_quantity = field "l" = 0.00008
                    assert!((report.order_last_filled_quantity - 0.00008).abs() < 1e-10,
                        "last fill qty wrong: {}", report.order_last_filled_quantity);
                    // order_filled_accumulated_quantity = field "z" = 0.00008
                    assert!((report.order_filled_accumulated_quantity - 0.00008).abs() < 1e-10);
                    assert!(report.is_maker);
                }
                other => panic!("wrong UserEventStream variant: {other:?}"),
            },
            other => panic!("executionReport mis-routed (fill event silently dropped?): {other:?}"),
        }
    }
}
