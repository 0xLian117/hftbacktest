use hftbacktest::types::{OrdType, Side, Status, TimeInForce};
use serde::{Deserialize, Serialize};

use super::{from_str_to_side, from_str_to_status, from_str_to_tif, from_str_to_type};
use crate::utils::{from_str_to_f64, to_lowercase};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Depth {
    pub last_update_id: i64,
    pub asks: Vec<(String, String)>,
    pub bids: Vec<(String, String)>,
}

#[derive(Deserialize, Debug)]
#[serde(untagged)]
pub enum OrderResponseResult {
    Ok(OrderResponse),
    Err(ErrorResponse),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderResponse {
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    pub order_id: u64,
    pub order_list_id: i64,
    pub client_order_id: String,
    pub transact_time: i64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub orig_qty: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub executed_qty: f64,
    // Binance SPOT 新单 POST /api/v3/order 响应**不含** origQuoteOrderQty(它只在 GET 查单响应里)。
    // 无 default → 整个 OrderResponse untagged 解码失败(「did not match any variant」)→ 被当提交失败
    // → Status::Expired,而订单其实已在交易所 resting → farm 瞎记账 + 幽灵仓位(QUI-106 实测 7 笔 maker 成交)。
    // default+deserialize_with:字段缺失用 f64 默认 0.0,存在时正常解析。
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub orig_quote_order_qty: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub cummulative_quote_qty: f64,
    #[serde(deserialize_with = "from_str_to_status")]
    pub status: Status,
    #[serde(deserialize_with = "from_str_to_tif")]
    pub time_in_force: TimeInForce,
    #[serde(rename = "type")]
    #[serde(deserialize_with = "from_str_to_type")]
    pub order_type: OrdType,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    // 下面三个防御性加 default:不同 respType/版本可能省略(workingTime 早期缺失、STP 模式后加)。
    #[serde(default)]
    pub working_time: i64,
    #[serde(default)]
    pub self_trade_prevention_mode: String,
    #[serde(default)]
    pub fills: Vec<Fill>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ErrorResponse {
    pub code: i64,
    pub msg: String,
}

#[derive(Debug, Deserialize, Clone)]
// untagged: Binance 取消响应是扁平 JSON(成功→CancelOrderResponse 形状,失败→ErrorResponse 形状)。
// 无 untagged 时 serde 用外部标记({"ok":{...}})匹配,Binance 不这样→成功撤单解码失败→
// cancel 被当作错误→update_cancel_fail(非-2011)→order 状态不变→farm 误认单仍 New→幽灵单。
#[serde(untagged)]
pub enum CancelOrderResponseResult {
    Ok(CancelOrderResponse),
    Err(ErrorResponse),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CancelOrderResponse {
    #[serde(deserialize_with = "to_lowercase")]
    pub symbol: String,
    pub order_id: u64,
    pub order_list_id: i64,
    pub orig_client_order_id: String,
    pub client_order_id: String,
    pub transact_time: i64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub orig_qty: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub executed_qty: f64,
    // 与 OrderResponse 对齐:撤单响应也可能省略这些字段(版本/状态而异),缺则整个 untagged 解码失败
    // → 成功撤单被当错误 → farm 以为单还在 → 孤儿(Codex P1)。
    #[serde(default, deserialize_with = "from_str_to_f64")]
    pub orig_quote_order_qty: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub cummulative_quote_qty: f64,
    #[serde(deserialize_with = "from_str_to_status")]
    pub status: Status,
    #[serde(deserialize_with = "from_str_to_tif")]
    pub time_in_force: TimeInForce,
    #[serde(rename = "type")]
    #[serde(deserialize_with = "from_str_to_type")]
    pub order_type: OrdType,
    #[serde(deserialize_with = "from_str_to_side")]
    pub side: Side,
    #[serde(default)]
    pub self_trade_prevention_mode: String,
}

impl From<CancelOrderResponse> for OrderResponse {
    fn from(order: CancelOrderResponse) -> Self {
        Self {
            symbol: order.symbol,
            order_id: order.order_id,
            order_list_id: order.order_list_id,
            client_order_id: order.client_order_id,
            transact_time: order.transact_time,
            price: order.price,
            orig_qty: order.orig_qty,
            executed_qty: order.executed_qty,
            orig_quote_order_qty: order.orig_quote_order_qty,
            cummulative_quote_qty: order.cummulative_quote_qty,
            status: order.status,
            time_in_force: order.time_in_force,
            order_type: order.order_type,
            side: order.side,
            working_time: order.transact_time,
            self_trade_prevention_mode: order.self_trade_prevention_mode,
            fills: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Fill {
    #[serde(deserialize_with = "from_str_to_f64")]
    pub price: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub qty: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub commission: f64,
    #[serde(deserialize_with = "to_lowercase")]
    pub commission_asset: String,
    pub trade_id: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfomation {
    pub maker_commission: u64,
    pub taker_commission: u64,
    pub buyer_commission: u64,
    pub seller_commission: u64,
    pub commission_rates: CommissionRates,
    pub can_trade: bool,
    pub can_withdraw: bool,
    pub can_deposit: bool,
    pub brokered: bool,
    pub require_self_trade_prevention: bool,
    pub prevent_sor: bool,
    pub update_time: i64,
    pub account_type: String, // Consider using an enum if account types are fixed
    pub balances: Vec<BalanceEntry>,
    pub permissions: Vec<String>, // Consider using an enum if permissions are fixed
    pub uid: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CommissionRates {
    #[serde(deserialize_with = "from_str_to_f64")]
    pub maker: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub taker: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub buyer: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub seller: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BalanceEntry {
    #[serde(deserialize_with = "to_lowercase")]
    pub asset: String,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub free: f64,
    #[serde(deserialize_with = "from_str_to_f64")]
    pub locked: f64,
}

#[cfg(test)]
mod tests {
    use super::OrderResponseResult;

    // QUI-106 回归:Binance SPOT 新单 POST /api/v3/order 的 FULL 响应**不含 origQuoteOrderQty**
    // (它只在 GET 查单响应里)。之前 OrderResponse 无 default → 整个 untagged 解码失败 → 订单被误当
    // 提交失败(Expired)、实际在交易所 resting → farm 幽灵仓位。此测锁住:真实新单响应必须解成 Ok(NEW)。
    #[test]
    fn spot_new_order_full_response_without_orig_quote_order_qty_decodes_ok() {
        // 真实形状(resting LIMIT_MAKER,未成交,fills=[]),故意省略 origQuoteOrderQty。
        let json = r#"{
            "symbol":"BTCFDUSD","orderId":123456,"orderListId":-1,"clientOrderId":"m1sabc",
            "transactTime":1784126821000,"price":"65454.37000000","origQty":"0.00010000",
            "executedQty":"0.00000000","cummulativeQuoteQty":"0.00000000","status":"NEW",
            "timeInForce":"GTC","type":"LIMIT_MAKER","side":"BUY",
            "workingTime":1784126821000,"selfTradePreventionMode":"NONE","fills":[]
        }"#;
        match serde_json::from_str::<OrderResponseResult>(json).expect("must decode") {
            OrderResponseResult::Ok(o) => {
                assert_eq!(o.order_id, 123456);
                assert!((o.orig_quote_order_qty - 0.0).abs() < 1e-12); // 缺失 → default 0.0
                assert!((o.price - 65454.37).abs() < 1e-6);
            }
            OrderResponseResult::Err(e) => panic!("resting NEW order mis-decoded as error: {e:?}"),
        }
    }

    #[test]
    fn spot_order_error_response_still_decodes_as_err() {
        let json = r#"{"code":-2010,"msg":"Account has insufficient balance for requested action."}"#;
        match serde_json::from_str::<OrderResponseResult>(json).expect("must decode") {
            OrderResponseResult::Err(e) => assert_eq!(e.code, -2010),
            OrderResponseResult::Ok(_) => panic!("error response mis-decoded as Ok"),
        }
    }

    // QUI-106 BUG-A 추가 회귀: LIMIT_MAKER POST 응답 기본값은 ACK(5필드)이므로
    // newOrderRespType=FULL을 명시적으로 요청해야 FULL 응답이 옴.
    // ACK 응답은 OrderResponse 필수 필드(price/origQty/status 등)가 없어 decode 실패.
    // 이 테스트는 ACK 형상이 Ok로 해석되면 안 됨을 검증한다.
    // (실 운영에서는 rest.rs가 &newOrderRespType=FULL을 추가하므로 이 형상이 올 일 없음.)
    #[test]
    fn spot_ack_response_without_full_fields_does_not_decode_as_ok() {
        // ACK 형상: 5필드만 있음 (Binance LIMIT_MAKER 기본 응답)
        let ack_json = r#"{
            "symbol":"BTCFDUSD","orderId":25749045654,"orderListId":-1,
            "clientOrderId":"m1sabc","transactTime":1784163492705
        }"#;
        // untagged enum: OrderResponse 해석 실패 → ErrorResponse 해석 시도 → code/msg 없음 → 실패
        // 전체 decode 실패여야 함 (둘 다 아님)
        let result = serde_json::from_str::<OrderResponseResult>(ack_json);
        assert!(result.is_err(), "ACK-only response must NOT decode as OrderResponseResult Ok or Err — it should fail. Got: {:?}", result);
    }

    // BUG-A 회귀: FULL 응답 with immediate fill (filled taker, fills populated) decodes Ok.
    #[test]
    fn spot_full_response_with_immediate_fill_decodes_ok() {
        let json = r#"{
            "symbol":"BTCFDUSD","orderId":25749045654,"orderListId":-1,"clientOrderId":"m1sabc",
            "transactTime":1784163492705,"price":"64699.03000000","origQty":"0.00008000",
            "executedQty":"0.00008000","cummulativeQuoteQty":"5.17592240","status":"FILLED",
            "timeInForce":"GTC","type":"LIMIT_MAKER","side":"BUY",
            "workingTime":1784163492705,"selfTradePreventionMode":"EXPIRE_MAKER",
            "fills":[{"price":"64699.03000000","qty":"0.00008000","commission":"0.00000008","commissionAsset":"BTC","tradeId":999}]
        }"#;
        match serde_json::from_str::<OrderResponseResult>(json).expect("FULL FILLED response must decode") {
            OrderResponseResult::Ok(o) => {
                assert_eq!(o.order_id, 25749045654);
                assert!((o.executed_qty - 0.00008).abs() < 1e-10);
                assert_eq!(o.fills.len(), 1);
                assert!((o.fills[0].qty - 0.00008).abs() < 1e-10);
            }
            OrderResponseResult::Err(e) => panic!("FULL FILLED response decoded as error: {e:?}"),
        }
    }
}
