//! Binance SPOT WebSocket API (ws-api) 下单消息结构。
//!
//! 复用 REST 的 `OrderResponse`/`ErrorResponse`（命令响应同语义，见 `super::rest`），
//! 不造第三态源：ws-api 的成功响应喂给 `OrderManager::update_from_rest`，错误响应经
//! `BinanceSpotError::OrderError{code,msg}`（与 REST 同 variant）喂 `update_submit_fail`/
//! `update_cancel_fail`。
//!
//! **免签**：`session.logon`（user_data_stream.rs）成功后，同一 session 的 `order.place`/
//! `order.cancel` 免带 apiKey/signature（仅 timestamp）。故请求 params 不含 apiKey/signature。

use serde::{Deserialize, Serialize};

use super::rest::{ErrorResponse, OrderResponse};
use crate::binancespot::BinanceSpotError;

/// ws-api 请求信封：`{"id":..,"method":..,"params":{..}}`（镜像 stream::SignRequest）。
#[derive(Debug, Serialize, Clone)]
pub struct WsApiRequest<P> {
    pub id: String,
    pub method: String,
    pub params: P,
}

/// `order.place` 的 params。**免 apiKey/signature**（session 已鉴权）。
///
/// 字段名显式 rename 到 Binance 约定（不用 rename_all：`order_type` 要映射到裸 `type`，
/// 而 `new_client_order_id` 要 camelCase，两者混用）。GTX → `LIMIT_MAKER`（无 timeInForce），
/// 镜像 rest.rs:221-228。
#[derive(Debug, Serialize, Clone)]
pub struct OrderPlaceParams {
    pub symbol: String,
    pub side: String,
    #[serde(rename = "type")]
    pub order_type: String,
    #[serde(rename = "timeInForce", skip_serializing_if = "Option::is_none")]
    pub time_in_force: Option<String>,
    pub quantity: String,
    pub price: String,
    #[serde(rename = "newClientOrderId")]
    pub new_client_order_id: String,
    // FULL：与 REST 同，避免 LIMIT_MAKER 默认 ACK 响应缺字段（QUI-106 幽灵仓位根因）。
    #[serde(rename = "newOrderRespType")]
    pub new_order_resp_type: String,
    pub timestamp: u64,
}

/// `order.cancel` 的 params。**免 apiKey/signature**。
#[derive(Debug, Serialize, Clone)]
pub struct OrderCancelParams {
    pub symbol: String,
    #[serde(rename = "origClientOrderId")]
    pub orig_client_order_id: String,
    pub timestamp: u64,
}

/// ws-api 下/撤单响应。`order.place` 与 `order.cancel` 的成功 `result` 都能解成
/// `OrderResponse`（cancel 响应字段是 OrderResponse 的超集，多出的 origClientOrderId 被
/// 忽略，缺失的 workingTime/fills/origQuoteOrderQty 走 default）→ 统一喂 update_from_rest，
/// 无需区分 place/cancel，也不需要 `From<CancelOrderResponse>`（REST 路才需要，因 rest.rs
/// 的 cancel_order 返回 CancelOrderResponseResult）。`rateLimits` 等额外字段被忽略。
#[derive(Debug, Deserialize, Clone)]
pub struct WsApiOrderResponse {
    #[allow(dead_code)]
    pub id: String,
    pub status: i32,
    #[serde(default)]
    pub result: Option<OrderResponse>,
    #[serde(default)]
    pub error: Option<ErrorResponse>,
}

/// ws-api 下/撤单收敛结果。与 REST 路同构：Ok(OrderResponse) 喂 update_from_rest；
/// Err(OrderError{code,msg}) 喂 update_submit_fail/update_cancel_fail。
#[derive(Debug)]
pub enum WsApiOrderResult {
    Ok(OrderResponse),
    Err(BinanceSpotError),
}

impl WsApiOrderResponse {
    pub fn into_result(self) -> WsApiOrderResult {
        if self.status == 200 {
            if let Some(resp) = self.result {
                return WsApiOrderResult::Ok(resp);
            }
        }
        let (code, msg) = self
            .error
            .map(|e| (e.code, e.msg))
            .unwrap_or((self.status as i64, "ws-api order error (no error body)".to_string()));
        WsApiOrderResult::Err(BinanceSpotError::OrderError { code, msg })
    }
}

/// 仅探测顶层 `id`：ws-api 响应帧（AuthResponse/SubscribeResponse/order response）都带 id；
/// executionReport wrapper（`{"subscriptionId":..,"event":{..}}`）无顶层 id → None。
/// 用于在 UserStream 匹配前判断该帧是否路由到某个在飞的 order 请求（见 wsapi::ws_api_response_id）。
#[derive(Debug, Deserialize)]
pub struct IdProbe {
    pub id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_place_success_decodes_and_into_result_ok() {
        // 真实 order.place 成功响应（resting NEW LIMIT_MAKER，含 rateLimits）。
        let f = r#"{
            "id":"place-abc","status":200,
            "result":{
                "symbol":"BTCFDUSD","orderId":123456,"orderListId":-1,"clientOrderId":"m1sabc",
                "transactTime":1784126821000,"price":"65454.37000000","origQty":"0.00010000",
                "executedQty":"0.00000000","cummulativeQuoteQty":"0.00000000","status":"NEW",
                "timeInForce":"GTC","type":"LIMIT_MAKER","side":"BUY",
                "workingTime":1784126821000,"selfTradePreventionMode":"NONE","fills":[]
            },
            "rateLimits":[{"rateLimitType":"ORDERS","interval":"SECOND","intervalNum":10,"limit":50,"count":1}]
        }"#;
        let resp: WsApiOrderResponse = serde_json::from_str(f).expect("must decode");
        match resp.into_result() {
            WsApiOrderResult::Ok(o) => {
                assert_eq!(o.order_id, 123456);
                assert!((o.price - 65454.37).abs() < 1e-6);
            }
            WsApiOrderResult::Err(e) => panic!("success frame mis-decoded as err: {e:?}"),
        }
    }

    #[test]
    fn order_place_error_decodes_as_orderror() {
        // 免签或余额不足等错误：status != 200 + error{code,msg}。
        let f = r#"{"id":"place-x","status":400,"error":{"code":-2010,"msg":"Account has insufficient balance for requested action."}}"#;
        let resp: WsApiOrderResponse = serde_json::from_str(f).expect("must decode");
        match resp.into_result() {
            WsApiOrderResult::Err(BinanceSpotError::OrderError { code, .. }) => {
                assert_eq!(code, -2010)
            }
            other => panic!("error frame mis-decoded: {other:?}"),
        }
    }

    #[test]
    fn order_cancel_success_result_decodes_into_orderresponse() {
        // cancel 成功响应形状：含 origClientOrderId（OrderResponse 无此字段 → 忽略），
        // 无 workingTime/fills/origQuoteOrderQty（走 default）。锁定「直解 OrderResponse」成立。
        let f = r#"{
            "id":"cancel-abc","status":200,
            "result":{
                "symbol":"BTCFDUSD","origClientOrderId":"m1sabc","orderId":123456,"orderListId":-1,
                "clientOrderId":"cancelXYZ","transactTime":1784126900000,"price":"65454.37000000",
                "origQty":"0.00010000","executedQty":"0.00000000","cummulativeQuoteQty":"0.00000000",
                "status":"CANCELED","timeInForce":"GTC","type":"LIMIT_MAKER","side":"BUY",
                "selfTradePreventionMode":"NONE"
            }
        }"#;
        let resp: WsApiOrderResponse = serde_json::from_str(f).expect("cancel response must decode");
        match resp.into_result() {
            WsApiOrderResult::Ok(o) => {
                assert_eq!(o.order_id, 123456);
                assert_eq!(o.status, hftbacktest::types::Status::Canceled);
                // workingTime 缺失 → default 0（下游 update_from_rest 不读该字段，无害）。
                assert_eq!(o.working_time, 0);
            }
            WsApiOrderResult::Err(e) => panic!("cancel success mis-decoded as err: {e:?}"),
        }
    }

    #[test]
    fn serialize_limit_maker_omits_tif_and_auth_fields() {
        // GTX → LIMIT_MAKER（无 timeInForce）；免签（无 apiKey/signature）；type rename；FULL。
        let req = WsApiRequest {
            id: "place-1".to_string(),
            method: "order.place".to_string(),
            params: OrderPlaceParams {
                symbol: "BTCFDUSD".to_string(),
                side: "SELL".to_string(),
                order_type: "LIMIT_MAKER".to_string(),
                time_in_force: None,
                quantity: "0.00010".to_string(),
                price: "65454.37".to_string(),
                new_client_order_id: "m1sabc".to_string(),
                new_order_resp_type: "FULL".to_string(),
                timestamp: 1784126821000,
            },
        };
        let s = serde_json::to_string(&req).unwrap();
        assert!(s.contains(r#""method":"order.place""#));
        assert!(s.contains(r#""type":"LIMIT_MAKER""#));
        assert!(s.contains(r#""newOrderRespType":"FULL""#));
        assert!(s.contains(r#""newClientOrderId":"m1sabc""#));
        assert!(!s.contains("timeInForce"), "LIMIT_MAKER must omit timeInForce: {s}");
        assert!(!s.contains("apiKey"), "logon session → no apiKey: {s}");
        assert!(!s.contains("signature"), "logon session → no signature: {s}");
    }

    #[test]
    fn serialize_limit_gtc_includes_tif() {
        let req = WsApiRequest {
            id: "place-2".to_string(),
            method: "order.place".to_string(),
            params: OrderPlaceParams {
                symbol: "BTCFDUSD".to_string(),
                side: "BUY".to_string(),
                order_type: "LIMIT".to_string(),
                time_in_force: Some("GTC".to_string()),
                quantity: "0.00010".to_string(),
                price: "65454.37".to_string(),
                new_client_order_id: "m1sxyz".to_string(),
                new_order_resp_type: "FULL".to_string(),
                timestamp: 1784126821000,
            },
        };
        let s = serde_json::to_string(&req).unwrap();
        assert!(s.contains(r#""timeInForce":"GTC""#), "non-GTX must include timeInForce: {s}");
        assert!(s.contains(r#""type":"LIMIT""#));
    }

    #[test]
    fn serialize_cancel_uses_orig_client_order_id() {
        let req = WsApiRequest {
            id: "cancel-1".to_string(),
            method: "order.cancel".to_string(),
            params: OrderCancelParams {
                symbol: "BTCFDUSD".to_string(),
                orig_client_order_id: "m1sabc".to_string(),
                timestamp: 1784126900000,
            },
        };
        let s = serde_json::to_string(&req).unwrap();
        assert!(s.contains(r#""method":"order.cancel""#));
        assert!(s.contains(r#""origClientOrderId":"m1sabc""#));
        assert!(!s.contains("apiKey"));
        assert!(!s.contains("signature"));
    }
}
