use chrono::Utc;
use hftbacktest::types::{OrdType, Side, TimeInForce};
use serde::Deserialize;

use super::msg::rest;
use crate::{
    binancespot::{
        BinanceSpotError,
        msg::rest::{
            AccountInfomation,
            CancelOrderResponse,
            CancelOrderResponseResult,
            OrderResponse,
            OrderResponseResult,
        },
    },
    utils::sign_ed25519,
};

#[derive(Clone)]
pub struct BinanceSpotClient {
    client: reqwest::Client,
    url: String,
    pub api_key: String,
    pub secret: String,
    // pub
}

impl BinanceSpotClient {
    pub fn new(url: &str, api_key: &str, secret: &str) -> Self {
        let client = reqwest::Client::new();
        Self {
            client,
            url: url.to_string(),
            api_key: api_key.to_string(),
            secret: secret.to_string(),
        }
    }
    async fn get_noauth<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        query: String,
    ) -> Result<T, reqwest::Error> {
        let resp = self
            .client
            .get(format!("{}{}?{}", self.url, path, query))
            .header("Accept", "application/json")
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }
    async fn get<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        mut query: String,
    ) -> Result<T, reqwest::Error> {
        let time = Utc::now().timestamp_millis() - 1000;
        if !query.is_empty() {
            query.push('&');
        }
        query.push_str("recvWindow=5000&timestamp=");
        query.push_str(&time.to_string());
        let signature = sign_ed25519(&self.secret, &query);
        let resp = self
            .client
            .get(format!(
                "{}{}?{}&signature={}",
                self.url, path, query, signature
            ))
            .header("Accept", "application/json")
            .header("X-MBX-APIKEY", &self.api_key)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    async fn put<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        body: String,
    ) -> Result<T, reqwest::Error> {
        let time = Utc::now().timestamp_millis() - 1000;
        let sign_body = format!("recvWindow=5000&timestamp={time}{body}");
        let signature = sign_ed25519(&self.secret, &sign_body);
        let resp = self
            .client
            .put(format!(
                "{}{}?recvWindow=5000&timestamp={}&signature={}",
                self.url, path, time, signature
            ))
            .header("Accept", "application/json")
            .header("X-MBX-APIKEY", &self.api_key)
            .body(body)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    async fn post<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        body: String,
    ) -> Result<T, reqwest::Error> {
        let time = Utc::now().timestamp_millis() - 1000;
        let sign_body = format!("recvWindow=5000&timestamp={time}{body}");
        let signature = sign_ed25519(&self.secret, &sign_body);
        let resp = self
            .client
            .post(format!(
                "{}{}?recvWindow=5000&timestamp={}&signature={}",
                self.url, path, time, signature
            ))
            .header("Accept", "application/json")
            .header("X-MBX-APIKEY", &self.api_key)
            .body(body)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    async fn delete<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        body: String,
    ) -> Result<T, reqwest::Error> {
        let time = Utc::now().timestamp_millis() - 1000;
        let sign_body = format!("recvWindow=5000&timestamp={time}{body}");
        let signature = sign_ed25519(&self.secret, &sign_body);
        let resp = self
            .client
            .delete(format!(
                "{}{}?recvWindow=5000&timestamp={}&signature={}",
                self.url, path, time, signature
            ))
            .header("Accept", "application/json")
            .header("X-MBX-APIKEY", &self.api_key)
            .body(body)
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    pub async fn get_depth(&self, symbol: &str) -> Result<rest::Depth, reqwest::Error> {
        // SPOT depth REST 要大写 symbol(小写 → -1100,错误体无 lastUpdateId → decode 失败)。
        // 同 F3 大写坑,延伸到 market-data 路径(QUI-105 只修了下单路径;旧「natural refresh」不依赖
        // 快照故没暴露,新序列化 buffer-until-snapshot 触发了它)。用规范 /api/v3/depth。
        let resp: rest::Depth = self
            .get_noauth("/api/v3/depth", format!("symbol={}&limit=1000", symbol.to_uppercase()))
            .await?;
        Ok(resp)
    }

    pub async fn get_account_information(&self) -> Result<AccountInfomation, reqwest::Error> {
        let resp: AccountInfomation = self.get("/api/v3/account", String::new()).await?;
        Ok(resp)
    }

    pub async fn cancel_all_orders(&self, symbol: &str) -> Result<(), reqwest::Error> {
        // Binance SPOT 严格要求 symbol 大写(-1100),不同于容忍小写的 futures。stream 订阅用小写。
        let _: serde_json::Value = self
            .delete("/api/v3/openOrders", format!("symbol={}", symbol.to_uppercase()))
            .await?;
        Ok(())
    }

    pub async fn cancel_order(
        &self,
        client_order_id: &str,
        symbol: &str,
    ) -> Result<CancelOrderResponse, BinanceSpotError> {
        let mut body = String::with_capacity(100);
        body.push_str("symbol=");
        body.push_str(&symbol.to_uppercase()); // SPOT 要大写(-1100)
        body.push_str("&origClientOrderId=");
        body.push_str(client_order_id);

        let resp: CancelOrderResponseResult = self.delete("/api/v3/order", body).await?;
        match resp {
            CancelOrderResponseResult::Ok(resp) => Ok(resp),
            CancelOrderResponseResult::Err(resp) => Err(BinanceSpotError::OrderError {
                code: resp.code,
                msg: resp.msg,
            }),
        }
    }

    pub async fn submit_order(
        &self,
        client_order_id: &str,
        symbol: &str,
        side: Side,
        price: f64,
        price_prec: usize,
        qty: f64,
        order_type: OrdType,
        time_in_force: TimeInForce,
    ) -> Result<OrderResponse, BinanceSpotError> {
        let mut body = String::with_capacity(200);
        body.push_str("newClientOrderId=");
        body.push_str(client_order_id);
        body.push_str("&symbol=");
        body.push_str(&symbol.to_uppercase()); // SPOT 要大写(-1100)
        body.push_str("&side=");
        body.push_str(side.as_ref());
        body.push_str("&price=");
        body.push_str(&format!("{price:.price_prec$}"));
        body.push_str("&quantity=");
        body.push_str(&format!("{qty:.5}"));
        // Binance SPOT post-only = order type LIMIT_MAKER(无 timeInForce)。现货不认 GTX
        // (-1115);GTX 是 futures 的 post-only timeInForce。其余 TIF 照常 type+timeInForce。
        if matches!(time_in_force, TimeInForce::GTX) {
            body.push_str("&type=LIMIT_MAKER");
        } else {
            body.push_str("&type=");
            body.push_str(order_type.as_ref());
            body.push_str("&timeInForce=");
            body.push_str(time_in_force.as_ref());
        }
        // Binance SPOT LIMIT_MAKER 默认响应类型是 ACK(只含 5 字段:symbol/orderId/orderListId/clientOrderId/transactTime)。
        // 不显式请求 FULL → OrderResponse 的 price/origQty/status 等必填字段缺失 → untagged decode 失败
        // → "data did not match any variant" → 订单被误当 Expired → farm 幽灵仓位。
        body.push_str("&newOrderRespType=FULL");

        let resp: OrderResponseResult = self.post("/api/v3/order", body).await?;
        match resp {
            OrderResponseResult::Ok(resp) => Ok(resp),
            OrderResponseResult::Err(resp) => Err(BinanceSpotError::OrderError {
                code: resp.code,
                msg: resp.msg,
            }),
        }
    }
}
