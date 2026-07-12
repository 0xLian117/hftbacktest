use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use chrono::Utc;
use hftbacktest::types::{OrdType, Side, TimeInForce};
use serde::Deserialize;

use super::msg::{rest, rest::PositionInformationV2};
use crate::{
    binancefutures::{
        BinanceFuturesError,
        msg::{
            rest::{OrderResponse, OrderResponseResult},
            stream::ListenKey,
        },
    },
    utils::sign_hmac_sha256,
};

// QUI-87 客户端限速：博主处方 token bucket（PRACTICES §3.2），vendor 原本裸打 REST。
// 保守 request-rate 兜底（20/s、burst 40，远松于 Binance UM ~40/s IP 权重，不误伤
// dust probe/soak 的正常序列）；per-endpoint 精细 weight + config 化留策略 cadence 清楚后。
const RL_RATE_PER_S: f64 = 20.0;
const RL_BURST: f64 = 40.0;

struct TokenBucket {
    rate: f64,
    capacity: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    fn new(rate: f64, capacity: f64) -> Self {
        Self { rate, capacity, tokens: capacity, last: Instant::now() }
    }

    fn refill(&mut self, now: Instant) {
        let dt = now.duration_since(self.last).as_secs_f64();
        if dt > 0.0 {
            self.tokens = (self.tokens + dt * self.rate).min(self.capacity);
            self.last = now;
        }
    }

    /// 取 1 token；成功返回 None，否则返回还需等待的时长（供调用方 sleep 后重试）。
    fn take(&mut self, now: Instant) -> Option<Duration> {
        self.refill(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            None
        } else {
            Some(Duration::from_secs_f64((1.0 - self.tokens) / self.rate))
        }
    }
}

#[derive(Clone)]
pub struct BinanceFuturesClient {
    client: reqwest::Client,
    url: String,
    api_key: String,
    secret: String,
    // Arc 共享：client 的所有 clone 共用一个桶 = 每 client 一份速率预算（正确）。
    bucket: Arc<Mutex<TokenBucket>>,
}

impl BinanceFuturesClient {
    pub fn new(url: &str, api_key: &str, secret: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.to_string(),
            api_key: api_key.to_string(),
            secret: secret.to_string(),
            bucket: Arc::new(Mutex::new(TokenBucket::new(RL_RATE_PER_S, RL_BURST))),
        }
    }

    /// 限速闸：取一个 token，取不到就 sleep 到有（锁只在 take 时持有、不跨 await）。
    async fn rate_gate(&self) {
        loop {
            let wait = self.bucket.lock().unwrap().take(Instant::now());
            match wait {
                None => return,
                Some(d) => tokio::time::sleep(d).await,
            }
        }
    }

    async fn get_noauth<T: for<'a> Deserialize<'a>>(
        &self,
        path: &str,
        query: String,
    ) -> Result<T, reqwest::Error> {
        self.rate_gate().await;
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
        self.rate_gate().await;
        let time = Utc::now().timestamp_millis() - 1000;
        if !query.is_empty() {
            query.push('&');
        }
        query.push_str("recvWindow=5000&timestamp=");
        query.push_str(&time.to_string());
        let signature = sign_hmac_sha256(&self.secret, &query);
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
        self.rate_gate().await;
        let time = Utc::now().timestamp_millis() - 1000;
        let sign_body = format!("recvWindow=5000&timestamp={time}{body}");
        let signature = sign_hmac_sha256(&self.secret, &sign_body);
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
        self.rate_gate().await;
        let time = Utc::now().timestamp_millis() - 1000;
        let sign_body = format!("recvWindow=5000&timestamp={time}{body}");
        let signature = sign_hmac_sha256(&self.secret, &sign_body);
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
        self.rate_gate().await;
        let time = Utc::now().timestamp_millis() - 1000;
        let sign_body = format!("recvWindow=5000&timestamp={time}{body}");
        let signature = sign_hmac_sha256(&self.secret, &sign_body);
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

    pub async fn start_user_data_stream(&self) -> Result<String, reqwest::Error> {
        let resp: Result<ListenKey, _> = self.post("/fapi/v1/listenKey", String::new()).await;
        resp.map(|v| v.listen_key)
    }

    pub async fn keepalive_user_data_stream(&self) -> Result<(), reqwest::Error> {
        let _: serde_json::Value = self.put("/fapi/v1/listenKey", String::new()).await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
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
    ) -> Result<OrderResponse, BinanceFuturesError> {
        let mut body = String::with_capacity(200);
        body.push_str("newClientOrderId=");
        body.push_str(client_order_id);
        body.push_str("&symbol=");
        body.push_str(symbol);
        body.push_str("&side=");
        body.push_str(side.as_ref());
        body.push_str("&price=");
        body.push_str(&format!("{price:.price_prec$}"));
        body.push_str("&quantity=");
        body.push_str(&format!("{qty:.5}"));
        body.push_str("&type=");
        body.push_str(order_type.as_ref());
        body.push_str("&timeInForce=");
        body.push_str(time_in_force.as_ref());

        let resp: OrderResponseResult = self.post("/fapi/v1/order", body).await?;
        match resp {
            OrderResponseResult::Ok(resp) => Ok(resp),
            OrderResponseResult::Err(resp) => Err(BinanceFuturesError::OrderError {
                code: resp.code,
                msg: resp.msg,
            }),
        }
    }

    pub async fn submit_orders(
        &self,
        orders: Vec<(String, String, Side, f64, usize, f64, OrdType, TimeInForce)>,
    ) -> Result<Vec<Result<OrderResponse, BinanceFuturesError>>, BinanceFuturesError> {
        if orders.len() > 5 {
            return Err(BinanceFuturesError::InvalidRequest);
        }
        let mut body = String::with_capacity(2000 * orders.len());
        body.push_str("{\"batchOrders\":[");
        for (i, order) in orders.iter().enumerate() {
            if i > 0 {
                body.push(',');
            }
            body.push_str("{\"newClientOrderId\":\"");
            body.push_str(&order.0);
            body.push_str("\",\"symbol\":\"");
            body.push_str(&order.1);
            body.push_str("\",\"side\":\"");
            body.push_str(order.2.as_ref());
            body.push_str("\",\"price\":\"");
            body.push_str(&format!("{:.prec$}", order.3, prec = order.4));
            body.push_str("\",\"quantity\":\"");
            body.push_str(&format!("{:.5}", order.5));
            body.push_str("\",\"type\":\"");
            body.push_str(order.6.as_ref());
            body.push_str("\",\"timeInForce\":\"");
            body.push_str(order.7.as_ref());
            body.push_str("\"}");
        }
        body.push_str("]}");

        let resp: Vec<OrderResponseResult> = self.post("/fapi/v1/batchOrders", body).await?;
        Ok(resp
            .into_iter()
            .map(|resp| match resp {
                OrderResponseResult::Ok(resp) => Ok(resp),
                OrderResponseResult::Err(resp) => Err(BinanceFuturesError::OrderError {
                    code: resp.code,
                    msg: resp.msg,
                }),
            })
            .collect())
    }

    pub async fn modify_order(
        &self,
        client_order_id: &str,
        symbol: &str,
        side: Side,
        price: f64,
        price_prec: usize,
        qty: f64,
    ) -> Result<OrderResponse, BinanceFuturesError> {
        let mut body = String::with_capacity(100);
        body.push_str("symbol=");
        body.push_str(symbol);
        body.push_str("&origClientOrderId=");
        body.push_str(client_order_id);
        body.push_str("&side=");
        body.push_str(side.as_ref());
        body.push_str("&price=");
        body.push_str(&format!("{price:.price_prec$}"));
        body.push_str("&quantity=");
        body.push_str(&format!("{qty:.5}"));

        let resp: OrderResponseResult = self.put("/fapi/v1/order", body).await?;
        match resp {
            OrderResponseResult::Ok(resp) => Ok(resp),
            OrderResponseResult::Err(resp) => Err(BinanceFuturesError::OrderError {
                code: resp.code,
                msg: resp.msg,
            }),
        }
    }

    pub async fn cancel_order(
        &self,
        client_order_id: &str,
        symbol: &str,
    ) -> Result<OrderResponse, BinanceFuturesError> {
        let mut body = String::with_capacity(100);
        body.push_str("symbol=");
        body.push_str(symbol);
        body.push_str("&origClientOrderId=");
        body.push_str(client_order_id);

        let resp: OrderResponseResult = self.delete("/fapi/v1/order", body).await?;
        match resp {
            OrderResponseResult::Ok(resp) => Ok(resp),
            OrderResponseResult::Err(resp) => Err(BinanceFuturesError::OrderError {
                code: resp.code,
                msg: resp.msg,
            }),
        }
    }

    pub async fn cancel_orders(
        &self,
        symbol: &str,
        client_order_ids: Vec<String>,
    ) -> Result<Vec<Result<OrderResponse, BinanceFuturesError>>, BinanceFuturesError> {
        if client_order_ids.len() > 10 {
            return Err(BinanceFuturesError::InvalidRequest);
        }
        let mut body = String::with_capacity(100);
        body.push_str("{\"symbol\":\"");
        body.push_str(symbol);
        body.push_str("\",\"origClientOrderIdList\":[");
        for (i, client_order_id) in client_order_ids.iter().enumerate() {
            if i > 0 {
                body.push(',');
            }
            body.push('\"');
            body.push_str(client_order_id);
            body.push('\"');
        }
        body.push_str("]}");
        let resp: Vec<OrderResponseResult> = self.post("/fapi/v1/batchOrders", body).await?;
        Ok(resp
            .into_iter()
            .map(|resp| match resp {
                OrderResponseResult::Ok(resp) => Ok(resp),
                OrderResponseResult::Err(resp) => Err(BinanceFuturesError::OrderError {
                    code: resp.code,
                    msg: resp.msg,
                }),
            })
            .collect())
    }

    pub async fn cancel_all_orders(&self, symbol: &str) -> Result<(), reqwest::Error> {
        let _: serde_json::Value = self
            .delete("/fapi/v1/allOpenOrders", format!("symbol={symbol}"))
            .await?;
        Ok(())
    }

    pub async fn get_position_information(
        &self,
    ) -> Result<Vec<PositionInformationV2>, reqwest::Error> {
        let resp: Vec<PositionInformationV2> =
            self.get("/fapi/v2/positionRisk", String::new()).await?;
        Ok(resp)
    }

    pub async fn get_depth(&self, symbol: &str) -> Result<rest::Depth, reqwest::Error> {
        let resp: rest::Depth = self
            .get_noauth("/fapi/v1/depth", format!("symbol={symbol}&limit=1000"))
            .await?;
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // QUI-87：TokenBucket take(now) 确定性测（now 是参数，无 wall-clock flakiness）。
    #[test]
    fn token_bucket_drains_and_refills() {
        let t0 = Instant::now();
        let mut tb = TokenBucket::new(10.0, 2.0); // 10/s, burst 2
        // last 初始化为 new() 内的 Instant::now()≈t0；用 t0 取两次 → 都成功（满桶 2）
        assert!(tb.take(t0).is_none());
        assert!(tb.take(t0).is_none());
        // 第三次空桶 → 返回等待时长（≈1 token / 10/s = 0.1s 量级）
        let w = tb.take(t0).expect("empty bucket should return wait");
        assert!(w.as_secs_f64() > 0.0 && w.as_secs_f64() <= 0.11, "wait={w:?}");
        // 前进 0.2s → 补 2 token（0.2*10），再取两次成功
        let t1 = t0 + Duration::from_millis(200);
        assert!(tb.take(t1).is_none());
        assert!(tb.take(t1).is_none());
        assert!(tb.take(t1).is_some()); // 又空
    }

    #[test]
    fn token_bucket_caps_at_capacity() {
        let t0 = Instant::now();
        let mut tb = TokenBucket::new(10.0, 2.0);
        // 前进很久也不超过 capacity=2 → 只能连取 2 次
        let t1 = t0 + Duration::from_secs(100);
        assert!(tb.take(t1).is_none());
        assert!(tb.take(t1).is_none());
        assert!(tb.take(t1).is_some());
    }
}
