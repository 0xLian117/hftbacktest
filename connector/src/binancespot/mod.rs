mod market_data_stream;
mod msg;
mod ordermanager;
mod rest;
mod user_data_stream;
mod wsapi;

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use hftbacktest::{
    prelude::get_precision,
    types::{ErrorKind, LiveError, LiveEvent, Order, Status, TimeInForce, Value},
};
use serde::Deserialize;
use thiserror::Error;
use tokio::{
    sync::{broadcast, broadcast::Sender, mpsc::UnboundedSender, oneshot},
    time::timeout,
};
use tokio_tungstenite::tungstenite;
use tracing::{debug, error, warn};

use crate::{
    binancespot::{
        msg::{
            rest::OrderResponse,
            wsapi::{OrderCancelParams, OrderPlaceParams, WsApiOrderResult, WsApiRequest},
        },
        ordermanager::{OrderManager, SharedOrderManager},
        rest::BinanceSpotClient,
        wsapi::{SharedWsApi, WsApiCommand},
    },
    connector::{Connector, ConnectorBuilder, GetOrders, PublishEvent},
    utils::{ExponentialBackoff, Retry, generate_rand_string, get_timestamp},
};

/// ws-api 下/撤单的 fallback 触发阈值（非正常路延迟）：超过则落 REST。远短于重连 backoff，
/// 死 socket 快速 fallback。⚠️ 到 500ms 时该单 event2order 已远超盈利门（MEM ~5ms）→ 补发的
/// REST 单在 fresh 语义上已失效；连接器只保证「不双单」，弃单交上层 latency guard（QUI-110）。
const WS_API_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Error, Debug)]
pub enum BinanceSpotError {
    #[error("InstrumentNotFound")]
    InstrumentNotFound,
    #[error("InvalidRequest")]
    InvalidRequest,
    #[error("ListenKeyExpired")]
    ListenKeyExpired,
    #[error("ConnectionInterrupted")]
    ConnectionInterrupted,
    #[error("ConnectionAbort: {0}")]
    ConnectionAbort(String),
    #[error("ReqError: {0:?}")]
    ReqError(#[from] reqwest::Error),
    #[error("OrderError: {code} - {msg})")]
    OrderError { code: i64, msg: String },
    #[error("PrefixUnmatched")]
    PrefixUnmatched,
    #[error("OrderNotFound")]
    OrderNotFound,
    #[error("Tunstenite: {0:?}")]
    Tunstenite(#[from] tungstenite::Error),
    #[error("Config: {0:?}")]
    Config(#[from] toml::de::Error),
}

impl From<BinanceSpotError> for Value {
    fn from(value: BinanceSpotError) -> Value {
        match value {
            BinanceSpotError::InstrumentNotFound => Value::String(value.to_string()),
            BinanceSpotError::InvalidRequest => Value::String(value.to_string()),
            BinanceSpotError::ReqError(error) => {
                let mut map = HashMap::new();
                if let Some(code) = error.status() {
                    map.insert("status_code".to_string(), Value::String(code.to_string()));
                }
                map.insert("msg".to_string(), Value::String(error.to_string()));
                Value::Map(map)
            }
            BinanceSpotError::OrderError { code, msg } => Value::Map({
                let mut map = HashMap::new();
                map.insert("code".to_string(), Value::Int(code));
                map.insert("msg".to_string(), Value::String(msg));
                map
            }),
            BinanceSpotError::Tunstenite(error) => Value::String(format!("{error}")),
            BinanceSpotError::ListenKeyExpired => Value::String(value.to_string()),
            BinanceSpotError::ConnectionInterrupted => Value::String(value.to_string()),
            BinanceSpotError::ConnectionAbort(_) => Value::String(value.to_string()),
            BinanceSpotError::Config(_) => Value::String(value.to_string()),
            BinanceSpotError::PrefixUnmatched => Value::String(value.to_string()),
            BinanceSpotError::OrderNotFound => Value::String(value.to_string()),
        }
    }
}

#[derive(Deserialize)]
pub struct Config {
    stream_url: String,
    api_url: String,
    #[serde(default)]
    order_prefix: String,
    #[serde(default)]
    api_key: String,
    #[serde(default)]
    secret: String,
    #[serde(default)]
    ws_api_url: String,
}

type SharedSymbolSet = Arc<Mutex<HashSet<String>>>;

/// A connector for Binance Spot.
pub struct BinanceSpot {
    config: Config,
    symbols: SharedSymbolSet,
    order_manager: SharedOrderManager,
    client: BinanceSpotClient,
    symbol_tx: Sender<String>,
    // ws-api 下单 session slot：None = 未 logon（下单走 REST），Some = 可走 WS。由 user_data_stream
    // 的 connect() 在 session.logon 成功后填入，退出时 ClearOnDrop 清回 None。
    ws_api: SharedWsApi,
}

impl BinanceSpot {
    pub fn connect_market_data_stream(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        let base_url = self.config.stream_url.clone();
        let client = self.client.clone();
        let symbol_tx = self.symbol_tx.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: BinanceSpotError| {
                    error!(
                        ?error,
                        "An error occurred in the market data stream connection."
                    );
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.into(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = market_data_stream::MarketDataStream::new(
                        client.clone(),
                        ev_tx.clone(),
                        symbol_tx.subscribe(),
                    );
                    debug!("Connecting to the market data stream...");
                    stream.connect(&base_url).await?;
                    debug!("The market data stream connection is permanently closed.");
                    Ok(())
                })
                .await;
        });
    }

    pub fn connect_user_data_stream(&self, ev_tx: UnboundedSender<PublishEvent>) {
        let base_url = self.config.ws_api_url.clone();
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();
        let instruments = self.symbols.clone();
        let symbol_tx = self.symbol_tx.clone();
        let ws_api = self.ws_api.clone();

        tokio::spawn(async move {
            let _ = Retry::new(ExponentialBackoff::default())
                .error_handler(|error: BinanceSpotError| {
                    error!(
                        ?error,
                        "An error occurred in the user data stream connection."
                    );
                    ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                            ErrorKind::ConnectionInterrupted,
                            error.into(),
                        ))))
                        .unwrap();
                    Ok(())
                })
                .retry(|| async {
                    let mut stream = user_data_stream::UserDataStream::new(
                        client.clone(),
                        ev_tx.clone(),
                        order_manager.clone(),
                        instruments.clone(),
                        symbol_tx.subscribe(),
                        ws_api.clone(),
                    );

                    debug!("Requesting the listen key for the user data stream...");
                    // let listen_key = stream.get_listen_key().await?;

                    debug!("Connecting to the user data stream...");
                    stream.connect(&base_url).await?;
                    debug!("The user data stream connection is permanently closed.");
                    Ok(())
                })
                .await;
        });
    }
}

impl ConnectorBuilder for BinanceSpot {
    type Error = BinanceSpotError;

    fn build_from(config: &str) -> Result<Self, Self::Error> {
        let config: Config = toml::from_str(config)?;

        let order_manager = Arc::new(Mutex::new(OrderManager::new(&config.order_prefix)));
        let client = BinanceSpotClient::new(&config.api_url, &config.api_key, &config.secret);
        let (symbol_tx, _) = broadcast::channel(500);

        Ok(BinanceSpot {
            config,
            symbols: Default::default(),
            order_manager,
            client,
            symbol_tx,
            ws_api: Arc::new(Mutex::new(None)),
        })
    }
}

impl Connector for BinanceSpot {
    fn register(&mut self, symbol: String) {
        // Binance futures symbols must be lowercase to subscribe to the WebSocket stream.
        if symbol.to_lowercase() != symbol {
            error!("Binance Futures symbol must be lowercase.");
        }
        let symbol = symbol.to_lowercase();
        let mut symbols = self.symbols.lock().unwrap();
        if !symbols.contains(&symbol) {
            symbols.insert(symbol.clone());
            self.symbol_tx.send(symbol).unwrap();
        }
    }

    fn order_manager(&self) -> Arc<Mutex<dyn GetOrders + Send + 'static>> {
        self.order_manager.clone()
    }

    fn run(&mut self, ev_tx: UnboundedSender<PublishEvent>) {
        self.connect_market_data_stream(ev_tx.clone());
        // Connects to the user stream only if the API key and secret are provided.
        if !self.config.api_key.is_empty() && !self.config.secret.is_empty() {
            self.connect_user_data_stream(ev_tx.clone());
        }
    }

    fn submit(&self, symbol: String, mut order: Order, tx: UnboundedSender<PublishEvent>) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();
        let ws_api = self.ws_api.clone();

        tokio::spawn(async move {
            let client_order_id = order_manager
                .lock()
                .unwrap()
                .prepare_client_order_id(symbol.clone(), order.clone());

            let client_order_id = match client_order_id {
                Some(id) => id,
                None => {
                    warn!(
                        ?order,
                        "Coincidentally, creates a duplicated client order id. \
                        This order request will be expired."
                    );
                    order.req = Status::None;
                    order.status = Status::Expired;
                    tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                        .unwrap();
                    return;
                }
            };

            // ws-api 优先：session up 则走 WS，超时/断线落 REST（同 client_order_id → R1：
            // Binance 拒重复 coid，最坏一单 + 一条无害 dup error）。
            let handle = ws_api.lock().unwrap().clone();
            if let Some(handle) = handle {
                let price = order.price_tick as f64 * order.tick_size;
                let price_prec = get_precision(order.tick_size);
                let side: &str = order.side.as_ref();
                // GTX → LIMIT_MAKER（无 timeInForce），镜像 rest.rs / submit_order。
                let (order_type, time_in_force): (String, Option<String>) =
                    if matches!(order.time_in_force, TimeInForce::GTX) {
                        ("LIMIT_MAKER".to_string(), None)
                    } else {
                        let ot: &str = order.order_type.as_ref();
                        let tif: &str = order.time_in_force.as_ref();
                        (ot.to_string(), Some(tif.to_string()))
                    };
                let id = generate_rand_string(16);
                let req = WsApiRequest {
                    id: id.clone(),
                    method: "order.place".to_string(),
                    params: OrderPlaceParams {
                        symbol: symbol.to_uppercase(), // SPOT 要大写(-1100)
                        side: side.to_string(),
                        order_type,
                        time_in_force,
                        quantity: format!("{:.5}", order.qty),
                        price: format!("{price:.price_prec$}"),
                        new_client_order_id: client_order_id.clone(),
                        new_order_resp_type: "FULL".to_string(),
                        timestamp: get_timestamp(),
                    },
                };
                let text = serde_json::to_string(&req).unwrap();
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(WsApiCommand { id, text, resp_tx })
                    .is_ok()
                {
                    match timeout(WS_API_TIMEOUT, resp_rx).await {
                        Ok(Ok(WsApiOrderResult::Ok(resp))) => {
                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_from_rest(&client_order_id, &resp)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                                    .unwrap();
                            }
                            return;
                        }
                        Ok(Ok(WsApiOrderResult::Err(error))) => {
                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_submit_fail(&client_order_id, &error)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                                    .unwrap();
                            }
                            tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                ErrorKind::OrderError,
                                error.into(),
                            ))))
                            .unwrap();
                            return;
                        }
                        // timeout（Err(Elapsed)）或 session 中途掉线（Ok(Err(Canceled)））→ 落 REST。
                        _ => {
                            warn!(
                                %client_order_id,
                                "ws-api order.place timed out or session dropped; falling back to REST."
                            );
                        }
                    }
                }
                // cmd_tx.send 失败（循环已退出）→ 落 REST。
            }

            submit_via_rest(&client, &order_manager, &symbol, client_order_id, order, &tx).await;
        });
    }

    fn cancel(&self, symbol: String, order: Order, tx: UnboundedSender<PublishEvent>) {
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();
        let ws_api = self.ws_api.clone();

        tokio::spawn(async move {
            let client_order_id = order_manager
                .lock()
                .unwrap()
                .get_client_order_id(&symbol, order.order_id);

            let client_order_id = match client_order_id {
                Some(id) => id,
                None => {
                    warn!(
                        order_id = order.order_id,
                        "client_order_id corresponding to order_id is not found; \
                        this may be due to the order already being canceled or filled."
                    );
                    return;
                }
            };

            // ws-api 优先；超时/断线落 REST（cancel 幂等：同 origClientOrderId 重发无害）。
            let handle = ws_api.lock().unwrap().clone();
            if let Some(handle) = handle {
                let id = generate_rand_string(16);
                let req = WsApiRequest {
                    id: id.clone(),
                    method: "order.cancel".to_string(),
                    params: OrderCancelParams {
                        symbol: symbol.to_uppercase(), // SPOT 要大写(-1100)
                        orig_client_order_id: client_order_id.clone(),
                        timestamp: get_timestamp(),
                    },
                };
                let text = serde_json::to_string(&req).unwrap();
                let (resp_tx, resp_rx) = oneshot::channel();
                if handle
                    .cmd_tx
                    .send(WsApiCommand { id, text, resp_tx })
                    .is_ok()
                {
                    match timeout(WS_API_TIMEOUT, resp_rx).await {
                        // cancel 成功响应直解 OrderResponse（字段是超集）→ update_from_rest。
                        Ok(Ok(WsApiOrderResult::Ok(resp))) => {
                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_from_rest(&client_order_id, &resp)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                                    .unwrap();
                            }
                            return;
                        }
                        Ok(Ok(WsApiOrderResult::Err(error))) => {
                            if let Some(order) = order_manager
                                .lock()
                                .unwrap()
                                .update_cancel_fail(&client_order_id, &error)
                            {
                                tx.send(PublishEvent::LiveEvent(LiveEvent::Order { symbol, order }))
                                    .unwrap();
                            }
                            tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                                ErrorKind::OrderError,
                                error.into(),
                            ))))
                            .unwrap();
                            return;
                        }
                        _ => {
                            warn!(
                                %client_order_id,
                                "ws-api order.cancel timed out or session dropped; falling back to REST."
                            );
                        }
                    }
                }
            }

            cancel_via_rest(&client, &order_manager, &symbol, client_order_id, &tx).await;
        });
    }
}

/// REST 单笔下单路径（ws-api 未就绪 / 超时 / 断线时的 fallback）。抽自原 submit 内联体，
/// 行为不变：成功 → update_from_rest；失败 → update_submit_fail + Error event。
async fn submit_via_rest(
    client: &BinanceSpotClient,
    order_manager: &SharedOrderManager,
    symbol: &str,
    client_order_id: String,
    order: Order,
    tx: &UnboundedSender<PublishEvent>,
) {
    let result = client
        .submit_order(
            &client_order_id,
            symbol,
            order.side,
            order.price_tick as f64 * order.tick_size,
            get_precision(order.tick_size),
            order.qty,
            order.order_type,
            order.time_in_force,
        )
        .await;
    match result {
        Ok(resp) => {
            if let Some(order) = order_manager
                .lock()
                .unwrap()
                .update_from_rest(&client_order_id, &resp)
            {
                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                    symbol: symbol.to_string(),
                    order,
                }))
                .unwrap();
            }
        }
        Err(error) => {
            if let Some(order) = order_manager
                .lock()
                .unwrap()
                .update_submit_fail(&client_order_id, &error)
            {
                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                    symbol: symbol.to_string(),
                    order,
                }))
                .unwrap();
            }
            tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                ErrorKind::OrderError,
                error.into(),
            ))))
            .unwrap();
        }
    }
}

/// REST 单笔撤单路径（ws-api fallback）。抽自原 cancel 内联体，行为不变。
async fn cancel_via_rest(
    client: &BinanceSpotClient,
    order_manager: &SharedOrderManager,
    symbol: &str,
    client_order_id: String,
    tx: &UnboundedSender<PublishEvent>,
) {
    let result = client.cancel_order(&client_order_id, symbol).await;
    match result {
        Ok(resp) => {
            let cancel_order_resp = OrderResponse::from(resp);
            if let Some(order) = order_manager
                .lock()
                .unwrap()
                .update_from_rest(&client_order_id, &cancel_order_resp)
            {
                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                    symbol: symbol.to_string(),
                    order,
                }))
                .unwrap();
            }
        }
        Err(error) => {
            if let Some(order) = order_manager
                .lock()
                .unwrap()
                .update_cancel_fail(&client_order_id, &error)
            {
                tx.send(PublishEvent::LiveEvent(LiveEvent::Order {
                    symbol: symbol.to_string(),
                    order,
                }))
                .unwrap();
            }
            tx.send(PublishEvent::LiveEvent(LiveEvent::Error(LiveError::with(
                ErrorKind::OrderError,
                error.into(),
            ))))
            .unwrap();
        }
    }
}
