use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use futures_util::{SinkExt, StreamExt};
use hftbacktest::prelude::*;
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::{self, UnboundedSender},
        oneshot,
    },
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{debug, error, warn};

use crate::{
    binancespot::{
        BinanceSpotError,
        SharedSymbolSet,
        msg::{
            stream::{
                SignParams,
                SignRequest,
                UserEventStream,
                UserStream,
                UserStreamSubscribeRequest,
            },
            wsapi::{WsApiOrderResponse, WsApiOrderResult},
        },
        ordermanager::SharedOrderManager,
        rest::BinanceSpotClient,
        wsapi::{ClearOnDrop, SharedWsApi, WsApiCommand, WsApiHandle, ws_api_response_id},
    },
    connector::PublishEvent,
    utils::{generate_rand_string, get_timestamp, sign_ed25519},
};

pub struct UserDataStream {
    symbols: SharedSymbolSet,
    client: BinanceSpotClient,
    ev_tx: UnboundedSender<PublishEvent>,
    order_manager: SharedOrderManager,
    symbol_rx: Receiver<String>,
    ws_api: SharedWsApi,
}

impl UserDataStream {
    pub fn new(
        client: BinanceSpotClient,
        ev_tx: UnboundedSender<PublishEvent>,
        order_manager: SharedOrderManager,
        symbols: SharedSymbolSet,
        symbol_rx: Receiver<String>,
        ws_api: SharedWsApi,
    ) -> Self {
        Self {
            symbols,
            client,
            ev_tx,
            order_manager,
            symbol_rx,
            ws_api,
        }
    }

    // pub async fn get_listen_key(&self) -> Result<String, BinanceSpotError> {
    //     Ok(self.client.start_user_data_stream().await?)
    // }

    fn process_message(&self, stream: UserEventStream) -> Result<(), BinanceSpotError> {
        match stream {
            UserEventStream::OutboundAccountPosition(data) => {
                let event_time = data.event_time;
                for balance in data.balances {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Position {
                            symbol: balance.asset,
                            qty: balance.free,
                            exch_ts: event_time * 1_000_000,
                        }))
                        .unwrap();
                }
            }
            UserEventStream::BalanceUpdate(_data) => {}
            UserEventStream::ExecutionReport(data) => {
                match self.order_manager.lock().unwrap().update_from_ws(&data) {
                    Ok(Some(order)) => {
                        self.ev_tx
                            .send(PublishEvent::LiveEvent(LiveEvent::Order {
                                symbol: data.symbol.clone(),
                                order,
                            }))
                            .unwrap();
                    }
                    Ok(None) => {
                        // order已经删除
                    }
                    Err(BinanceSpotError::PrefixUnmatched) => {
                        // order不是当前connector创建的
                    }
                    Err(error) => {
                        error!(
                            ?error,
                            ?data,
                            "Couldn't update the order from OrderTradeUpdate message."
                        );
                    }
                }
            }
            UserEventStream::ListStatus(_data) => {}
        }
        Ok(())
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), BinanceSpotError> {
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut ping_checker = time::interval(Duration::from_secs(10));

        let symbols: HashSet<_> = self.symbols.lock().unwrap().iter().cloned().collect();
        let client = self.client.clone();
        let order_manager = self.order_manager.clone();
        let ev_tx = self.ev_tx.clone();
        let mut last_ping = Instant::now();

        // ws-api 下单通道：本循环是唯一 writer；submit/cancel 经 cmd_rx 汇入，响应按 id 关联回
        // pending 里各自的 oneshot。ClearOnDrop 覆盖所有退出路径：清 shared slot（新单直接 REST）；
        // 循环返回时 pending 自动析构 → 在飞请求的 oneshot 全 Canceled → 各 caller 落 REST。
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<WsApiCommand>();
        let mut pending: HashMap<String, oneshot::Sender<WsApiOrderResult>> = HashMap::new();
        let _clear_ws_api = ClearOnDrop(self.ws_api.clone());

        let mut req = SignRequest {
            id: generate_rand_string(16),
            method: "session.logon".to_string(),
            params: SignParams {
                api_key: self.client.api_key.clone(),
                signature: None,
                timestamp: get_timestamp(),
            },
        };
        // session.logon 签名负载 = params(字母序,仅 apiKey+timestamp),不是整个 SignRequest。
        // 之前 serde_qs(整个 req) 含 id/method/params[...] 嵌套 → Binance 拒登录 (ws 1008)。
        let payload = format!(
            "apiKey={}&timestamp={}",
            req.params.api_key, req.params.timestamp
        );
        let signature = sign_ed25519(&self.client.secret, &payload);
        req.params.signature = Some(signature);
        let _ = write
            .send(Message::Text(serde_json::to_string(&req).unwrap().into()))
            .await;

        tokio::spawn(async move {
            // Cancel all orders before connecting to the stream in order to start with the
            // clean state.
            for symbol in &symbols {
                if let Err(error) = cancel_all(
                    client.clone(),
                    symbol.clone(),
                    order_manager.clone(),
                    ev_tx.clone(),
                )
                .await
                {
                    error!(?error, %symbol, "Couldn't cancel all orders.");
                }
            }

            // Fetches the initial states such as positions and open orders.
            if let Err(error) =
                get_position_information(client.clone(), symbols, ev_tx.clone()).await
            {
                error!(?error, "Couldn't get position information.");
            }
        });

        loop {
            select! {
                _ = ping_checker.tick() => {
                    if last_ping.elapsed() > Duration::from_secs(300) {
                        warn!("Ping timeout.");
                        return Err(BinanceSpotError::ConnectionInterrupted);
                    }
                    // 清理已超时的在飞请求：caller 超时后 drop 了 receiver（is_closed），其 sender 若
                    // 留在 pending（响应始终未到）会在长连接上无界累积。每 10s 扫一次兜底（正常路径下
                    // 响应到达时即被 remove）。
                    pending.retain(|_, resp_tx| !resp_tx.is_closed());
                }
                cmd = cmd_rx.recv() => {
                    if let Some(cmd) = cmd {
                        // 先登记 pending 再发；write 失败则 return（触发重连），pending 随之析构
                        // → 该单 oneshot Canceled → caller 落 REST。
                        pending.insert(cmd.id, cmd.resp_tx);
                        write.send(Message::Text(cmd.text.into())).await?;
                    }
                    // None: 所有 sender 已 drop（本循环仍持有 cmd_tx，故循环存活期不会发生）。
                }
                msg = self.symbol_rx.recv() => {
                    match msg {
                        Ok(symbol) => {
                            let client = self.client.clone();
                            let order_manager = self.order_manager.clone();
                            let ev_tx = self.ev_tx.clone();

                            tokio::spawn(async move {
                                if let Err(error) = cancel_all(
                                    client.clone(),
                                    symbol.clone(),
                                    order_manager.clone(),
                                    ev_tx.clone()
                                ).await {
                                    error!(?error, %symbol, "Couldn't cancel all orders.");
                                }
                            });
                        }
                        Err(RecvError::Closed) => {
                            return Ok(());
                        }
                        Err(RecvError::Lagged(num)) => {
                            error!("{num} subscription requests were missed.");
                        }
                    }
                }
                message = read.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        // ws-api 下/撤单响应优先：顶层 id 命中 pending → 解 WsApiOrderResponse 路由回
                        // caller。未命中（logon/subscribe 响应、executionReport wrapper 无顶层 id）→
                        // fall through 到 UserStream 匹配（R3 消歧：logon/subscribe id 从不进 pending）。
                        if let Some(id) = ws_api_response_id(&text, &pending) {
                            match serde_json::from_str::<WsApiOrderResponse>(&text) {
                                Ok(resp) => {
                                    if let Some(resp_tx) = pending.remove(&id) {
                                        let _ = resp_tx.send(resp.into_result());
                                    }
                                }
                                Err(error) => {
                                    error!(?error, %text, "Couldn't parse ws-api order response.");
                                    // 丢弃 pending 条目 → oneshot Canceled → caller 落 REST。
                                    pending.remove(&id);
                                }
                            }
                            continue;
                        }
                        match serde_json::from_str::<UserStream>(&text) {
                            Ok(UserStream::EventStream(stream)) => {
                                self.process_message(stream.event)?;
                            }
                            Ok(UserStream::AuthResponse(result)) => {
                                debug!(?result, "Subscription request response is received.");
                                if result.status == 200 {
                                    write.send(Message::Text(
                                        serde_json::to_string(&UserStreamSubscribeRequest {
                                            id: generate_rand_string(16),
                                            method: "userDataStream.subscribe".to_string(),
                                        })
                                        .unwrap().into(),
                                    )).await?;
                                    // logon 成功 → ws-api 下单可用：填 shared slot，submit/cancel 从此走 WS。
                                    *self.ws_api.lock().unwrap() =
                                        Some(WsApiHandle { cmd_tx: cmd_tx.clone() });
                                }
                            }
                            Ok(UserStream::SubscribeResponse(resp)) => {
                                debug!(?resp, "Subscription request error response is received.");
                            }
                            Err(error) => {
                                error!(?error, %text, "Couldn't parse Stream.");
                            }
                        }
                    }
                    Some(Ok(Message::Ping(data))) => {
                        write.send(Message::Pong(data)).await?;
                        last_ping = Instant::now();
                    }
                    Some(Ok(Message::Close(close_frame))) => {
                        return Err(BinanceSpotError::ConnectionAbort(
                            close_frame.map(|f| f.to_string()).unwrap_or(String::new())
                        ));
                    }
                    Some(Ok(Message::Binary(_)))
                    | Some(Ok(Message::Frame(_)))
                    | Some(Ok(Message::Pong(_))) => {}
                    Some(Err(error)) => {
                        return Err(BinanceSpotError::from(error));
                    }
                    None => {
                        return Err(BinanceSpotError::ConnectionInterrupted);
                    }
                }
            }
        }
    }
}

pub async fn cancel_all(
    client: BinanceSpotClient,
    symbol: String,
    order_manager: SharedOrderManager,
    ev_tx: UnboundedSender<PublishEvent>,
) -> Result<(), BinanceSpotError> {
    // todo: rate-limit throttling.
    client.cancel_all_orders(&symbol).await?;
    let orders = order_manager.lock().unwrap().cancel_all_from_rest(&symbol);
    for order in orders {
        ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Order {
                symbol: symbol.clone(),
                order,
            }))
            .unwrap();
    }
    Ok(())
}

pub async fn get_position_information(
    client: BinanceSpotClient,
    mut symbols: HashSet<String>,
    ev_tx: UnboundedSender<PublishEvent>,
) -> Result<(), BinanceSpotError> {
    // todo: rate-limit throttling.
    let account_infomation = client.get_account_information().await?;
    let exch_ts = account_infomation.update_time * 1_000_000;
    account_infomation.balances.into_iter().for_each(|balance| {
        symbols.remove(&balance.asset);
        ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Position {
                symbol: balance.asset,
                qty: balance.free,
                exch_ts,
            }))
            .unwrap();
    });
    for symbol in symbols {
        ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Position {
                symbol,
                qty: 0.0,
                exch_ts: 0,
            }))
            .unwrap();
    }
    Ok(())
}
