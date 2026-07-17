use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
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
    // QUI-108:reconcile 触发流(每次 register 广播,含重注册)。取代旧的 symbol_rx——本流只做启动对账,
    // 订阅就绪后按此重跑 sweep+report。market-data 订阅另走 symbol_tx。
    reconcile_rx: Receiver<String>,
    ws_api: SharedWsApi,
}

impl UserDataStream {
    pub fn new(
        client: BinanceSpotClient,
        ev_tx: UnboundedSender<PublishEvent>,
        order_manager: SharedOrderManager,
        symbols: SharedSymbolSet,
        reconcile_rx: Receiver<String>,
        ws_api: SharedWsApi,
    ) -> Self {
        Self {
            symbols,
            client,
            ev_tx,
            order_manager,
            reconcile_rx,
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
            // Fetches the initial states such as positions and open orders (per-asset balances).
            // QUI-108:cancel_all + Reconciled 信号**不在此**发——移到订阅(executionReport 通道)就绪后
            // (见下方 SubscribeResponse / reconcile_rx),否则可能在 exec 通道 live 前放行 farm → 漏 fill。
            if let Err(error) =
                get_position_information(client.clone(), symbols, ev_tx.clone()).await
            {
                error!(?error, "Couldn't get position information.");
            }
        });

        // QUI-108 启动对账状态:订阅(executionReport 通道)就绪前不发 Reconciled(P0)。就绪时对每个已注册
        // symbol 跑一次 reconcile;之后 reconcile_rx 的每次触发(farm 重启重注册)也逐个响应(P1#2)。inflight
        // 去重防同一 symbol 并发 sweep(订阅快照 + reconcile_rx 同时命中)提前放行 gate(P1#3)。
        let mut subscribed = false;
        let inflight: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

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
                        // caller 已超时放弃（receiver 关闭）→ 不发出，避免 writer 卡顿恢复后传输「已弃」
                        // 订单。no-re-place 设计下 caller 未走 REST，故此单直接不下，caller 下轮重报价。
                        if cmd.resp_tx.is_closed() {
                            continue;
                        }
                        // 先登记 pending 再发；write 失败则 return（触发重连），pending 随之析构
                        // → 该单 oneshot Canceled → caller 落 REST（若尚未发出）。
                        pending.insert(cmd.id, cmd.resp_tx);
                        write.send(Message::Text(cmd.text.into())).await?;
                    }
                    // None: 所有 sender 已 drop（本循环仍持有 cmd_tx，故循环存活期不会发生）。
                }
                msg = self.reconcile_rx.recv() => {
                    match msg {
                        Ok(symbol) => {
                            // reconcile 触发(每次 register 广播,含 farm 重启的重注册)。仅在订阅就绪后动作
                            // (P0);未就绪时忽略——该 symbol 已在 self.symbols,订阅成功时统一 reconcile。
                            if subscribed {
                                spawn_reconcile(
                                    inflight.clone(),
                                    self.client.clone(),
                                    symbol,
                                    self.order_manager.clone(),
                                    self.ev_tx.clone(),
                                );
                            }
                        }
                        Err(RecvError::Closed) => {
                            return Ok(());
                        }
                        Err(RecvError::Lagged(num)) => {
                            error!("{num} reconcile triggers were missed.");
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
                                debug!(?result, "session.logon response received.");
                                if result.status == 200 {
                                    // logon 成功 → 请求 executionReport 订阅。**下单 handle 尚不发布**
                                    // ——要等订阅成功、对账通道就绪后才发布（见 SubscribeResponse）。
                                    write.send(Message::Text(
                                        serde_json::to_string(&UserStreamSubscribeRequest {
                                            id: generate_rand_string(16),
                                            method: "userDataStream.subscribe".to_string(),
                                        })
                                        .unwrap().into(),
                                    )).await?;
                                } else {
                                    // logon 失败（key 无效/签名错等）→ 重连（ClearOnDrop 清 slot）。
                                    error!(?result, "session.logon failed; reconnecting.");
                                    return Err(BinanceSpotError::ConnectionInterrupted);
                                }
                            }
                            Ok(UserStream::SubscribeResponse(resp)) => {
                                debug!(?resp, "userDataStream.subscribe response received.");
                                if resp.status == 200 {
                                    // 订阅成功 → executionReport 通道就绪 → **此刻**才发布下单 handle，
                                    // 保证任何经 WS 下的单都有对账通道（超时/ambiguous 单靠它对账）。
                                    *self.ws_api.lock().unwrap() =
                                        Some(WsApiHandle { cmd_tx: cmd_tx.clone() });
                                    // QUI-108:通道就绪后才跑启动对账(cancel_all + openOrders 收敛 + 发
                                    // Reconciled)。对当前所有已注册 symbol 各跑一次;inflight 去重(reconcile_rx
                                    // 可能同时命中同一 symbol)。之后重注册经 reconcile_rx 分支响应。
                                    subscribed = true;
                                    let registered: Vec<String> =
                                        self.symbols.lock().unwrap().iter().cloned().collect();
                                    for symbol in registered {
                                        spawn_reconcile(
                                            inflight.clone(),
                                            self.client.clone(),
                                            symbol,
                                            self.order_manager.clone(),
                                            self.ev_tx.clone(),
                                        );
                                    }
                                } else {
                                    error!(?resp, "userDataStream.subscribe failed; reconnecting.");
                                    return Err(BinanceSpotError::ConnectionInterrupted);
                                }
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

/// QUI-108:去重地 spawn 一次 per-symbol reconcile。`inflight` 集合防同一 symbol 并发 sweep——
/// 订阅成功时对快照全量 spawn,同一时刻 reconcile_rx 可能也命中同一 symbol;两个 sweep 并发时,一个
/// 的 DELETE 未落地另一个已 emit `Reconciled(0)` → gate 提前放行(P1#3)。已在 reconcile 中 → 跳过。
fn spawn_reconcile(
    inflight: Arc<Mutex<HashSet<String>>>,
    client: BinanceSpotClient,
    symbol: String,
    order_manager: SharedOrderManager,
    ev_tx: UnboundedSender<PublishEvent>,
) {
    if !inflight.lock().unwrap().insert(symbol.clone()) {
        return; // 该 symbol 的 reconcile 已在进行 → 不重复 spawn
    }
    tokio::spawn(async move {
        sweep_and_report(client, symbol.clone(), order_manager, ev_tx).await;
        inflight.lock().unwrap().remove(&symbol);
    });
}

/// QUI-108 启动对账（per symbol）：`cancel_all` 扫净 → openOrders 收敛复查 → 发
/// `LiveEvent::Reconciled { symbol, open_orders }`。farm 首单 gate 阻塞轮询 `reconcile_status`,
/// 据此把连接器的 cancel-all 串行化到下单之前（解 QUI-109 r3 cancel_all-vs-新单竞态）。
///
/// **仅在 executionReport 订阅就绪后**由 `spawn_reconcile` 触发（订阅成功时对已注册 symbol 全量 +
/// 之后每次 register 经 reconcile_rx）——保证 Reconciled(=可下单) 时对账通道已 live,不漏补发单的 fill；
/// 且 farm 重启撞存活 connector 也能经重注册拿到新信号（P0/P1#2）。
///
/// **fail-closed**:cancel_all 失败不阻断(仍复查+发信号,count 反映真实残留);openOrders 复查
/// 全部失败 → 发 `u32::MAX` 哨兵 → farm loud-exit(绝不 default 0 假 clean)。
async fn sweep_and_report(
    client: BinanceSpotClient,
    symbol: String,
    order_manager: SharedOrderManager,
    ev_tx: UnboundedSender<PublishEvent>,
) {
    if let Err(error) = cancel_all(client.clone(), symbol.clone(), order_manager, ev_tx.clone()).await {
        error!(?error, %symbol, "startup reconcile: cancel_all failed — proceeding to recheck (fail-closed).");
    }
    // 收敛复查:DELETE allOpenOrders 后 openOrders 有亚秒最终一致性延迟(镜像 livebot run_reconcile
    // 的 5×300ms:先睡后查)。取最后一次成功读数;全部失败 → u32::MAX(fail-closed)。
    let mut open_orders = u32::MAX;
    for attempt in 0..5 {
        time::sleep(Duration::from_millis(300)).await;
        match client.get_open_orders(&symbol).await {
            Ok(orders) => {
                open_orders = orders.len() as u32;
                if open_orders == 0 {
                    break;
                }
            }
            Err(error) => {
                warn!(?error, %symbol, attempt, "startup reconcile: openOrders query failed (retrying).");
            }
        }
    }
    if open_orders != 0 {
        error!(%symbol, open_orders, "startup reconcile: venue NOT clean after cancel-all (farm will refuse to trade).");
    }
    let _ = ev_tx.send(PublishEvent::LiveEvent(LiveEvent::Reconciled { symbol, open_orders }));
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
