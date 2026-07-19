use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::{live::ipc::TO_ALL, prelude::*};
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
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
        msg::{
            rest,
            stream,
            stream::{MarketEventStream, MarketStream},
        },
        rest::BinanceSpotClient,
    },
    connector::PublishEvent,
    utils::{generate_rand_string, parse_depth, parse_px_qty_tup},
};

/// 每 symbol 的 L2 depth 同步态（QUI-106，Binance 现货官方 local-order-book 算法）。
/// 现货 depth diff 有 U(first_update_id)/u(last_update_id)，**无 futures 的 pu** → 用 U/u 连续性。
enum DepthSync {
    /// 已请求 REST 快照，其间到达的 diff 先缓冲；快照到达后 drop-stale + align + 回放。
    AwaitingSnapshot { buffer: Vec<stream::Depth> },
    /// 已对齐；稳态要求下一条 diff 的 U == prev_u + 1，违反则 resync。
    Synced { prev_u: i64 },
}

pub struct MarketDataStream {
    client: BinanceSpotClient,
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
    // QUI-113：重连须重放已注册 symbol。broadcast `symbol_rx` 只送**订阅之后**的注册,重连新建的 stream
    // 拿到的新 Receiver 收不到过去广播的 symbol → 不重放就会重连后静默失订阅、在死盘口报价。connect() 用此
    // 权威 set 重订阅全部(register() 在 mod.rs 维护)。镜像 binancefutures/market_data_stream.rs。
    symbols: Arc<Mutex<HashSet<String>>>,
    depth_sync: HashMap<String, DepthSync>,
    resync_count: HashMap<String, u64>,
    rest_tx: UnboundedSender<(String, rest::Depth)>,
    rest_rx: UnboundedReceiver<(String, rest::Depth)>,
}

impl MarketDataStream {
    pub fn new(
        client: BinanceSpotClient,
        ev_tx: UnboundedSender<PublishEvent>,
        symbol_rx: Receiver<String>,
        symbols: Arc<Mutex<HashSet<String>>>,
    ) -> Self {
        let (rest_tx, rest_rx) = unbounded_channel::<(String, rest::Depth)>();
        Self {
            client,
            ev_tx,
            symbol_rx,
            symbols,
            depth_sync: Default::default(),
            resync_count: Default::default(),
            rest_tx,
            rest_rx,
        }
    }

    /// 异步取 REST depth 快照 → 回 rest_rx → process_snapshot 对齐。
    fn request_snapshot(&self, symbol: &str) {
        let client_ = self.client.clone();
        let symbol = symbol.to_string();
        let rest_tx = self.rest_tx.clone();
        tokio::spawn(async move {
            match client_.get_depth(&symbol).await {
                Ok(depth) => {
                    let _ = rest_tx.send((symbol, depth));
                }
                Err(error) => {
                    error!(?error, %symbol, "Couldn't get the market depth via REST.");
                }
            }
        });
    }

    /// 发一批 L2 档位事件（BatchStart..BatchEnd）。diff 用 `event_time*1e6`，快照体用 local now（GAP#1）。
    fn emit_depth(
        &self,
        symbol: &str,
        bids: Vec<(String, String)>,
        asks: Vec<(String, String)>,
        exch_ts: i64,
    ) {
        match parse_depth(bids, asks) {
            Ok((bids, asks)) => {
                self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();
                for (px, qty) in bids {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: symbol.to_string(),
                            event: Event {
                                ev: LOCAL_BID_DEPTH_EVENT,
                                exch_ts,
                                local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }
                for (px, qty) in asks {
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: symbol.to_string(),
                            event: Event {
                                ev: LOCAL_ASK_DEPTH_EVENT,
                                exch_ts,
                                local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }
                self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
            }
            Err(error) => {
                error!(?error, "Couldn't parse depth levels.");
            }
        }
    }

    /// 快照前清两侧（GAP#3，mirror futures）：快照只带非零档，重同步时旧档否则会残留（幽灵档 QUI-79）。
    fn clear_both(&self, symbol: &str, exch_ts: i64) {
        for clear_ev in [LOCAL_BID_DEPTH_CLEAR_EVENT, LOCAL_ASK_DEPTH_CLEAR_EVENT] {
            self.ev_tx
                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol: symbol.to_string(),
                    event: Event {
                        ev: clear_ev,
                        exch_ts,
                        local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                        order_id: 0,
                        px: f64::NAN,
                        qty: 0.0,
                        ival: 0,
                        fval: 0.0,
                    },
                }))
                .unwrap();
        }
    }

    /// bookTicker → 两条 BBO Feed（QUI-106，bot 存进 Instrument.last_bbo，不进 L2）。
    /// 现货 bookTicker 无交易所时戳 → exch_ts=local_ts=now（feed_latency local 侧仍准，farm feed-age guard 照常）。
    /// **BatchStart..BatchEnd 包住 bid+ask**：一帧的 bid/ask 原子可见——bot 在 batch 内处理完两条才在
    /// BatchEnd 返回 elapse（bot.rs elapse_：batch_mode 里 MarketFeed 不早返回），消除「新 bid+旧 ask」
    /// 半更新窗口（否则急涨/急跌时 bbo 会瞬时内部交叉，farm_guard 会拒该报价空跑一轮，QUI-106 validate 实测）。
    /// 不 u 去重（覆盖写，同 QUI-86）。
    fn process_book_ticker(&self, data: stream::BookTicker) {
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        let sides = [
            (LOCAL_BID_DEPTH_BBO_EVENT, data.best_bid, data.best_bid_qty),
            (LOCAL_ASK_DEPTH_BBO_EVENT, data.best_ask, data.best_ask_qty),
        ];
        // 先解析两侧;任一侧解析失败则整帧丢弃(不发半帧,避免只更一侧造成的交叉)。
        let mut parsed = [(0u64, 0.0f64, 0.0f64); 2];
        for (i, (ev_kind, px_s, qty_s)) in sides.into_iter().enumerate() {
            match parse_px_qty_tup(px_s, qty_s) {
                Ok((px, qty)) => parsed[i] = (ev_kind, px, qty),
                Err(e) => {
                    error!(error = ?e, "Couldn't parse spot bookTicker px/qty — drop frame.");
                    return;
                }
            }
        }
        self.ev_tx.send(PublishEvent::BatchStart(TO_ALL)).unwrap();
        for (ev_kind, px, qty) in parsed {
            self.ev_tx
                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol: data.symbol.clone(),
                    event: Event {
                        ev: ev_kind,
                        exch_ts: now,
                        local_ts: now,
                        order_id: 0,
                        px,
                        qty,
                        ival: 0,
                        fval: 0.0,
                    },
                }))
                .unwrap();
        }
        self.ev_tx.send(PublishEvent::BatchEnd(TO_ALL)).unwrap();
    }

    fn process_message(&mut self, stream: MarketEventStream) {
        match stream {
            MarketEventStream::DepthUpdate(data) => {
                let sym = data.symbol.clone();
                // 决策阶段:不跨 self 方法调用持有 depth_sync 的可变借用。
                // emit=Some(bids,asks,exch_ts) 稳态发档;resync=Some(diff,is_gap) 需(重)取快照。
                let mut emit: Option<(Vec<(String, String)>, Vec<(String, String)>, i64)> = None;
                let mut resync: Option<(stream::Depth, bool)> = None;
                match self.depth_sync.get_mut(&sym) {
                    Some(DepthSync::AwaitingSnapshot { buffer }) => buffer.push(data),
                    Some(DepthSync::Synced { prev_u }) => {
                        if data.first_update_id == *prev_u + 1 {
                            *prev_u = data.last_update_id; // 持锁时推进
                            emit = Some((data.bids, data.asks, data.event_time * 1_000_000));
                        } else {
                            warn!(%sym, expected = *prev_u + 1, got = data.first_update_id, "spot depth gap — resync");
                            resync = Some((data, true));
                        }
                    }
                    None => resync = Some((data, false)), // 首个 diff:缓冲 + 取快照(非 gap,不计数)
                }
                if let Some((bids, asks, exch_ts)) = emit {
                    self.emit_depth(&sym, bids, asks, exch_ts);
                }
                if let Some((diff, is_gap)) = resync {
                    if is_gap {
                        *self.resync_count.entry(sym.clone()).or_insert(0) += 1;
                    }
                    self.depth_sync
                        .insert(sym.clone(), DepthSync::AwaitingSnapshot { buffer: vec![diff] });
                    self.request_snapshot(&sym);
                }
            }
            MarketEventStream::Trade(data) => match parse_px_qty_tup(data.price, data.quantity) {
                Ok((px, qty)) => {
                    if data.ignore {
                        return;
                    }
                    self.ev_tx
                        .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                            symbol: data.symbol,
                            event: Event {
                                ev: {
                                    if data.is_market_maker {
                                        LOCAL_SELL_TRADE_EVENT
                                    } else {
                                        LOCAL_BUY_TRADE_EVENT
                                    }
                                },
                                exch_ts: data.event_time * 1_000_000,
                                local_ts: Utc::now().timestamp_nanos_opt().unwrap(),
                                order_id: 0,
                                px,
                                qty,
                                ival: 0,
                                fval: 0.0,
                            },
                        }))
                        .unwrap();
                }
                Err(e) => {
                    error!(error = ?e, "Couldn't parse trade stream.");
                }
            },
            _ => unreachable!(),
        }
    }

    /// REST 快照到达:Binance 现货官方 local-order-book 对齐(drop-stale + first-align + U/u 连续回放)。
    fn process_snapshot(&mut self, symbol: String, data: rest::Depth) {
        // 仅当正等待本 symbol 快照时才处理并取出缓冲;否则丢弃(raced/重复 REST 回复)。
        let buffer = match self.depth_sync.get_mut(&symbol) {
            Some(DepthSync::AwaitingSnapshot { buffer }) => std::mem::take(buffer),
            _ => {
                debug!(%symbol, "snapshot arrived but not awaiting — drop (raced resync)");
                return;
            }
        };
        let snap_id = data.last_update_id;
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        // 先 CLEAR 两侧(GAP#3),再发快照体。快照体 exch_ts=local now(GAP#1:现货 REST Depth 无交易所时戳)。
        self.clear_both(&symbol, now);
        self.emit_depth(&symbol, data.bids, data.asks, now);

        // 回放缓冲 diff:drop u<=lastUpdateId;首个 survivor U<=lastUpdateId+1<=u;之后 U==prev_u+1。
        // ⚠️ 回放 diff 各用自己的 event_time*1e6(现货 diff 有 E),不套快照的 local-now。
        let mut prev_u = snap_id;
        let mut aligned = false;
        for d in buffer {
            if d.last_update_id <= snap_id {
                continue; // 快照已覆盖
            }
            let ok = if !aligned {
                let first_ok = d.first_update_id <= snap_id + 1 && snap_id + 1 <= d.last_update_id;
                if first_ok {
                    aligned = true;
                }
                first_ok
            } else {
                d.first_update_id == prev_u + 1
            };
            if !ok {
                warn!(%symbol, snap_id, U = d.first_update_id, u = d.last_update_id, prev_u, aligned, "snapshot/buffer gap — resync");
                *self.resync_count.entry(symbol.clone()).or_insert(0) += 1;
                // 丢弃已 desync 的缓冲,起空缓冲重取快照;后续 live diff 会重新缓冲。
                self.depth_sync
                    .insert(symbol.clone(), DepthSync::AwaitingSnapshot { buffer: Vec::new() });
                self.request_snapshot(&symbol);
                return;
            }
            let exch_ts = d.event_time * 1_000_000;
            prev_u = d.last_update_id;
            self.emit_depth(&symbol, d.bids, d.asks, exch_ts);
        }
        self.depth_sync.insert(symbol, DepthSync::Synced { prev_u });
    }

    pub async fn connect(&mut self, url: &str) -> Result<(), BinanceSpotError> {
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();
        let mut ping_checker = time::interval(Duration::from_secs(10));
        let mut last_ping = Instant::now();

        // QUI-113：(重)连时重订阅全部已注册 symbol。新 stream 的 broadcast Receiver 收不到过去的注册,
        // 不重放会重连后静默失订阅 → 死盘口报价。先 collect 释锁再 await(锁不跨 await)。新 stream 的
        // depth_sync 为空 → 首条 diff 触发 request_snapshot → process_snapshot 先 CLEAR 两侧再贴快照 → 得
        // 新鲜非交叉 book。初连时与 symbol_rx broadcast 对同一 symbol 各发一次 SUBSCRIBE,Binance 幂等无害。
        let registered: Vec<String> = self.symbols.lock().unwrap().iter().cloned().collect();
        for symbol in registered {
            let id = generate_rand_string(16);
            write.send(Message::Text(format!(r#"{{
                "method": "SUBSCRIBE",
                "params": [
                    "{symbol}@trade",
                    "{symbol}@depth@100ms",
                    "{symbol}@bookTicker"
                ],
                "id": "{id}"
            }}"#).into())).await?;
        }

        loop {
            select! {
                Some((symbol, data)) = self.rest_rx.recv() => {
                    self.process_snapshot(symbol, data);
                }
                _ = ping_checker.tick() => {
                    if last_ping.elapsed() > Duration::from_secs(300) {
                        warn!("Ping timeout.");
                        return Err(BinanceSpotError::ConnectionInterrupted);
                    }
                }
                msg = self.symbol_rx.recv() => match msg {
                    Ok(symbol) => {
                        let id = generate_rand_string(16);
                        write.send(Message::Text(format!(r#"{{
                            "method": "SUBSCRIBE",
                            "params": [
                                "{symbol}@trade",
                                "{symbol}@depth@100ms",
                                "{symbol}@bookTicker"
                            ],
                            "id": "{id}"
                        }}"#).into())).await?;
                    }
                    Err(RecvError::Closed) => {
                        return Ok(());
                    }
                    Err(RecvError::Lagged(num)) => {
                        error!("{num} subscription requests were missed.");
                    }
                },
                message = read.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        match serde_json::from_str::<MarketStream>(&text) {
                            Ok(MarketStream::EventStream(stream)) => {
                                self.process_message(stream);
                            }
                            Ok(MarketStream::BookTicker(bt)) => {
                                self.process_book_ticker(bt);
                            }
                            Ok(MarketStream::Result(result)) => {
                                debug!(?result, "Subscription request response is received.");
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
