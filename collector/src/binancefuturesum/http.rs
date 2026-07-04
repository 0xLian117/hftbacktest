use std::{
    io,
    io::ErrorKind,
    time::{Duration, Instant},
};

use anyhow::Error;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    select,
    sync::mpsc::{UnboundedSender, unbounded_channel},
    time::interval,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Bytes, Message, Utf8Bytes, client::IntoClientRequest},
};
use tracing::{error, warn};

pub async fn fetch_symbol_list() -> Result<Vec<String>, reqwest::Error> {
    Ok(reqwest::Client::new()
        .get("https://fapi.binance.com/fapi/v1/exchangeInfo")
        .header("Accept", "application/json")
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?
        .get("symbols")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .filter(|j_symbol| j_symbol.get("contractType").unwrap().as_str().unwrap() == "PERPETUAL")
        .map(|j_symbol| {
            j_symbol
                .get("symbol")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect())
}

pub async fn fetch_depth_snapshot(symbol: &str) -> Result<String, reqwest::Error> {
    reqwest::Client::new()
        .get(format!(
            "https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000"
        ))
        .header("Accept", "application/json")
        .send()
        .await?
        .text()
        .await
}

pub async fn connect(
    url: &str,
    ws_tx: UnboundedSender<(DateTime<Utc>, Utf8Bytes)>,
) -> Result<(), anyhow::Error> {
    let request = url.into_client_request()?;
    let (ws_stream, _) = connect_async(request).await?;
    let (mut write, mut read) = ws_stream.split();
    let (tx, mut rx) = unbounded_channel::<Bytes>();

    tokio::spawn(async move {
        while let Some(data) = rx.recv().await {
            if write.send(Message::Pong(data)).await.is_err() {
                let _ = write.close().await;
                return;
            }
        }
    });

    let mut last_ping = Instant::now();
    let mut checker = interval(Duration::from_secs(10));

    loop {
        select! {
            msg = read.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let recv_time = Utc::now();
                    if ws_tx.send((recv_time, text)).is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Binary(_))) => {}
                Some(Ok(Message::Ping(data))) => {
                    if tx.send(data).is_err() {
                        return Err(Error::from(io::Error::new(
                            ErrorKind::ConnectionAborted,
                            "closed",
                        )));
                    }
                    last_ping = Instant::now();
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(close_frame))) => {
                    warn!(?close_frame, "closed");
                    return Err(Error::from(io::Error::new(
                        ErrorKind::ConnectionAborted,
                        "closed",
                    )));
                }
                Some(Ok(Message::Frame(_))) => {}
                Some(Err(e)) => {
                    return Err(Error::from(e));
                }
                None => {
                    break;
                }
            },
            _ = checker.tick() => {
                if last_ping.elapsed() > Duration::from_secs(300) {
                    warn!("Ping timeout.");
                    return Err(Error::from(io::Error::new(
                        ErrorKind::TimedOut,
                        "Ping",
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Binance USDT-M routes market-data streams across two endpoints:
/// bookTicker/depth on `/public`, trade/aggTrade/markPrice/forceOrder on
/// `/market`. The unrouted `/stream?streams=` endpoint only delivers `/public`
/// streams, so trades are SILENTLY dropped there. Route each stream by type.
pub const WS_PUBLIC: &str = "wss://fstream.binance.com/public";
pub const WS_MARKET: &str = "wss://fstream.binance.com/market";

pub fn classify_stream(stream: &str) -> &'static str {
    // `stream` is a template like "$symbol@trade" or "$symbol@depth@0ms";
    // classify by the type after the first '@'.
    let ty = stream.split_once('@').map_or(stream, |(_, t)| t);
    if ty.starts_with("depth") {
        "public"
    } else if matches!(ty, "trade" | "aggTrade" | "markPrice" | "markPrice@1s" | "forceOrder") {
        "market"
    } else {
        // bookTicker, miniTicker, ticker, … default to public
        "public"
    }
}

pub async fn keep_connection(
    base_url: &'static str,
    streams: Vec<String>,
    symbol_list: Vec<String>,
    ws_tx: UnboundedSender<(DateTime<Utc>, Utf8Bytes)>,
) {
    let mut error_count = 0;
    loop {
        let connect_time = Instant::now();
        let streams_str = symbol_list
            .iter()
            .flat_map(|pair| {
                streams
                    .iter()
                    .cloned()
                    .map(|stream| {
                        stream
                            .replace("$symbol", pair.to_lowercase().as_str())
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>()
            .join("/");
        if let Err(error) = connect(
            &format!("{base_url}/stream?streams={streams_str}"),
            ws_tx.clone(),
        )
        .await
        {
            error!(?error, "websocket error");
            error_count += 1;
            if connect_time.elapsed() > Duration::from_secs(30) {
                error_count = 0;
            }
            if error_count > 3 {
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else if error_count > 10 {
                tokio::time::sleep(Duration::from_secs(5)).await;
            } else if error_count > 20 {
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        } else {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::classify_stream;

    #[test]
    fn routes_um_streams_to_endpoints() {
        // trades + mark price + liquidations -> /market
        assert_eq!(classify_stream("$symbol@trade"), "market");
        assert_eq!(classify_stream("$symbol@aggTrade"), "market");
        assert_eq!(classify_stream("$symbol@markPrice@1s"), "market");
        assert_eq!(classify_stream("$symbol@forceOrder"), "market");
        // book + depth -> /public
        assert_eq!(classify_stream("$symbol@bookTicker"), "public");
        assert_eq!(classify_stream("$symbol@depth@0ms"), "public");
        assert_eq!(classify_stream("$symbol@depth"), "public");
    }
}
