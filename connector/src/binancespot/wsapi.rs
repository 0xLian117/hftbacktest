//! Binance SPOT ws-api 下单 session handle（reader-loop-as-actor）。
//!
//! `SplitSink` 需 `&mut` 且不能跨任务共享（会与 select! 死锁/交错半帧），故让
//! `user_data_stream::connect()` 的 select! 循环做唯一 writer；submit/cancel 经 mpsc 把请求
//! 汇入循环，响应按 id 关联回各自的 oneshot。

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{mpsc, oneshot};

use crate::binancespot::msg::wsapi::{IdProbe, WsApiOrderResult};

/// 汇入循环的一条下/撤单请求。`text` 是已序列化好的 ws-api 帧（order.place / order.cancel）；
/// `id` 是该帧的顶层 id（循环用它把响应路由回 `resp_tx`）。
pub struct WsApiCommand {
    pub id: String,
    pub text: String,
    pub resp_tx: oneshot::Sender<WsApiOrderResult>,
}

/// 下单侧持有的 cloneable 句柄。存在 `SharedWsApi` slot 里，session 起来才 `Some`。
#[derive(Clone)]
pub struct WsApiHandle {
    pub cmd_tx: mpsc::UnboundedSender<WsApiCommand>,
}

/// `BinanceSpot` 上的共享 slot：`None` = ws-api 未就绪（→ 下单直接走 REST），
/// `Some` = logon 成功、可走 WS。
pub type SharedWsApi = Arc<Mutex<Option<WsApiHandle>>>;

/// 循环任何退出路径（断线/超时/close/error）都把 slot 清回 `None`：新的 submit/cancel 快照到
/// `None` → 直接 REST。循环内的 `pending` map 随函数返回自动析构 → 在飞请求的 oneshot 收端全
/// `Canceled` → 各 caller 落 REST（R4：单一守卫覆盖所有退出路径）。
pub struct ClearOnDrop(pub SharedWsApi);

impl Drop for ClearOnDrop {
    fn drop(&mut self) {
        *self.0.lock().unwrap() = None;
    }
}

/// 判断一帧文本是否应路由到某个在飞的 order 请求。
///
/// R3 消歧：AuthResponse / SubscribeResponse 与 order response 都是 `{id,status,..}`。真正的保证
/// 是 **id 命名空间不相交**——`session.logon`/`userDataStream.subscribe` 的 id 由
/// `generate_rand_string` 生成、**从不 insert 进 `pending`**，故它们必返回 `None`、fall-through
/// 到 UserStream 匹配。仅当顶层 id 命中 `pending` 时返回 `Some(id)`（→ 解 WsApiOrderResponse 路由）。
pub fn ws_api_response_id(
    text: &str,
    pending: &HashMap<String, oneshot::Sender<WsApiOrderResult>>,
) -> Option<String> {
    let probe: IdProbe = serde_json::from_str(text).ok()?;
    let id = probe.id?;
    if pending.contains_key(&id) {
        Some(id)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn clear_on_drop_resets_slot() {
        let shared: SharedWsApi = Arc::new(Mutex::new(None));
        let (tx, _rx) = mpsc::unbounded_channel::<WsApiCommand>();
        *shared.lock().unwrap() = Some(WsApiHandle { cmd_tx: tx });
        {
            let _guard = ClearOnDrop(shared.clone());
            assert!(shared.lock().unwrap().is_some());
        }
        // guard dropped → slot cleared.
        assert!(shared.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn pending_drop_cancels_inflight() {
        let (resp_tx, resp_rx) = oneshot::channel::<WsApiOrderResult>();
        let mut pending: HashMap<String, oneshot::Sender<WsApiOrderResult>> = HashMap::new();
        pending.insert("order-1".to_string(), resp_tx);
        drop(pending);
        // caller awaiting resp_rx sees Canceled → falls back to REST.
        assert!(resp_rx.await.is_err());
    }

    #[test]
    fn only_pending_id_routes_auth_falls_through() {
        let (resp_tx, _resp_rx) = oneshot::channel::<WsApiOrderResult>();
        let mut pending: HashMap<String, oneshot::Sender<WsApiOrderResult>> = HashMap::new();
        pending.insert("order-1".to_string(), resp_tx);

        // order response with matching id → routes.
        let order_resp = r#"{"id":"order-1","status":200,"result":{}}"#;
        assert_eq!(
            ws_api_response_id(order_resp, &pending),
            Some("order-1".to_string())
        );

        // AuthResponse: has id/status but its id was never inserted into pending → miss.
        let auth = r#"{"id":"logon-random-id","status":200,"result":{"apiKey":"x","authorizedSince":1,"connectedSince":1,"returnRateLimits":true,"serverTime":1,"userDataStream":true}}"#;
        assert_eq!(ws_api_response_id(auth, &pending), None);

        // executionReport wrapper: no top-level id → miss (falls through to UserStream).
        let exec = r#"{"subscriptionId":0,"event":{"e":"executionReport","i":1}}"#;
        assert_eq!(ws_api_response_id(exec, &pending), None);
    }
}
