use std::sync::{Arc, Mutex};

use chrono::Utc;
use hashbrown::HashMap;
use hftbacktest::types::{Order, OrderId, Status};
use tracing::error;

use crate::{
    binancespot::{
        BinanceSpotError, // msg::{rest::OrderResponse, stream::OrderTradeUpdate},
        msg::{rest::OrderResponse, stream::ExecutionReport},
    },
    connector::GetOrders,
    utils::{RefSymbolOrderId, SymbolOrderId, generate_rand_string},
};

#[derive(Debug)]
struct OrderExt {
    symbol: String,
    order: Order,
    removed_by_ws: bool,
    removed_by_rest: bool,
}

pub type SharedOrderManager = Arc<Mutex<OrderManager>>;

pub type ClientOrderId = String;

#[derive(Default, Debug)]
pub struct OrderManager {
    prefix: String,
    orders: HashMap<ClientOrderId, OrderExt>,
    order_id_map: HashMap<SymbolOrderId, ClientOrderId>,
}

impl OrderManager {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            orders: Default::default(),
            order_id_map: Default::default(),
        }
    }

    pub fn update_from_ws(
        &mut self,
        resp: &ExecutionReport,
    ) -> Result<Option<Order>, BinanceSpotError> {
        if !resp.client_order_id.starts_with(&self.prefix) {
            return Err(BinanceSpotError::PrefixUnmatched);
        }
        let order_ext = self
            .orders
            .get_mut(&resp.client_order_id)
            .ok_or(BinanceSpotError::OrderNotFound)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        // QUI-114:累计成交权威 = qty − leaves_qty(不用 exec_qty:WS 路是「最后一笔」量)。记更新前累计,
        // 用于判「迟到的更高累计成交」。
        let old_cum = order_ext.order.qty - order_ext.order.leaves_qty;
        if resp.event_time * 1_000_000 >= order_ext.order.exch_timestamp {
            order_ext.order.side = resp.side;
            order_ext.order.time_in_force = resp.time_in_force;
            order_ext.order.exch_timestamp = resp.event_time * 1_000_000;
            order_ext.order.status = resp.order_status;
            order_ext.order.exec_price_tick =
                (resp.last_filled_price / order_ext.order.tick_size).round() as i64;
            order_ext.order.exec_qty = resp.order_last_filled_quantity;
            order_ext.order.order_type = resp.order_type;
        }
        // QUI-114 P0:累计成交单调合并,**独立于 event ts**。乱序到达的 fill(event_time 更旧但累计
        // 更高)= 真实迟到成交,绝不能被上面的 ts 门控丢弃;ts 只门控 status/价格等元数据。qty(委托量)
        // 随累计权威值一起写入(委托量对同一单不变)。
        if resp.order_filled_accumulated_quantity > order_ext.order.qty - order_ext.order.leaves_qty + 1e-12 {
            order_ext.order.qty = resp.quantity;
            order_ext.order.leaves_qty = resp.quantity - resp.order_filled_accumulated_quantity;
            // QUI-131(parity): 仅在**累计成交增加**(=这笔事件确有成交)时写 maker。放此块而非上面的 ts 门控块,
            // 才能覆盖 QUI-114 的迟到 fill(event_time 更旧、走此块、跳过门控);且 NEW/CANCELED 不进此块 →
            // 不被非成交事件的 `m`(语义未定义)污染。`m`=is_maker(executionReport)。
            order_ext.order.maker = resp.is_maker;
        }

        // QUI-114:即便另一源已把该单标终态(already_removed),若本次**累计成交增加**(cancel 竞态里迟到的
        // 最终 fill)仍**发布纠正** → bot/OMS 据此补仓、不重开单。result 在下方 orders.remove 之前算好,故
        // 纠正先发布再删除,无需保留窗口。累计不增 = 冗余/stale → None。
        let new_cum = order_ext.order.qty - order_ext.order.leaves_qty;
        let result = if !already_removed || new_cum > old_cum + 1e-12 {
            Some(order_ext.order.clone())
        } else {
            None
        };

        if order_ext.order.status != Status::New
            && order_ext.order.status != Status::PartiallyFilled
        {
            order_ext.removed_by_ws = true;
            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(&resp.client_order_id).unwrap();
            }
        }

        Ok(result)
    }

    pub fn update_submit_fail(
        &mut self,
        client_order_id: &ClientOrderId,
        error: &BinanceSpotError,
    ) -> Option<Order> {
        match error {
            BinanceSpotError::OrderError { code: -5022, .. } => {
                // GTX rejection.
            }
            BinanceSpotError::OrderError { code: -1008, .. } => {
                // Server is currently overloaded with other requests. Please try again in a few minutes.
                error!(
                    "Server is currently overloaded with other requests. Please try again in a few minutes."
                );
            }
            BinanceSpotError::OrderError { code: -2019, .. } => {
                // Margin is insufficient.
                error!("Margin is insufficient.");
            }
            BinanceSpotError::OrderError { code: -1015, .. } => {
                // Too many new orders; current limit is 300 orders per TEN_SECONDS.
                error!("Too many new orders; current limit is 300 orders per TEN_SECONDS.");
            }
            error => {
                error!(?error, "submit error");
            }
        }
        self.update_from_rest_fail(client_order_id, Some(Status::Expired))
    }

    pub fn update_cancel_fail(
        &mut self,
        client_order_id: &ClientOrderId,
        error: &BinanceSpotError,
    ) -> Option<Order> {
        match error {
            BinanceSpotError::OrderError { code: -2011, .. } => {
                // The given order may no longer exist; it could have already been filled or
                // canceled. But, it cannot determine the order status because it lacks the
                // necessary information.
                self.update_from_rest_fail(client_order_id, Some(Status::None))
            }
            error => {
                error!(?error, "cancel error");
                self.update_from_rest_fail(client_order_id, None)
            }
        }
    }

    pub fn update_from_rest_fail(
        &mut self,
        client_order_id: &ClientOrderId,
        status: Option<Status>,
    ) -> Option<Order> {
        let order_ext = self.orders.get_mut(client_order_id)?;
        // .ok_or(BinanceFuturesError::OrderNotFound)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        if let Some(status) = status {
            order_ext.order.status = status;
        }
        order_ext.order.req = Status::None;

        let result = if already_removed {
            None
        } else {
            Some(order_ext.order.clone())
        };

        if order_ext.order.status != Status::New
            && order_ext.order.status != Status::PartiallyFilled
        {
            order_ext.removed_by_rest = true;
            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(client_order_id).unwrap();
            }
        }

        result
    }

    pub fn update_from_rest(
        &mut self,
        client_order_id: &ClientOrderId,
        resp: &OrderResponse,
    ) -> Option<Order> {
        let order_ext = self.orders.get_mut(client_order_id)?;
        // .ok_or(BinanceFuturesError::OrderNotFound)?;

        let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;
        let old_cum = order_ext.order.qty - order_ext.order.leaves_qty; // QUI-114 累计 = qty−leaves
        if resp.transact_time * 1_000_000 >= order_ext.order.exch_timestamp {
            order_ext.order.side = resp.side;
            order_ext.order.time_in_force = resp.time_in_force;
            order_ext.order.exch_timestamp = resp.transact_time * 1_000_000;
            order_ext.order.status = resp.status;
            // The last filled price isn't available in the REST response.
            // Execution details are expected to be received via the WebSocket stream.
            order_ext.order.exec_qty = resp.executed_qty;
            order_ext.order.order_type = resp.order_type;
            order_ext.order.req = Status::None;
        }
        // QUI-114 P0:累计成交单调合并,独立于 REST transact_time(乱序 REST 快照的 executed_qty
        // 更高即真实迟到成交)。ts 只门控 status/元数据。
        if resp.executed_qty > order_ext.order.qty - order_ext.order.leaves_qty + 1e-12 {
            order_ext.order.qty = resp.orig_qty;
            order_ext.order.leaves_qty = resp.orig_qty - resp.executed_qty;
        }

        // QUI-114:已终态后累计成交增加(迟到 fill)仍发布纠正(先发布再删除,见 update_from_ws 注释)。
        let new_cum = order_ext.order.qty - order_ext.order.leaves_qty;
        let result = if !already_removed || new_cum > old_cum + 1e-12 {
            Some(order_ext.order.clone())
        } else {
            None
        };

        if order_ext.order.status != Status::New
            && order_ext.order.status != Status::PartiallyFilled
        {
            order_ext.removed_by_rest = true;
            if !already_removed {
                self.order_id_map.remove(&RefSymbolOrderId::new(
                    &order_ext.symbol,
                    order_ext.order.order_id,
                ));
            }

            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                self.orders.remove(client_order_id).unwrap();
            }
        }

        result
    }

    pub fn prepare_client_order_id(&mut self, symbol: String, order: Order) -> Option<String> {
        let symbol_order_id = SymbolOrderId::new(symbol.clone(), order.order_id);
        if self.order_id_map.contains_key(&symbol_order_id) {
            return None;
        }

        let client_order_id = format!("{}{}", self.prefix, generate_rand_string(16));
        if self.orders.contains_key(&client_order_id) {
            return None;
        }

        self.order_id_map
            .insert(symbol_order_id, client_order_id.clone());
        self.orders.insert(
            client_order_id.clone(),
            OrderExt {
                symbol,
                order,
                removed_by_ws: false,
                removed_by_rest: false,
            },
        );
        Some(client_order_id)
    }

    pub fn get_client_order_id(&self, symbol: &str, order_id: OrderId) -> Option<String> {
        self.order_id_map
            .get(&RefSymbolOrderId::new(symbol, order_id))
            .cloned()
    }

    /// Due to API instability or network issues, discrepancies can occur where an order is deleted
    /// by one channel but remains active because its deletion wasn't confirmed by both channels.
    /// The gc method resolves this by removing orders that were deleted by one channel but not
    /// confirmed by the other, after a defined threshold period.
    pub fn gc(&mut self) {
        let now = Utc::now().timestamp_nanos_opt().unwrap();
        let stale_ts = now - 300_000_000_000;
        let stale_ids: Vec<(_, _)> = self
            .orders
            .iter()
            .filter(|&(_, wrapper)| {
                wrapper.order.status != Status::New
                    && wrapper.order.status != Status::PartiallyFilled
                    && wrapper.order.status != Status::Unsupported
                    && wrapper.order.exch_timestamp < stale_ts
            })
            .map(|(client_order_id, wrapper)| {
                (
                    client_order_id.clone(),
                    SymbolOrderId::new(wrapper.symbol.clone(), wrapper.order.order_id),
                )
            })
            .collect();
        for (client_order_id, order_id) in stale_ids.iter() {
            if self.order_id_map.contains_key(order_id) {
                // todo: something went wrong?
                self.order_id_map.remove(order_id).unwrap();
            }
            self.orders.remove(client_order_id);
        }
    }

    pub fn cancel_all_from_rest(&mut self, symbol: &str) -> Vec<Order> {
        let mut removed_orders = Vec::new();
        let mut removed_order_ids = Vec::new();
        for (client_order_id, order_ext) in &mut self.orders {
            if order_ext.symbol != symbol {
                continue;
            }
            let already_removed = order_ext.removed_by_ws || order_ext.removed_by_rest;

            order_ext.removed_by_rest = true;
            order_ext.order.status = Status::Canceled;
            // todo: check if the exchange timestamp exists in the REST response.
            order_ext.order.exch_timestamp = Utc::now().timestamp_nanos_opt().unwrap();
            if !already_removed {
                self.order_id_map
                    .remove(&RefSymbolOrderId::new(symbol, order_ext.order.order_id));
                removed_orders.push(order_ext.order.clone());
            }

            // Completely deletes the order if it is removed by both the REST response and the
            // WebSocket stream.
            if order_ext.removed_by_ws && order_ext.removed_by_rest {
                removed_order_ids.push(client_order_id.clone());
            }
        }

        for order_id in removed_order_ids {
            self.orders.remove(&order_id).unwrap();
        }
        removed_orders
    }
}

impl GetOrders for OrderManager {
    fn orders(&self, symbol: Option<String>) -> Vec<Order> {
        self.orders
            .iter()
            .filter(|(_, order)| {
                symbol.as_ref().map(|s| order.symbol == *s).unwrap_or(true) && order.order.active()
            })
            .map(|(_, order)| &order.order)
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use hftbacktest::types::{OrdType, Order, Side, TimeInForce};

    use super::*;

    // 构造一个 executionReport(直接反序列化到 ExecutionReport;多余字段被忽略)。
    // 参数化 coid / execution_type(x) / status(X) / last_fill(l) / cum(z) / is_maker(m) / event_time(E)。
    fn report(coid: &str, x: &str, xs: &str, l: &str, z: &str, m: bool, e: i64) -> ExecutionReport {
        let json = format!(
            r#"{{"E":{e},"s":"btcfdusd","c":"{coid}","S":"BUY","o":"LIMIT","f":"GTC",
            "q":"0.00800000","p":"64685.0","P":"0","F":"0","g":-1,"x":"{x}","X":"{xs}",
            "r":"NONE","i":1,"l":"{l}","z":"{z}","L":"64685.0","n":"0","T":{e},"t":1,"I":1,
            "w":false,"m":{m},"M":true,"O":1,"Z":"0","Y":"0","Q":"0","V":"NONE"}}"#
        );
        serde_json::from_str(&json).expect("executionReport must decode")
    }

    fn register(om: &mut OrderManager) -> String {
        let order = Order::new(1, 646850, 0.1, 0.008, Side::Buy, OrdType::Limit, TimeInForce::GTC);
        om.prepare_client_order_id("btcfdusd".to_string(), order)
            .expect("register")
    }

    #[test]
    fn maker_set_on_fill_not_on_ack() {
        let mut om = OrderManager::new("t1s");
        let coid = register(&mut om);
        // NEW ack(无成交,z=0)→ 不进 L74 块 → maker 不被写。
        let acked = om.update_from_ws(&report(&coid, "NEW", "NEW", "0", "0", false, 1000)).unwrap();
        assert!(!acked.unwrap().maker, "ack 阶段不应写 maker");
        // TRADE fill(m=true,z 增加)→ L74 fire → maker=true。
        let filled = om.update_from_ws(&report(&coid, "TRADE", "FILLED", "0.008", "0.008", true, 2000)).unwrap();
        assert!(filled.unwrap().maker, "fill 后 maker 应为 true");
    }

    #[test]
    fn maker_survives_late_out_of_order_fill() {
        // QUI-114 迟到 fill:event_time 更旧、跳过 L61 ts 门控、只走 L74 累计合并块。
        // maker 若只写在 L61 块会丢;写在 L74 块才能被这笔迟到 fill 更新。
        let mut om = OrderManager::new("t1s");
        let coid = register(&mut om);
        // 部分成交 A(E=3000, z=0.003, m=false/taker)。
        let a = om.update_from_ws(&report(&coid, "TRADE", "PARTIALLY_FILLED", "0.003", "0.003", false, 3000)).unwrap();
        assert!(!a.unwrap().maker);
        // 迟到最终 fill B(E=2999<3000 → L61 skip;z=0.005>0.003 → L74 fire;m=true)。
        let b = om.update_from_ws(&report(&coid, "TRADE", "FILLED", "0.002", "0.005", true, 2999)).unwrap();
        assert!(b.unwrap().maker, "迟到 fill 的 maker 不应丢(必须走 L74 块写)");
    }
}
