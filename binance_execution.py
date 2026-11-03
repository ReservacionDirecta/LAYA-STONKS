#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Laya STONKS — Motor de Ejecución en Binance con Optimización de Comisiones (Maker-First).

Objetivos de optimización de comisiones:
  1. ENTRADA MAKER (Post-Only):
     - Spot: tipo de orden LIMIT_MAKER. Si cruza el libro, Binance la rechaza sin cobrar comisión taker.
     - Futuros: tipo LIMIT con timeInForce='GTX' (Post-Only). Ahorra 60% de comisión (0.018% vs 0.045%).
  2. SALIDAS MAKER (Take Profit):
     - TP1 y TP colocados pasivamente como órdenes LIMIT en el libro.
  3. DESCUENTO BNB:
     - Monitoreo del saldo de BNB para asegurar el 25% (Spot) o 10% (Futuros) de descuento.
  4. FILTROS DE PRECISIÓN DE LOTES Y PRECIO:
     - Ajuste exacto a LOT_SIZE (stepSize) y PRICE_FILTER (tickSize) según exchangeInfo.
     - Validación de MIN_NOTIONAL (evita rechazos por órdenes menores a $5 o $10).
"""

import math
import time
from typing import Dict, Any, Optional, Tuple
from binance.client import Client
from binance.exceptions import BinanceAPIException


def round_step(value: float, step: float) -> float:
    """Redondea hacia abajo un valor al múltiplo exacto de step (stepSize o tickSize)."""
    if step <= 0:
        return value
    precision = max(0, int(round(-math.log10(step))))
    factor = 1.0 / step
    return round(math.floor(value * factor + 1e-9) / factor, precision)


class BinanceExecutionEngine:
    """Motor de órdenes conectado a Binance Spot y Futuros con política Maker-First."""

    def __init__(self, api_key: str, api_secret: str, testnet: bool = True, live_orders: bool = False):
        self.api_key = api_key
        self.api_secret = api_secret
        self.testnet = testnet
        self.live_orders = live_orders
        self.client: Optional[Client] = None
        self._symbol_cache: Dict[str, Dict[str, Any]] = {}

        if self.api_key and self.api_secret:
            try:
                self.client = Client(self.api_key, self.api_secret, testnet=self.testnet)
            except Exception as e:
                print(f"[EXEC] Error conectando a Binance: {e}")

    def is_connected(self) -> bool:
        return self.client is not None

    def get_symbol_rules(self, symbol: str) -> Dict[str, Any]:
        """Obtiene y cachea los filtros oficiales de Binance (tickSize, stepSize, minNotional)."""
        symbol = symbol.upper()
        if symbol in self._symbol_cache:
            return self._symbol_cache[symbol]

        if not self.client:
            # Reglas seguras por defecto
            return {"tick_size": 0.01, "step_size": 0.0001, "min_qty": 0.0001, "min_notional": 5.0}

        try:
            info = self.client.get_symbol_info(symbol)
            if not info:
                return {"tick_size": 0.01, "step_size": 0.0001, "min_qty": 0.0001, "min_notional": 5.0}

            rules = {
                "tick_size": 0.01,
                "step_size": 0.0001,
                "min_qty": 0.0001,
                "min_notional": 5.0,
            }
            for f in info.get("filters", []):
                ftype = f.get("filterType")
                if ftype == "PRICE_FILTER":
                    rules["tick_size"] = float(f.get("tickSize", 0.01))
                elif ftype == "LOT_SIZE":
                    rules["step_size"] = float(f.get("stepSize", 0.0001))
                    rules["min_qty"] = float(f.get("minQty", 0.0001))
                elif ftype in ("NOTIONAL", "MIN_NOTIONAL"):
                    rules["min_notional"] = float(f.get("minNotional", 5.0))

            self._symbol_cache[symbol] = rules
            return rules
        except Exception as e:
            print(f"[EXEC] Error obteniendo filtros para {symbol}: {e}")
            return {"tick_size": 0.01, "step_size": 0.0001, "min_qty": 0.0001, "min_notional": 5.0}

    def format_price_qty(self, symbol: str, price: float, qty: float) -> Tuple[float, float, bool, str]:
        """Ajusta precio y cantidad según las reglas del exchange. Verifica min_notional."""
        rules = self.get_symbol_rules(symbol)
        adj_price = round_step(price, rules["tick_size"])
        adj_qty = round_step(qty, rules["step_size"])

        notional = adj_price * adj_qty
        if adj_qty < rules["min_qty"]:
            return adj_price, adj_qty, False, f"Cantidad {adj_qty} < minQty {rules['min_qty']}"
        if notional < rules["min_notional"]:
            return adj_price, adj_qty, False, f"Nocional ${notional:.2f} < minNotional ${rules['min_notional']:.2f}"

        return adj_price, adj_qty, True, "OK"

    def check_bnb_fee_discount(self) -> Dict[str, Any]:
        """Comprueba si la cuenta tiene BNB para aprovechar el descuento en comisiones."""
        if not self.client:
            return {"bnb_balance": 0.0, "has_discount": False, "msg": "Sin cliente Binance"}
        try:
            acc = self.client.get_account()
            bnb_free = 0.0
            for b in acc.get("balances", []):
                if b.get("asset") == "BNB":
                    bnb_free = float(b.get("free", 0.0))
                    break
            has_discount = bnb_free >= 0.01
            msg = (f"Saldo BNB: {bnb_free:.4f} (Descuento del 25% ACTIVO)" if has_discount
                   else f"Saldo BNB bajo: {bnb_free:.4f}. Agrega BNB para activar -25% en comisiones.")
            return {"bnb_balance": bnb_free, "has_discount": has_discount, "msg": msg}
        except Exception as e:
            return {"bnb_balance": 0.0, "has_discount": False, "msg": f"Error consultando BNB: {e}"}

    def place_maker_entry_spot(self, symbol: str, side: str, qty: float, price: float) -> Dict[str, Any]:
        """Coloca una orden de entrada LIMIT_MAKER en Spot.
        Si la orden cruzaría el libro (ejecutaría como Taker), Binance la rechaza de inmediato sin coste."""
        symbol = symbol.upper()
        side = side.upper()
        adj_p, adj_q, ok, err = self.format_price_qty(symbol, price, qty)
        if not ok:
            return {"ok": False, "error": err}

        # Formato de string para evitar problemas de precisión en la API
        rules = self.get_symbol_rules(symbol)
        p_dec = max(0, int(round(-math.log10(rules["tick_size"]))))
        q_dec = max(0, int(round(-math.log10(rules["step_size"]))))
        p_str = f"{adj_p:.{p_dec}f}"
        q_str = f"{adj_q:.{q_dec}f}"

        if not self.live_orders:
            return {
                "ok": True,
                "simulated": True,
                "order_type": "LIMIT_MAKER",
                "symbol": symbol,
                "side": side,
                "qty": adj_q,
                "price": adj_p,
                "fee_type": "maker",
                "fee_saved_pct": 25.0, # vs taker
            }

        try:
            res = self.client.create_order(
                symbol=symbol,
                side=side,
                type="LIMIT_MAKER",
                quantity=q_str,
                price=p_str,
            )
            return {"ok": True, "simulated": False, "order_id": res.get("orderId"), "data": res}
        except BinanceAPIException as e:
            if e.code == -2010: # Order would immediately trigger or execute as taker
                return {"ok": False, "error": "Rechazada por política Maker (habría ejecutado como Taker). Esperando mejor precio."}
            return {"ok": False, "error": f"Binance error ({e.code}): {e.message}"}
        except Exception as e:
            return {"ok": False, "error": f"Error inesperado: {e}"}

    def place_oco_exit_spot(self, symbol: str, qty: float, tp_price: float, sl_price: float) -> Dict[str, Any]:
        """Coloca una orden OCO de salida en Spot (Take Profit Límite Maker + Stop Loss)."""
        symbol = symbol.upper()
        rules = self.get_symbol_rules(symbol)
        p_dec = max(0, int(round(-math.log10(rules["tick_size"]))))
        q_dec = max(0, int(round(-math.log10(rules["step_size"]))))

        adj_tp = round_step(tp_price, rules["tick_size"])
        adj_sl = round_step(sl_price, rules["tick_size"])
        adj_qty = round_step(qty, rules["step_size"])

        # StopLimit ligeramente por debajo del stopPrice para garantizar llenado
        adj_sl_limit = round_step(adj_sl * 0.998, rules["tick_size"])

        if not self.live_orders:
            return {
                "ok": True,
                "simulated": True,
                "order_type": "OCO",
                "symbol": symbol,
                "qty": adj_qty,
                "tp_price": adj_tp,
                "sl_stop": adj_sl,
                "sl_limit": adj_sl_limit,
            }

        try:
            res = self.client.create_oco_order(
                symbol=symbol,
                side="SELL",
                quantity=f"{adj_qty:.{q_dec}f}",
                price=f"{adj_tp:.{p_dec}f}",
                stopPrice=f"{adj_sl:.{p_dec}f}",
                stopLimitPrice=f"{adj_sl_limit:.{p_dec}f}",
                stopLimitTimeInForce="GTC",
            )
            return {"ok": True, "simulated": False, "order_list_id": res.get("orderListId"), "data": res}
        except Exception as e:
            return {"ok": False, "error": f"Error creando OCO: {e}"}

    def cancel_all_orders(self, symbol: str) -> Dict[str, Any]:
        """Cancela todas las órdenes abiertas de un símbolo."""
        if not self.live_orders or not self.client:
            return {"ok": True, "simulated": True}
        try:
            open_orders = self.client.get_open_orders(symbol=symbol)
            cancelled = []
            for o in open_orders:
                c = self.client.cancel_order(symbol=symbol, orderId=o["orderId"])
                cancelled.append(c.get("orderId"))
            return {"ok": True, "cancelled": cancelled}
        except Exception as e:
            return {"ok": False, "error": str(e)}
