#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Laya STONKS — Verificador de Binance SPOT TESTNET (solo lectura, no opera).

Comprueba, usando las credenciales de config.json:
  1. Conectividad con https://testnet.binance.vision (ping + hora del servidor).
  2. Si las claves son VALIDAS para la testnet (GET firmado /api/v3/account).
  3. Saldo disponible en la testnet (gekkoBTC, USDT, etc.).
  4. Tapas de mercado reales (ticker BTCUSDT) para confirmar que hay datos.

IMPORTANTE: las claves de la cuenta REAL de Binance NO funcionan en la
testnet. Para la testnet se generan claves propias en
https://testnet.binance.vision (login con GitHub).

Uso: python testnet_check.py
"""

import hashlib
import hmac
import json
import os
import sys
import time
import urllib.parse
import urllib.request

# Consolas Windows antiguas: evitar UnicodeEncodeError al imprimir saldos
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

BASE = "https://testnet.binance.vision/api"
CONFIG_FILE = "config.json"

MASK = ("api_key no encontrada",)


def load_config(path=CONFIG_FILE):
    if not os.path.exists(path):
        raise SystemExit("No se encontro " + path + "; ejecuta este script en la carpeta del proyecto.")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def mask_key(key):
    if not key:
        return "(vacia)"
    return key[:6] + "..." + key[-4:] + " (" + str(len(key)) + " chars)"


def http(method, url, params=None, headers=None):
    qs = urllib.parse.urlencode(params or {})
    full = url + ("?" + qs if qs else "")
    req = urllib.request.Request(full, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def signed(method, path, key, secret, params=None):
    p = dict(params or {})
    p["timestamp"] = int(time.time() * 1000)
    p["recvWindow"] = 10000
    qs = urllib.parse.urlencode(p)
    sig = hmac.new(secret.encode("utf-8"), qs.encode("utf-8"), hashlib.sha256).hexdigest()
    url = BASE + path + "?" + qs + "&signature=" + sig
    req = urllib.request.Request(url, method=method)
    req.add_header("X-MBX-APIKEY", key)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def main():
    cfg = load_config()
    binance = cfg.get("binance", {})
    key, secret = binance.get("api_key", ""), binance.get("api_secret", "")
    testnet_flag = bool(binance.get("testnet", False))

    print("=" * 62)
    print(" Laya STONKS - verificacion de Binance SPOT TESTNET")
    print("=" * 62)
    print(" Flag 'testnet' en config.json:", "true" if testnet_flag else "false (DESPACTIVADO)")
    print(" Modo del bot:", cfg.get("mode", "paper"), "(el modo live solo se usa si lo cambias)")
    print(" API key:", mask_key(key))

    # 1) Conectividad publica
    try:
        st, ping = http("GET", BASE + "/v3/ping")
        st2, srv = http("GET", BASE + "/v3/time")
        print(f" [1] Conectividad testnet: ping={st} hora-servidor={srv.get('serverTime')} OK")
    except Exception as e:
        print(" [1] FALLO conectividad testnet:", e)
        print("     (revisa tu conexion o proxy y vuelve a intentarlo)")
        return 1

    # 2) Datos de mercado publicos (no requieren claves)
    try:
        st3, ticker = http("GET", BASE + "/v3/ticker/price", params={"symbol": "BTCUSDT"})
        print(f" [2] Precio BTCUSDT en testnet: {ticker.get('price')} (datos reales disponibles)")
    except Exception as e:
        print(" [2] Aviso: no se pudo leer el ticker:", e)

    if not (key and secret):
        print(" [3] No hay claves en config.json - no se puede autenticar.")
        print("     Genera claves en https://testnet.binance.vision y pegalas en config.json")
        return 1

    # 3) Autenticacion firmada: ¿son claves de TESTNET validas?
    try:
        st4, acc = signed("GET", "/v3/account", key, secret)
        if st4 == 200:
            print(f" [3] Claves VALIDAS para la testnet (HTTP {st4})")
            bal = [b for b in acc.get("balances", []) if float(b.get("free", 0)) + float(b.get("locked", 0)) > 0]
            print(f"     Cuenta: {('canTrade' if acc.get('canTrade') else 'NO puede operar')}, "
                  f"{'USDT spot' if acc.get('accountType', '?') == 'SPOT' else acc.get('accountType')}")
            for b in bal[:10]:
                total = float(b["free"]) + float(b["locked"])
                if total > 0:
                    print(f"       - {b['asset']}: libre={b['free']} bloqueado={b['locked']}")
            if len(bal) > 10:
                print(f"       ... y {len(bal) - 10} activos mas")
        else:
            print(" [3] Claves rechazadas por la testnet (HTTP", str(st4) + ")")
            return 1
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:
            body = {}
        code = body.get("code")
        print(" [3] Claves NO validas para la testnet (HTTP " + str(e.code) + " code " + str(code) + "):",
              body.get("msg", e.reason))
        print("     Causa tipica: son claves de la cuenta REAL. La testnet usa claves propias.")
        print("     Siguiente paso: entra en https://testnet.binance.vision con GitHub,")
        print("     genera claves de testnet y pegalas en config.json (binance.api_key/api_secret).")
        return 1
    except Exception as e:
        print(" [3] Error inesperado al autenticar:", e)
        return 1

    print("=" * 62)
    print(" RESULTADO: testnet ACTIVADA y operativa para este proyecto.")
    print(" Siguiente nivel: poner \"mode\": \"live\" haria que bot.py usara")
    print(" datos reales de testnet (solo recomendable con claves de testnet).")
    print("=" * 62)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
