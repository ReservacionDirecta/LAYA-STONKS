#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Diagnóstico mínimo de la API Gemini: GET ListModels + POST generateContent."""
import json
import os
import time
import urllib.request
import urllib.error

BASE = "https://generativelanguage.googleapis.com/v1beta"


def main():
    key = os.environ.get("GEMINI_API_KEY", "")
    if not key:
        try:
            import winreg
            k = winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment")
            key, _ = winreg.QueryValueEx(k, "GEMINI_API_KEY")
        except Exception as e:
            key = ""
            print("[WARN] no key:", e)
    if not key:
        print("[FATAL] GEMINI_API_KEY vacía")
        return

    # 1) GET ListModels
    t0 = time.time()
    try:
        req = urllib.request.Request(f"{BASE}/models?key={key}")
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
        names = [m["name"] for m in data.get("models", []) if "generateContent" in m.get("supportedGenerationMethods", [])]
        print(f"GET ListModels OK: {len(names)} modelos ({time.time()-t0:.1f}s). flash* caja rápida (3.6-3.8):")
        print([n for n in names if "flash" in n and "3.8" in n or n.endswith("gemini-flash-latest")])
    except Exception as e:
        print("GET ListModels FAIL:", e)

    for model in ("gemini-3.5-flash", "gemini-3.6-flash", "gemini-3.7-flash"):
        # 2) POST mínimo con urllib (stdlib)
        url = f"{BASE}/models/{model}:generateContent?key={key}"
        payload = {"contents": [{"parts": [{"text": "Responde SOLO con la palabra ping"}]}],
                   "generationConfig": {"temperature": 0.0, "maxOutputTokens": 32}}
        t0 = time.time()
        try:
            req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                         headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req, timeout=60) as r:
                data = json.loads(r.read())
            txt = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            print(f"POST urllib {model}: OK {time.time()-t0:.1f}s -> {txt!r}")
        except urllib.error.HTTPError as e:
            print(f"POST urllib {model}: HTTP {e.code} ({time.time()-t0:.1f}s) -> {e.read().decode('utf-8', 'ignore')[:300]}")
        except Exception as e:
            print(f"POST urllib {model}: FAIL ({time.time()-t0:.1f}s) {type(e).__name__}: {e}")

        # 3) POST con requests (para comparar stack HTTP)
        try:
            import requests
            t0 = time.time()
            r = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=60)
            print(f"POST requests {model}: HTTP {r.status_code} ({time.time()-t0:.1f}s) -> {r.text[:200]!r}")
        except Exception as e:
            print(f"POST requests {model}: FAIL ({time.time()-t0:.1f}s) {type(e).__name__}: {str(e)[:200]}")


if __name__ == "__main__":
    main()
