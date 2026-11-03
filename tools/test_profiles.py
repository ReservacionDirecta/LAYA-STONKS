import sys
import server

def test_agentic_profiles_and_execution():
    print("=== TEST: Presets Agénticos (Conservador, Estándar, Agresivo) ===")
    
    # 1. Configuración y Manager
    cfg = server.load_config("config.json")
    manager = server.Manager(cfg)
    
    # Crear un simulador solo y uno multi-crypto
    sim_solo = manager.create_sim("sim-test-solo", "BTC Solo", 50.0, ["BTCUSDT"])
    sim_multi = manager.create_sim("sim-test-multi", "Multi Crypto", 50.0, ["BTCUSDT", "ETHUSDT"])
    
    # 2. Probar Perfil Conservador
    res_c = manager.apply_preset_profile("conservative")
    assert res_c["ok"] is True, f"Error aplicando perfil conservador: {res_c}"
    assert sim_solo.params["min_signal_score"] == 70.0
    assert sim_solo.params["sl_atr_mult"] == 1.6
    assert sim_solo.params["tp_rr"] == 2.5
    assert sim_solo.params["cooldown_bars"] == 6
    assert sim_solo.params["leverage"] == 1
    assert sim_multi.params["min_signal_score"] == 70.0
    assert sim_multi.params["sl_atr_mult"] == 1.6
    assert sim_multi.params["max_open"] == 2  # Proporcional conservado
    print("  [OK] Perfil Conservador aplicado correctamente a toda la flota.")
    
    # Probar evaluación de veredicto System 2 en Conservador
    appr, min_c, rec, conf, rsn = sim_solo._evaluate_system2_verdict("BTCUSDT", "LONG", {
        "recomendacion_laya": "LONG", "confianza": 0.60
    })
    assert appr is False, "En Conservador no debe aprobar con 60% de confianza (requiere >=65%)"
    assert min_c == 0.65
    print("  [OK] Conservador rechaza con 60% de confianza (umbral 65%).")

    appr2, min_c2, rec2, conf2, rsn2 = sim_solo._evaluate_system2_verdict("BTCUSDT", "LONG", {
        "recomendacion_laya": "LONG", "confianza": 0.75
    })
    assert appr2 is True, "En Conservador debe aprobar con 75% de confianza"
    print("  [OK] Conservador aprueba con 75% de confianza.")

    # 3. Probar Perfil Estándar
    res_s = manager.apply_preset_profile("standard")
    assert res_s["ok"] is True
    assert sim_solo.params["min_signal_score"] == 60.0
    assert sim_solo.params["sl_atr_mult"] == 1.4
    assert sim_solo.params["tp_rr"] == 2.2
    assert sim_solo.params["cooldown_bars"] == 3
    print("  [OK] Perfil Estándar aplicado correctamente.")

    appr_s, min_cs, _, _, _ = sim_solo._evaluate_system2_verdict("BTCUSDT", "LONG", {
        "recomendacion_laya": "LONG", "confianza": 0.55
    })
    assert appr_s is True, "En Estándar debe aprobar con 55% (umbral 50%)"
    assert min_cs == 0.50
    print("  [OK] Estándar aprueba con 55% de confianza (umbral 50%).")

    # 4. Probar Perfil Agresivo
    res_a = manager.apply_preset_profile("aggressive")
    assert res_a["ok"] is True
    assert sim_solo.params["min_signal_score"] == 50.0
    assert sim_solo.params["sl_atr_mult"] == 1.2
    assert sim_solo.params["tp_rr"] == 1.8
    assert sim_solo.params["cooldown_bars"] == 1
    assert sim_solo.params["leverage"] == 2
    print("  [OK] Perfil Agresivo aplicado correctamente.")

    appr_a, min_ca, _, _, _ = sim_solo._evaluate_system2_verdict("BTCUSDT", "LONG", {
        "recomendacion_laya": "LONG", "confianza": 0.42
    })
    assert appr_a is True, "En Agresivo debe aprobar con 42% (umbral 40%)"
    assert min_ca == 0.40
    print("  [OK] Agresivo aprueba con 42% de confianza (umbral 40%).")

    # Probar que en Agresivo, si Gemini devuelve WAIT pero el Quant score es alto (>=65), se aprueba
    m = sim_solo.markets["BTCUSDT"]
    m.diagnostics = {"quant": {"long": 72.0, "short": 20.0}}
    appr_wait, _, _, _, rsn_w = sim_solo._evaluate_system2_verdict("BTCUSDT", "LONG", {
        "recomendacion_laya": "WAIT", "confianza": 0.30
    })
    assert appr_wait is True, "En Agresivo debe aprobar con Quant Score alto aún si Gemini responde WAIT"
    print("  [OK] Agresivo aprueba con Quant Score alto (72) ante recomendación WAIT de System 2.")

    # 5. Probar Ajuste Dinámico de SL/TP por System 2 en Simulator.open
    sim_solo.cash = 50.0
    m.price = 100.0
    m.atr = 2.0
    sim_solo.params["tp_sl_mode"] = "atr"
    sim_solo.params["sl_atr_mult"] = 1.0  # base sl_dist = 2.0
    sim_solo.params["tp_rr"] = 2.0        # base tp_dist = 4.0
    sim_solo.params["min_sl_pct"] = 0.001
    sim_solo.params["max_sl_pct"] = 0.10
    
    # Asignar ajuste dinámico sugerido por IA: SL x0.80 (más ceñido), TP x1.20 (más amplio)
    sim_solo._ai_sl_tp_adjust = {
        "BTCUSDT": {"sl_factor": 0.80, "tp_factor": 1.20, "nota": "Ajuste dinámico IA"}
    }
    sim_solo.open("BTCUSDT", "LONG")
    pos = sim_solo.positions["BTCUSDT"]
    # base sl_dist fue 2.0 -> x0.80 = 1.6 -> sl esperado = 100 - 1.6 = 98.4
    # base tp_dist fue 4.0 -> x1.20 = 4.8 -> tp esperado = 100 + 4.8 = 104.8
    assert abs(pos["sl"] - 98.4) < 1e-4, f"SL esperado 98.4, obtenido {pos['sl']}"
    assert abs(pos["tp"] - 104.8) < 1e-4, f"TP esperado 104.8, obtenido {pos['tp']}"
    print("  [OK] Ajuste dinámico de SL (x0.80) y TP (x1.20) por System 2 aplicado con precisión en la orden.")

    print("\nTODAS LAS PRUEBAS DE PERFILES Y EJECUCIÓN PASARON AL 100%.")

if __name__ == "__main__":
    test_agentic_profiles_and_execution()
