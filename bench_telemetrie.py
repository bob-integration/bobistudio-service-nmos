# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Banc du CONTRAT DE TÉLÉMÉTRIE — À LANCER À LA MAIN, jamais depuis l'orchestrateur.

    ./venv/bin/python services/nmos/bench_telemetrie.py

Les monitors BCP-008 ne mesurent rien eux-mêmes : ils lisent `app.metrics.flux_etat_cache`,
rempli par la boucle de surveillance à partir du :8080 du moteur 2110_io. Ce banc vérifie ce
point de jonction — pas le protocole (voir `is12_bench.py`), mais la table qu'il publie.

Il fait tourner `rafraichir_metrics` sur un payload de synthèse, TOUTES les écritures en base et
tous les appels réseau neutralisés. Aucun conteneur n'est touché, aucune alerte n'est écrite.

Ce qui se casse ici en silence : une clé de flux qui change de forme (un flux audio qui écrase
son voisin faute d'`audio_idx`), un `stalled` qui n'est plus posé, un `link_up` qu'on se met à
supposer vrai quand il est absent. Rien de tout cela ne lève d'exception — les monitors
publieraient simplement des statuts faux, poliment."""
import json, os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from app import metrics

VMID = 424242
ECHECS = []


def verifier(c, quoi):
    print(("  ok   " if c else "  ÉCHEC ") + quoi)
    if not c:
        ECHECS.append(quoi)


PAYLOAD = {
    "fps": 50.0,
    "receivers": [
        {"idx": 0, "essence": "video", "fps": 50.0, "frame_index": 100, "mode": "mtl",
         "rx_latency_ms": 11.0, "signal": {"black": False}},
        {"idx": 1, "essence": "video", "fps": 0.0, "frame_index": 0, "mode": "error",
         "rx_error": "lcores"},
        {"idx": 3, "essence": "audio", "fps": 1000.0, "mode": "mtl", "frame_index": 5},
        {"idx": 2, "essence": "anc", "fps": 50.0, "mode": "idle", "frame_index": 0},
    ],
    "senders": [
        {"tx_idx": 0, "idx": 0, "essence": "video", "fps": 50.0, "fps_nominal": 50.0,
         "late": 4, "repeats": 2, "fps_source": 50.0, "inputs_latency_ms": {"a": 3}, "signal": {}},
        {"tx_idx": 0, "idx": 0, "essence": "audio", "audio_idx": 0, "inputs_latency_ms": {"a": 3}},
        {"tx_idx": 0, "idx": 0, "essence": "audio", "audio_idx": 1, "inputs_latency_ms": {"a": 3}},
        {"tx_idx": 1, "idx": 1, "essence": "anc", "inputs_latency_ms": {}},
    ],
    "nic": {"ports": [{"iface": "ens1f0", "link_up": True},
                      {"iface": "ens1f1", "link_up": False},
                      {"iface": "ens2f0"}],                     # link_up absent → ignoré
            "ip_warnings": []},
}

CONT = {"vmid": VMID, "hostname": "banc2110", "ip": "127.0.0.1", "node_id": 1,
        "statut": "running", "deploy_config": json.dumps(
            {"type": "2110_io", "params": {"hostname": "banc2110"}})}


def main():
    # Neutralisation de tout ce qui écrit ou parle au réseau.
    for nom in ("db_update_ip", "db_update_fps", "db_update_status", "db_add_alert",
                "db_update_usage"):
        setattr(metrics, nom, lambda *a, **k: None)
    metrics.db_get_container = lambda v: dict(CONT)
    metrics.get_container_ip = lambda *a, **k: "127.0.0.1"
    metrics.get_metrics = lambda ip, port=8080, avec_statut=False: (
        (dict(PAYLOAD), {"running": True, "path": "/opt/script.py"}) if avec_statut else dict(PAYLOAD))
    metrics._agent_session = lambda: type("S", (), {"get": lambda *a, **k: type(
        "R", (), {"status_code": 404})()})()
    metrics._check_signal = lambda *a, **k: None
    metrics._flux_panne = lambda *a, **k: None
    metrics._flux_ok = lambda *a, **k: None
    metrics._check_sessions_moteur = lambda *a, **k: None
    metrics._check_cadence = lambda *a, **k: None
    metrics._alarmes_slot = lambda *a, **k: {"drapeaux": {}, "niveau": "warning"}

    for _ in range(4):                       # plusieurs ticks : la détection de stall a besoin d'historique
        metrics.rafraichir_metrics(VMID)

    t = metrics.flux_etat_cache.get(VMID) or {}
    print("clés :", sorted(t))
    verifier(set(t) == {"rx:video:0", "rx:video:1", "rx:audio:3", "rx:anc:2",
                        "tx:video:0", "tx:audio:0.0", "tx:audio:0.1", "tx:anc:1"},
             "une entrée par flux, les deux audio du même slot TX distingués par audio_idx")
    verifier(t["rx:video:0"]["mode"] == "mtl" and t["rx:video:0"]["latency_ms"] == 11.0,
             "relais brut du récepteur vidéo (mode + latence)")
    verifier(t["rx:video:1"]["stalled"] is True and t["rx:video:1"]["error"] == "lcores",
             "un slot en erreur est marqué stalled avec sa cause")
    verifier(t["rx:video:0"]["stalled"] is True,
             "frame_index figé sur 4 relevés → stalled (seuil %d)" % metrics.RX_STALL_POLLS)
    verifier(t["rx:anc:2"]["mode"] == "idle", "les slots inactifs sont présents (≠ rx_served_cache)")
    verifier(t["tx:video:0"]["late"] == 4 and t["tx:video:0"]["repeats"] == 2,
             "compteurs cumulatifs TX relayés")
    verifier("tx_late" in (t["tx:video:0"].get("signal") or {}),
             "le drapeau tx_late calculé est recopié dans l'état du flux")

    liens = metrics.etat_liens(VMID)
    verifier(liens == {"up": ["ens1f0"], "down": ["ens1f1"]},
             "état des liens : le port sans link_up n'est compté nulle part")
    verifier(metrics.etat_flux(VMID, "tx", "audio", 0, 1) is not None,
             "etat_flux() retrouve le 2ᵉ flux audio du slot TX 0")
    verifier(metrics.etat_flux(VMID, "rx", "data", 2) == metrics.etat_flux(VMID, "rx", "anc", 2),
             "l'essence « data » (vocabulaire NMOS) et « anc » (vocabulaire moteur) mènent au même flux")
    verifier(metrics.etat_flux(VMID, "rx", "video", 99) is None, "slot inexistant → None")

    print("\nÉCHECS : %d" % len(ECHECS))
    for e in ECHECS:
        print("  - " + e)
    return 1 if ECHECS else 0


if __name__ == "__main__":
    sys.exit(main())
