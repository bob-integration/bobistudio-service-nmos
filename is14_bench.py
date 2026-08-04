# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Banc de conformité IS-14 — À LANCER À LA MAIN, jamais depuis l'orchestrateur.

    ./venv/bin/python services/nmos/is14_bench.py

Monte l'application Flask en client de test et parcourt la Configuration API comme le ferait un
contrôleur : découverte des chemins de rôles, lecture et écriture d'une propriété, invocation
d'une méthode, puis sauvegarde / validation / restauration.

Ce qui se casse en silence ici : un code HTTP qui ne distingue plus « chemin inconnu » (404) de
« écriture refusée » (200 + status 405 dans le corps), une restauration qui prétend avoir
appliqué une propriété en lecture seule, ou une validation qui MODIFIE le modèle — la spec exige
qu'elle n'y touche pas, et rien dans la réponse ne le trahirait.

Aucune écriture en base : seul le réglage d'activation IS-14 est simulé en mémoire."""
import json, os, sys

_RACINE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, _RACINE)

from services import nmos
from services.nmos import is14, modele, monitors, ncp
from app import metrics, node_health

ECHECS = []


def verifier(c, quoi):
    print(("  ok   " if c else "  ÉCHEC ") + quoi)
    if not c:
        ECHECS.append(quoi)


VMID, RID, SID = 999999, "aaaaaaaa-0000-0000-0000-000000000001", "bbbbbbbb-0000-0000-0000-000000000002"


def semer():
    nmos._receivers.clear(); nmos._senders.clear()
    nmos._recv_state.clear(); nmos._send_state.clear()
    nmos._receivers[RID] = {"label": "Rx 1 (video)"}
    nmos._senders[SID] = {"label": "Tx 1 (video)"}
    nmos._recv_state[RID] = {"vmid": VMID, "recv_idx": 0, "essence": "video",
                             "active": {"master_enable": True}}
    nmos._send_state[SID] = {"vmid": VMID, "tx_idx": 0, "essence": "video",
                             "active": {"master_enable": True}}
    metrics.flux_etat_cache[VMID] = {
        "rx:video:0": {"mode": "mtl", "fps": 50.0, "frame_index": 1, "stalled": False, "signal": {}},
        "tx:video:0": {"fps": 50.0, "fps_nominal": 50.0, "fps_source": 50.0, "repeats": 0,
                       "late": 0, "inputs_latency_ms": {"a": 1}, "stalled": False, "signal": {}}}
    metrics.nic_link_cache[VMID] = {"up": ["ens1f0"], "down": []}
    node_health._last["34"] = {"ptp": {"running": True, "locked": True, "synced": True,
                                       "gm_id": "00:0c:ec:ff:fe:0a:2b:a1"}}
    modele._type_conteneur = lambda vmid: "2110_io"
    monitors._ptp_du_conteneur = lambda vmid: node_health._last["34"]["ptp"]
    metrics.alarmes_slot = lambda *a, **k: {"drapeaux": {}, "niveau": "warning"}
    nmos._get_node_id = lambda: "deadbeef-0000-0000-0000-000000000000"
    is14.actif = lambda: True          # réglage simulé : rien n'est écrit en base


def main():
    semer()
    import main as appmod
    cli = appmod.app.test_client()
    B = is14.RACINE

    def get(u):
        r = cli.get(u)
        return r.status_code, (r.get_json() if r.data else None)

    print("=== modèle partagé ===")
    modele.acquerir("is14")
    verifier(modele.appareil() is not None, "le modèle est bâti")
    verifier(len(modele._moniteurs) == 2, "2 monitors (le modèle est le MÊME qu'IS-12)")

    print("\n=== découverte ===")
    verifier(get("/x-nmos/configuration/") == (200, ["v1.0/"]), "base → v1.0/")
    verifier(get(B + "/") == (200, ["rolePaths/"]), "v1.0 → rolePaths/")
    c, chemins = get(B + "/rolePaths/")
    verifier(c == 200 and "root/" in chemins, "rolePaths liste la racine")
    verifier("root.DeviceManager/" in chemins and "root.ClassManager/" in chemins
             and "root.BulkPropertiesManager/" in chemins, "les trois managers sont adressables")
    rx_path = next((p[:-1] for p in chemins if p.startswith("root.receivers.receiver_")), None)
    verifier(rx_path is not None, "le monitor de receiver a un chemin de rôles : %s" % rx_path)
    verifier(get(B + "/rolePaths/root/") == (200, ["bulkProperties/", "descriptor/",
                                                   "methods/", "properties/"]),
             "un chemin expose ses quatre sous-ressources")

    print("\n=== descripteurs ===")
    c, d = get(B + "/rolePaths/%s/descriptor" % rx_path)
    verifier(c == 200 and d["value"]["name"] == "NcReceiverMonitor", "descripteur de classe")
    noms = [p["name"] for p in d["value"]["properties"]]
    verifier("oid" in noms and "streamStatus" in noms, "l'héritage est inclus")
    c, d = get(B + "/rolePaths/%s/properties/4p4/descriptor" % rx_path)
    verifier(c == 200 and d["value"]["name"] == "NcConnectionStatus", "descripteur de type")

    print("\n=== lecture de propriétés ===")
    c, d = get(B + "/rolePaths/%s/properties/3p1/value" % rx_path)
    verifier(c == 200 and d["status"] == 200, "overallStatus lisible")
    c, d = get(B + "/rolePaths/root/properties/1p6/value")
    verifier(c == 200 and d["value"] == "Root block", "userLabel de la racine")
    c, props = get(B + "/rolePaths/%s/properties/" % rx_path)
    verifier(c == 200 and "4p14/" in props, "la liste des propriétés couvre le niveau 4")
    c, meths = get(B + "/rolePaths/%s/methods/" % rx_path)
    verifier(c == 200 and "4m3/" in meths, "la liste des méthodes contient ResetCountersAndMessages")

    print("\n=== ce que l'URL désigne : 404 ; ce que le modèle refuse : 200 + status ===")
    verifier(get(B + "/rolePaths/root.nexistepas/")[0] == 404, "chemin inconnu → 404 HTTP")
    verifier(get(B + "/rolePaths/root/properties/9p9/value")[0] == 404, "propriété inconnue → 404")
    verifier(get(B + "/rolePaths/root/properties/pasunid/value")[0] == 400, "id mal formé → 400")
    r = cli.put(B + "/rolePaths/%s/properties/3p1/value" % rx_path, json={"value": 1})
    verifier(r.status_code == 200 and r.get_json()["status"] == ncp.READONLY,
             "écriture sur une propriété en lecture seule → 200 HTTP + status 405")

    print("\n=== écriture autorisée ===")
    r = cli.put(B + "/rolePaths/%s/properties/3p3/value" % rx_path, json={"value": 7})
    verifier(r.status_code == 200 and r.get_json()["status"] == 200, "statusReportingDelay modifiable")
    verifier(get(B + "/rolePaths/%s/properties/3p3/value" % rx_path)[1]["value"] == 7,
             "la valeur écrite est relue")
    r = cli.put(B + "/rolePaths/%s/properties/3p3/value" % rx_path, json={})
    verifier(r.status_code == 400, "corps sans « value » → 400")

    print("\n=== invocation de méthode ===")
    r = cli.patch(B + "/rolePaths/%s/methods/4m1" % rx_path, json={"arguments": {}})
    verifier(r.status_code == 200 and r.get_json()["value"] == [],
             "GetLostPacketCounters → collection vide (rien n'est mesuré)")
    r = cli.patch(B + "/rolePaths/%s/methods/4m3" % rx_path, json={"arguments": {}})
    verifier(r.status_code == 200 and r.get_json()["status"] == 200, "ResetCountersAndMessages")
    r = cli.patch(B + "/rolePaths/%s/methods/4m3" % rx_path, json={})
    verifier(r.status_code == 400, "PATCH sans « arguments » → 400")

    print("\n=== sauvegarde ===")
    c, d = get(B + "/rolePaths/root/bulkProperties?recurse=true&includeDescriptors=true")
    verifier(c == 200 and d["status"] == 200, "sauvegarde complète servie")
    jeu = d["value"]
    verifier(bool(jeu.get("validationFingerprint")), "empreinte de validation présente")
    chemins_sauves = {".".join(v["path"]) for v in jeu["values"]}
    verifier("root" in chemins_sauves and rx_path in chemins_sauves,
             "la sauvegarde récursive couvre la racine ET les monitors (%d objets)" % len(jeu["values"]))
    verifier(all(ph["descriptor"] is not None for v in jeu["values"] for ph in v["values"]),
             "avec includeDescriptors, chaque propriété porte son descripteur")
    c, d2 = get(B + "/rolePaths/root/bulkProperties?recurse=true&includeDescriptors=false")
    verifier(all(ph["descriptor"] is None for v in d2["value"]["values"] for ph in v["values"]),
             "sans descripteurs, ils sont à null")
    verifier(not any(".ClassManager" in ".".join(v["path"]) for v in d2["value"]["values"]),
             "sans descripteurs, le ClassManager est OMIS (exigence de la spec)")
    c, d3 = get(B + "/rolePaths/root/bulkProperties?recurse=false")
    verifier(len(d3["value"]["values"]) == 1, "recurse=false ne rend que l'objet visé")

    print("\n=== validation : ne DOIT rien changer ===")
    # On fabrique un jeu qui remet statusReportingDelay à 3 et tente d'écraser une lecture seule.
    jeu_test = {"validationFingerprint": jeu["validationFingerprint"], "values": [{
        "path": rx_path.split("."), "dependencyPaths": [], "allowedMembersClasses": [],
        "isRebuildable": False, "values": [
            {"id": {"level": 3, "index": 3}, "descriptor": None, "value": 3},
            {"id": {"level": 3, "index": 1}, "descriptor": None, "value": 3},
            {"id": {"level": 9, "index": 9}, "descriptor": None, "value": "?"}]}]}
    r = cli.patch(B + "/rolePaths/%s/bulkProperties" % rx_path,
                  json={"arguments": {"dataSet": jeu_test, "recurse": False, "restoreMode": 0}})
    v = r.get_json()
    verifier(r.status_code == 200 and v["status"] == 200, "validation servie")
    entree = v["value"][0]
    verifier(entree["status"] == ncp.RESTORE_OK, "statut de validation Ok")
    types = {n["name"]: n["noticeType"] for n in entree["notices"]}
    verifier(types.get("overallStatus") == ncp.NOTICE_WARNING,
             "la propriété en lecture seule produit un avertissement, pas une erreur")
    verifier(types.get("?") == ncp.NOTICE_WARNING, "une propriété inconnue est signalée, sans échec")
    verifier(get(B + "/rolePaths/%s/properties/3p3/value" % rx_path)[1]["value"] == 7,
             "★ la validation n'a RIEN modifié (délai toujours à 7)")

    print("\n=== restauration ===")
    r = cli.put(B + "/rolePaths/%s/bulkProperties" % rx_path,
                json={"arguments": {"dataSet": jeu_test, "recurse": False, "restoreMode": 0}})
    verifier(r.status_code == 200 and r.get_json()["value"][0]["status"] == ncp.RESTORE_OK,
             "restauration acceptée")
    verifier(get(B + "/rolePaths/%s/properties/3p3/value" % rx_path)[1]["value"] == 3,
             "la propriété modifiable a bien été restaurée (7 → 3)")

    print("\n=== restauration « Rebuild » : acceptée, avec réserves ===")
    r = cli.put(B + "/rolePaths/%s/bulkProperties" % rx_path,
                json={"arguments": {"dataSet": jeu_test, "recurse": False, "restoreMode": 1}})
    v = r.get_json()["value"][0]
    verifier(v["status"] == ncp.RESTORE_OK, "un Rebuild est accepté même sans objet reconstructible")
    verifier(any("Rebuild not supported" in n["noticeMessage"] for n in v["notices"]),
             "…et le dit explicitement dans les notices")

    print("\n=== objet hors du jeu de sauvegarde ===")
    r = cli.put(B + "/rolePaths/root/bulkProperties",
                json={"arguments": {"dataSet": jeu_test, "recurse": True, "restoreMode": 0}})
    sorties = {".".join(e["path"]): e["status"] for e in r.get_json()["value"]}
    verifier(sorties.get("root") == ncp.RESTORE_NOT_FOUND,
             "un objet de la portée absent du jeu est rapporté NotFound, pas passé sous silence")
    verifier(sorties.get(rx_path) == ncp.RESTORE_OK, "…et celui qui y est, restauré")

    print("\n=== corps malformés ===")
    verifier(cli.put(B + "/rolePaths/root/bulkProperties", json={"arguments": {}}).status_code == 400,
             "dataSet absent → 400")

    print("\n=== annonce IS-04 ===")
    ctrl = [x for x in nmos._build_cluster_device_resource("x", "0:0")["controls"]
            if x["type"] == is14.TYPE_CONTROL]
    verifier(len(ctrl) == 1 and ctrl[0]["href"].endswith("/x-nmos/configuration/v1.0"),
             "le Device annonce urn:x-nmos:control:configuration/v1.0")

    print("\n=== désactivé, l'API n'existe pas ===")
    is14.actif = lambda: False
    verifier(get(B + "/rolePaths/")[0] == 404, "IS-14 éteint → 404")
    is14.actif = lambda: True

    modele.liberer("is14")
    verifier(modele.appareil() is None, "modèle démonté au dernier relâchement")

    print("\n" + "=" * 60)
    print("ÉCHECS : %d" % len(ECHECS))
    for e in ECHECS:
        print("  - " + e)
    return 1 if ECHECS else 0


if __name__ == "__main__":
    sys.exit(main())
