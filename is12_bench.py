# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Banc de conformité IS-12 / BCP-008 — À LANCER À LA MAIN, jamais depuis l'orchestrateur.

    ./venv/bin/python services/nmos/is12_bench.py

Monte le serveur IS-12 dans le process, s'y connecte en parlant RFC 6455 à la main, et vérifie
les points qui font l'interopérabilité : poignée de main, formats de message, identifiants de
propriétés et de méthodes, héritage des classes, touchpoints, et surtout les RÈGLES DE
TEMPORISATION de la BCP-008 — celles qu'on ne remarque pas quand elles sont fausses (un statut
qui remonte trop vite au vert, un compteur qui compte deux fois).

Le banc n'écrit RIEN : il remplace en mémoire, dans SON process, l'état NMOS et les caches de
télémétrie par un jeu d'essai, et écoute sur un port éphémère. Le lancer sur la machine de
production ne touche donc pas à l'orchestrateur qui y tourne.

Pourquoi ce fichier existe alors que le projet n'a pas de tests : parce que le mode de panne de
ce genre de code est SILENCIEUX. Une propriété publiée à 4p11 au lieu de 4p12 ne lève aucune
erreur, n'apparaît dans aucun journal, et se voit seulement le jour où un contrôleur tiers
affiche un statut faux — c'est-à-dire au pire moment."""
import base64, json, os, socket, struct, sys, time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from services import nmos
from services.nmos import is12, modele, ncp, monitors
from app import metrics, node_health

PORT = 45877
ECHECS = []


def verifier(cond, quoi):
    print(("  ok   " if cond else "  ÉCHEC ") + quoi)
    if not cond:
        ECHECS.append(quoi)


# ─── Jeu d'essai : 1 receiver vidéo + 1 sender vidéo sur un moteur 2110_io fictif ────────────
VMID = 999999
RID = "11111111-1111-1111-1111-111111111111"
SID = "22222222-2222-2222-2222-222222222222"

nmos._receivers.clear(); nmos._senders.clear()
nmos._recv_state.clear(); nmos._send_state.clear()
nmos._receivers[RID] = {"label": "Rx 1 (video)"}
nmos._senders[SID] = {"label": "Tx 1 (video)"}
nmos._recv_state[RID] = {"vmid": VMID, "recv_idx": 0, "essence": "video",
                         "active": {"master_enable": True}}
nmos._send_state[SID] = {"vmid": VMID, "tx_idx": 0, "essence": "video",
                         "active": {"master_enable": True}}

# Télémétrie injectée (ce que la boucle de surveillance aurait relevé sur :8080).
metrics.flux_etat_cache[VMID] = {
    "rx:video:0": {"mode": "mtl", "fps": 50.0, "frame_index": 10, "stalled": False,
                   "signal": {}, "latency_ms": 12.0},
    "tx:video:0": {"fps": 50.0, "fps_nominal": 50.0, "fps_source": 50.0, "repeats": 0,
                   "late": 3, "inputs_latency_ms": {"x": 5}, "stalled": False, "signal": {}},
}
metrics.nic_link_cache[VMID] = {"up": ["ens1f0"], "down": []}
node_health._last["34"] = {"ptp": {"running": True, "locked": True, "synced": True,
                                 "gm_id": "00:0c:ec:ff:fe:0a:2b:a1", "iface": "ens1f0"}}


class ClientWS:
    def __init__(self, port, chemin=is12.CHEMIN):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=5)
        cle = base64.b64encode(os.urandom(16)).decode()
        self.s.sendall(("GET %s HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\n"
                        "Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\n"
                        "Sec-WebSocket-Version: 13\r\n\r\n" % (chemin, cle)).encode())
        self.f = self.s.makefile("rb")
        self.notifications = []
        ligne = self.f.readline()
        self.statut = ligne.decode().strip()
        attendu = base64.b64encode(
            __import__("hashlib").sha1((cle + is12.GUID_WS).encode()).digest()).decode()
        self.accept_ok = False
        while True:
            l = self.f.readline()
            if not l or l in (b"\r\n", b"\n"):
                break
            if l.lower().startswith(b"sec-websocket-accept:"):
                self.accept_ok = l.split(b":", 1)[1].strip().decode() == attendu

    def envoyer(self, obj, opcode=0x1, charge=None):
        data = charge if charge is not None else json.dumps(obj).encode()
        masque = os.urandom(4)
        n = len(data)
        if n < 126:
            e = struct.pack("!BB", 0x80 | opcode, 0x80 | n)
        elif n < 1 << 16:
            e = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, n)
        else:
            e = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, n)
        self.s.sendall(e + masque + bytes(b ^ masque[i & 3] for i, b in enumerate(data)))

    def lire(self, timeout=5):
        self.s.settimeout(timeout)
        b1, b2 = struct.unpack("!BB", self.f.read(2))
        opcode, n = b1 & 0x0F, b2 & 0x7F
        if n == 126:
            n = struct.unpack("!H", self.f.read(2))[0]
        elif n == 127:
            n = struct.unpack("!Q", self.f.read(8))[0]
        charge = self.f.read(n) if n else b""
        if opcode == 0x1:
            return json.loads(charge.decode())
        return {"_opcode": opcode, "_charge": charge}

    def commander(self, oid, level, index, args=None, handle=1):
        self.envoyer({"messageType": 0, "commands": [
            {"handle": handle, "oid": oid, "methodId": {"level": level, "index": index},
             "arguments": args or {}}]})
        return self.reponse()

    def reponse(self, timeout=5):
        """Les notifications arrivent a tout moment : on les met de cote au lieu de les prendre
        pour la reponse a la commande en cours."""
        while True:
            m = self.lire(timeout)
            if m.get("messageType") == 2:
                self.notifications.extend(m["notifications"])
                continue
            return m

    def get(self, oid, level, index):
        r = self.commander(oid, 1, 1, {"id": {"level": level, "index": index}})
        return r["responses"][0]["result"]

    def fermer(self):
        # makefile() garde une reference sur le fd : sans fermer le fichier, socket.close() ne
        # ferme rien et le serveur ne voit jamais l'EOF.
        for x in (self.f, self.s):
            try:
                x.close()
            except OSError:
                pass


def main():
    # Rien ne doit dépendre de la base ni du parc réel : le banc se suffit à lui-même.
    nmos._get_node_id = lambda: "deadbeef-0000-0000-0000-000000000000"
    modele._type_conteneur = lambda vmid: "2110_io"
    monitors._ptp_du_conteneur = lambda vmid: node_health._last.get("34", {}).get("ptp")
    from app import database
    database.db_get_setting = lambda k, d=None: {"nmos_is12_port": PORT}.get(k, d)
    from app import metrics as _m
    _m.alarmes_slot = lambda *a, **k: {"drapeaux": {}, "niveau": "warning"}
    is12.PORT_DEFAUT = PORT

    print("\n=== démarrage ===")
    verifier(is12.start(), "le serveur démarre")
    verifier(len(modele._moniteurs) == 2, "2 monitors construits (1 RX, 1 TX)")

    print("\n=== poignée de main ===")
    c = ClientWS(PORT)
    verifier("101" in c.statut, "réponse 101 Switching Protocols")
    verifier(c.accept_ok, "Sec-WebSocket-Accept conforme (RFC 6455)")

    print("\n=== exploration du modèle ===")
    r = c.get(1, 2, 2)                                   # racine → members
    verifier(r["status"] == 200, "Get(root, 2p2) = 200")
    roles = {m["role"] for m in r["value"]}
    verifier({"DeviceManager", "ClassManager", "receivers", "senders"} <= roles,
             "la racine porte les managers et les deux blocs : %s" % sorted(roles))
    verifier(all(m["classId"] and m["oid"] for m in r["value"]),
             "chaque descripteur de membre porte classId et oid")

    r = c.commander(1, 2, 4, {"classId": [1, 2, 2], "includeDerived": True, "recurse": True})
    membres = r["responses"][0]["result"]["value"]
    verifier(len(membres) == 2, "FindMembersByClassId([1,2,2], derived, recurse) → 2 monitors")
    rx = next(m for m in membres if m["classId"] == [1, 2, 2, 1])
    tx = next(m for m in membres if m["classId"] == [1, 2, 2, 2])
    verifier(True, "NcReceiverMonitor et NcSenderMonitor trouvés")

    print("\n=== touchpoints (BCP-008 §Touchpoints) ===")
    tp = c.get(rx["oid"], 1, 7)["value"]
    verifier(isinstance(tp, list) and len(tp) == 1, "un seul touchpoint")
    verifier(tp[0]["contextNamespace"] == "x-nmos"
             and tp[0]["resource"] == {"resourceType": "receiver", "id": RID},
             "touchpoint x-nmos → receiver %s" % RID[:8])
    tps = c.get(tx["oid"], 1, 7)["value"]
    verifier(tps[0]["resource"]["resourceType"] == "sender", "touchpoint sender côté TX")

    print("\n=== statuts BCP-008 (tout est sain) ===")
    time.sleep(modele.PERIODE_ECH_S * 2)
    verifier(c.get(rx["oid"], 3, 1)["value"] == monitors.HEALTHY, "receiver overallStatus = Healthy")
    verifier(c.get(rx["oid"], 4, 4)["value"] == monitors.HEALTHY, "connectionStatus = Healthy")
    verifier(c.get(rx["oid"], 4, 11)["value"] == monitors.HEALTHY, "streamStatus = Healthy")
    verifier(c.get(rx["oid"], 4, 1)["value"] == monitors.ALL_UP, "linkStatus = AllUp")
    verifier(c.get(rx["oid"], 4, 7)["value"] == monitors.HEALTHY, "externalSynchronizationStatus = Healthy")
    verifier(c.get(rx["oid"], 4, 10)["value"] == "00:0c:ec:ff:fe:0a:2b:a1 on ens1f0",
             "synchronizationSourceId reprend le grandmaster et son interface")
    verifier(c.get(rx["oid"], 3, 3)["value"] == 3, "statusReportingDelay vaut 3 s par défaut")
    verifier(c.get(tx["oid"], 4, 4)["value"] == monitors.HEALTHY, "transmissionStatus = Healthy")
    verifier(c.get(tx["oid"], 4, 11)["value"] == monitors.HEALTHY, "essenceStatus = Healthy")

    print("\n=== compteurs d'erreurs de transmission (BCP-008-02) ===")
    r = c.commander(tx["oid"], 4, 1)["responses"][0]["result"]
    noms = {x["name"]: x["value"] for x in r["value"]}
    verifier(noms.get("late frames") == 0,
             "les compteurs repartent de zero a l'activation (autoResetCountersAndMessages)")
    metrics.flux_etat_cache[VMID]["tx:video:0"]["late"] = 11
    noms = {x["name"]: x["value"]
            for x in c.commander(tx["oid"], 4, 1)["responses"][0]["result"]["value"]}
    verifier(noms.get("late frames") == 8,
             "le compteur publie le DELTA depuis la RAZ (11 - 3), pas la valeur cumulee du moteur")
    r = c.commander(rx["oid"], 4, 1)["responses"][0]["result"]
    verifier(r["value"] == [], "GetLostPacketCounters renvoie une collection VIDE (rien n'est mesuré)")

    print("\n=== abonnement et notification ===")
    c.envoyer({"messageType": 3, "subscriptions": [rx["oid"], 999999]})
    r = c.lire()
    verifier(r["messageType"] == 4 and r["subscriptions"] == [rx["oid"]],
             "SubscriptionResponse filtre l'oid invalide")
    metrics.flux_etat_cache[VMID]["rx:video:0"]["stalled"] = True
    fin = time.time() + 6
    while time.time() < fin and len(c.notifications) < 3:
        try:
            m = c.lire(timeout=2)
        except (socket.timeout, struct.error, OSError):
            break
        if m.get("messageType") == 2:
            c.notifications.extend(m["notifications"])
    recu = c.notifications
    props = {(n["eventData"]["propertyId"]["level"], n["eventData"]["propertyId"]["index"]): n
             for n in recu}
    verifier((4, 4) in props, "PropertyChanged reçu sur connectionStatus (4p4)")
    verifier(props.get((4, 4), {}).get("eventData", {}).get("value") == monitors.UNHEALTHY,
             "connectionStatus notifié à Unhealthy")
    verifier(all(n["oid"] == rx["oid"] for n in recu), "toutes les notifications portent l'oid abonné")
    verifier(c.get(rx["oid"], 4, 6)["value"] == 1,
             "connectionStatusTransitionCounter incrémenté une seule fois")
    verifier(c.get(rx["oid"], 4, 5)["value"], "connectionStatusMessage explique la cause")
    verifier(c.get(rx["oid"], 3, 1)["value"] == monitors.UNHEALTHY,
             "overallStatus prend le pire des domaines")

    print("\n=== temporisation BCP (amélioration retardée) ===")
    metrics.flux_etat_cache[VMID]["rx:video:0"]["stalled"] = False
    time.sleep(1.5)
    verifier(c.get(rx["oid"], 4, 4)["value"] == monitors.UNHEALTHY,
             "1,5 s après le retour à la normale, le statut est ENCORE Unhealthy (délai 3 s)")
    time.sleep(2.5)
    verifier(c.get(rx["oid"], 4, 4)["value"] == monitors.HEALTHY,
             "4 s après, l'amélioration est publiée")
    verifier(str(c.get(rx["oid"], 4, 5)["value"] or "").startswith("Previously: "),
             "le message conserve la cause précédente préfixée « Previously: »")

    print("\n=== désactivation : Inactive immédiat, sans délai ===")
    nmos._recv_state[RID]["active"]["master_enable"] = False
    time.sleep(modele.PERIODE_ECH_S * 2)
    verifier(c.get(rx["oid"], 4, 4)["value"] == monitors.INACTIVE, "connectionStatus = Inactive")
    verifier(c.get(rx["oid"], 3, 1)["value"] == monitors.INACTIVE, "overallStatus = Inactive")
    verifier(c.get(rx["oid"], 4, 11)["value"] == monitors.INACTIVE, "streamStatus = Inactive")

    print("\n=== intention : slot TX au repos n'est PAS en panne ===")
    metrics.flux_etat_cache[VMID]["tx:video:0"]["fps_nominal"] = 0
    metrics.flux_etat_cache[VMID]["tx:video:0"]["stalled"] = True
    time.sleep(modele.PERIODE_ECH_S * 2)
    verifier(c.get(tx["oid"], 3, 1)["value"] == monitors.INACTIVE,
             "un slot sans cadence nominale est Inactive, pas Unhealthy")

    print("\n=== découverte des classes (NcClassManager) ===")
    r = c.commander(3, 3, 1, {"classId": [1, 2, 2, 1], "includeInherited": True})["responses"][0]["result"]
    noms = [p["name"] for p in r["value"]["properties"]]
    verifier(r["value"]["name"] == "NcReceiverMonitor", "GetControlClass rend NcReceiverMonitor")
    verifier("oid" in noms and "enabled" in noms and "overallStatus" in noms and "streamStatus" in noms,
             "l'héritage NcObject → NcWorker → NcStatusMonitor → NcReceiverMonitor est complet")
    r = c.commander(3, 3, 1, {"classId": [1, 2, 2, 1], "includeInherited": False})["responses"][0]["result"]
    verifier(all(p["id"]["level"] == 4 for p in r["value"]["properties"]),
             "sans héritage, seules les propriétés de niveau 4 sont rendues")
    r = c.commander(3, 3, 2, {"name": "NcConnectionStatus", "includeInherited": True})["responses"][0]["result"]
    verifier([i["name"] for i in r["value"]["items"]][0] == "Inactive",
             "GetDatatype rend l'énumération NcConnectionStatus")

    print("\n=== écritures autorisées et refusées ===")
    r = c.commander(rx["oid"], 1, 2, {"id": {"level": 3, "index": 3}, "value": 5})["responses"][0]["result"]
    verifier(r["status"] == 200 and c.get(rx["oid"], 3, 3)["value"] == 5,
             "statusReportingDelay est modifiable (exigence BCP)")
    r = c.commander(rx["oid"], 1, 2, {"id": {"level": 3, "index": 1}, "value": 1})["responses"][0]["result"]
    verifier(r["status"] == ncp.READONLY, "overallStatus refuse l'écriture (405 Readonly)")
    r = c.commander(rx["oid"], 1, 1, {"id": {"level": 9, "index": 9}})["responses"][0]["result"]
    verifier(r["status"] == ncp.PROPERTY_NOT_IMPLEMENTED, "propriété inconnue → 502")
    r = c.commander(123456, 1, 1, {"id": {"level": 1, "index": 1}})["responses"][0]["result"]
    verifier(r["status"] == ncp.BAD_OID, "oid inconnu → 404 BadOid")
    r = c.commander(rx["oid"], 7, 7)["responses"][0]["result"]
    verifier(r["status"] == ncp.METHOD_NOT_IMPLEMENTED, "méthode inconnue → 501")

    print("\n=== messages mal formés ===")
    c.envoyer(None, charge=b"{pas du json")
    r = c.lire()
    verifier(r["messageType"] == 5 and r["status"] == ncp.BAD_COMMAND_FORMAT,
             "JSON invalide → message Error (type 5)")
    c.envoyer({"commands": []})
    verifier(c.lire()["messageType"] == 5, "messageType absent → Error")
    c.envoyer({"messageType": 0})
    verifier(c.lire()["messageType"] == 5, "Command sans tableau commands → Error")

    print("\n=== ping / pong ===")
    c.envoyer(None, opcode=0x9, charge=b"bobi")
    r = c.lire()
    verifier(r.get("_opcode") == 0xA and r.get("_charge") == b"bobi", "pong renvoie la charge du ping")

    print("\n=== commandes multiples en un message ===")
    c.envoyer({"messageType": 0, "commands": [
        {"handle": 7, "oid": rx["oid"], "methodId": {"level": 1, "index": 1},
         "arguments": {"id": {"level": 1, "index": 5}}},
        {"handle": 8, "oid": tx["oid"], "methodId": {"level": 1, "index": 1},
         "arguments": {"id": {"level": 1, "index": 5}}}]})
    r = c.lire()
    verifier([x["handle"] for x in r["responses"]] == [7, 8], "deux réponses appariées par handle")
    verifier(r["responses"][0]["result"]["value"].startswith("receiver_"),
             "le rôle du monitor est dérivé de l'UUID de la ressource IS-04 (stable)")

    print("\n=== annonce IS-04 ===")
    dev = nmos._build_cluster_device_resource("x", "0:0")
    ncp_ctrl = [x for x in dev["controls"] if x["type"] == is12.TYPE_CONTROL]
    verifier(len(ncp_ctrl) == 1 and ncp_ctrl[0]["href"].startswith("ws://"),
             "le Device annonce urn:x-nmos:control:ncp/v1.0 en ws:// : %s"
             % (ncp_ctrl[0]["href"] if ncp_ctrl else "absent"))

    c.fermer()
    fin = time.time() + 3
    while time.time() < fin and is12._sessions:
        time.sleep(0.1)
    verifier(len(is12._sessions) == 0, "la session est libérée à la fermeture (%.1f s)"
             % (3 - max(0, fin - time.time())))
    is12.stop()
    verifier(not is12.actif(), "arrêt propre")
    verifier(is12.TYPE_CONTROL not in [x["type"] for x in nmos._build_cluster_device_resource("x", "0:0")["controls"]],
             "IS-12 arrêté n'est plus annoncé en IS-04")

    print("\n" + "=" * 60)
    print("ÉCHECS : %d" % len(ECHECS))
    for e in ECHECS:
        print("  - " + e)
    return 1 if ECHECS else 0


if __name__ == "__main__":
    sys.exit(main())
