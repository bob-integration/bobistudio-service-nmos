"""IS-07 — le tally publié en NMOS (Event & Tally Specification, v1.0.1).

Notre tally circule aujourd'hui en **TSL 5.0 UMD**, qui est ce que parlent les vrais pupitres.
IS-07, c'est ce que parlerait un contrôleur NMOS. Les deux ne s'opposent pas : le même état est
publié sur deux transports, pour deux publics.

═══ Ce qui est publié, et comment il est adressé ═════════════════════════════════════════════

Notre tally est adressé **par FLUX** : la table de correspondance TSL associe
`(connexion, source_shm) → tsl_index`, et l'état vit sous `(tsl_index, niveau)`. On publie donc
**une Source IS-07 par (flux, niveau)** — ce qui s'aligne exactement sur les flux MXL déjà exposés
en BCP-007-03, sans inventer une seconde façon de désigner les mêmes signaux.

Les niveaux LH / RH / TT viennent du modèle TSL. On les publie **tels quels**, dans le libellé :
les traduire en « program » et « preview » serait une convention de site, et la présumer ferait
mentir l'étiquette chez qui ne l'applique pas.

═══ Le type d'événement : une ÉNUMÉRATION, pas un booléen ════════════════════════════════════

Notre tally vaut `off`, `red`, `green` ou `amber`. IS-07 prévoit exactement ce cas :
« Enum: `{base}/enum/{Name}` ». On publie donc `string/enum/Tally`.

Le réduire à un booléen aurait demandé de décider quelle couleur signifie « à l'antenne » — une
décision d'exploitation qui n'appartient pas à ce module, et qui aurait effacé l'ambre et le vert.

═══ Le transport WebSocket ═══════════════════════════════════════════════════════════════════

Le Sender n'est publié QUE si le transport est réellement servi (`nmos_is07_ws`) : annoncer un
Sender sans son serveur promettrait un abonnement qui n'arriverait jamais.

Protocole (IS-07 § Transport - Websocket), côté serveur :
  · le client envoie `{"command": "subscription", "sources": [...]}` → on **renvoie aussitôt
    l'état courant** de chaque source demandée. « Each time a client submits its subscriptions
    list… the server will resend all the current states » ;
  · le client bat toutes les 5 s (`{"command": "health", "timestamp": "sec:nsec"}`) ;
  · **au bout de 12 s sans battement**, on efface ses abonnements et on ferme — la spec le dit et
    le chiffre (2 battements manqués + 2 s de latence).

On pousse sur CHANGEMENT, en s'accrochant au signal du service TSL (`_tally_dirty`), comme le fait
son propre distributeur. Interroger en boucle aurait fabriqué de la latence là où la donnée est
déjà événementielle.
"""

import json
import logging
import socket
import socketserver
import struct
import threading
import time

from flask import jsonify

log = logging.getLogger(__name__)

VERSION = "v1.0"
BASE = "/x-nmos/events/" + VERSION

# IS-07 § Event types : « Enum: {base}/enum/{Name} ».
TYPE_EVENEMENT = "string/enum/Tally"
VALEURS = ("off", "red", "green", "amber")

# Niveaux TSL, tels que le service les range (`tally_base`, +1, +2).
NIVEAUX = ((0, "LH"), (1, "RH"), (2, "TT"))

PORT_DEFAUT = 5011          # IS-12 occupe 5010 ; même motif : Waitress ne sait pas faire l'upgrade
SANTE_TIMEOUT_S = 12        # IS-07 : 2 battements manqués (5 s) + 2 s de latence
TRANSPORT = "urn:x-nmos:transport:websocket"


def actif():
    """Réglage `nmos_is07` — FERMÉ par défaut, comme toute surface externe."""
    from . import _setting
    return str(_setting("nmos_is07", "0")).strip().lower() in ("1", "true", "on", "yes")


def _sid(shm, niveau):
    """Identité d'une Source IS-07. Dérivée du FLUX et du niveau — donc stable tant que le flux
    porte le même nom, et indépendante de l'index TSL, qui est une adresse de pupitre et peut être
    réattribué par une simple modification de table."""
    from . import _stable_uuid
    return _stable_uuid("is07:source:%s:%d" % (shm, niveau))


def _fid(shm, niveau):
    from . import _stable_uuid
    return _stable_uuid("is07:flow:%s:%d" % (shm, niveau))


def _sources():
    """[(shm, tsl_index, niveau, nom_niveau)] — un par (flux tallyé, niveau)."""
    try:
        from app.database import db_get_tsl_mappings_all
        mappings = db_get_tsl_mappings_all() or []
    except Exception as e:
        log.warning("nmos/is07 : table de correspondance TSL illisible (%s)", e)
        return []
    vus, out = set(), []
    for m in mappings:
        shm = m["source_shm"]
        idx = m["tsl_index"]
        # Un même flux peut être mappé sur PLUSIEURS connexions TSL (deux pupitres). Le tally reste
        # le même signal : on ne le publie qu'une fois, sinon un contrôleur verrait des doublons
        # qui changent ensemble sans qu'il puisse savoir lequel fait foi.
        if shm in vus:
            continue
        vus.add(shm)
        for niveau, nom in NIVEAUX:
            out.append((shm, idx, niveau, nom))
    return out


def _valeur(tsl_index, niveau):
    try:
        from services import tsl
        return tsl.get_tally_level(tsl_index, niveau)
    except Exception:
        return "off"


def _ts():
    """Horodatage IS-07 « secondes:nanosecondes ». Même forme que les versions de ressource."""
    t = time.time()
    return "%d:%09d" % (int(t), int((t - int(t)) * 1e9))


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Ressources IS-04
# ══════════════════════════════════════════════════════════════════════════════════════════════

def _snd(shm, niveau):
    from . import _stable_uuid
    return _stable_uuid("is07:sender:%s:%d" % (shm, niveau))


def transport_params(sid):
    """`transport_params` IS-05 d'un Sender IS-07 sur WebSocket (§ Transport - Websocket).

    « All senders on one NMOS device should offer the same `connection_uri` to allow the number of
    WebSocket connections needed to be reduced » — on sert donc UNE seule URI pour toutes les
    sources, et le client discrimine par `ext_is_07_source_id`."""
    from . import _get_host_address
    cible = _par_id().get(sid)
    if not cible:
        return {}
    return {
        "connection_uri": href(),
        "connection_authorization": False,
        "ext_is_07_source_id": sid,
        "ext_is_07_rest_api_url": "http://%s:5000%s/sources/%s"
                                  % (_get_host_address(), BASE, sid),
    }


def ressources(device_id, version):
    """{"sources": [...], "flows": [...], "senders": [...]} à verser au modèle IS-04.

    ★ Les Senders ne sont publiés QUE si le transport WebSocket est réellement servi : un Sender
    annonce une URI sur laquelle les états sont POUSSÉS, et l'annoncer sans serveur promettrait un
    abonnement qui n'arriverait jamais."""
    if not actif():
        return {"sources": [], "flows": [], "senders": [], "connection": {}}
    srcs, flows, senders, conn = [], [], [], {}
    for shm, _idx, niveau, nom in _sources():
        sid, fid = _sid(shm, niveau), _fid(shm, niveau)
        lbl = "Tally %s — %s" % (nom, shm)
        srcs.append({
            "id": sid, "version": version, "label": lbl,
            "description": "Tally %s du flux %s (IS-07)" % (nom, shm),
            "tags": {"urn:x-mxl:shm": [shm], "urn:x-mxl:tally-level": [nom]},
            "device_id": device_id, "parents": [],
            "format": "urn:x-nmos:format:data",
            "caps": {}, "clock_name": None,
            "event_type": TYPE_EVENEMENT,
        })
        flows.append({
            "id": fid, "version": version, "label": lbl,
            "description": "Flow d'événements tally (IS-07)",
            "tags": {"urn:x-mxl:shm": [shm]},
            "device_id": device_id, "source_id": sid, "parents": [],
            "format": "urn:x-nmos:format:data",
            "media_type": "application/json",
            "event_type": TYPE_EVENEMENT,
        })
        if ws_actif():
            from . import _primary_iface
            senders.append({
                "id": _snd(shm, niveau), "version": version, "label": lbl,
                "description": "Sender d'événements tally (IS-07 / WebSocket)",
                "tags": {"urn:x-mxl:shm": [shm]},
                "device_id": device_id, "flow_id": fid,
                "transport": TRANSPORT,
                # IS-07 n'a pas de fichier de transport : la connexion se décrit entièrement par
                # les transport_params, comme pour MXL.
                "manifest_href": None,
                "interface_bindings": [_primary_iface()],
                "subscription": {"receiver_id": None, "active": False},
                "caps": {},
            })
            etat = {"receiver_id": None, "master_enable": True,
                    "transport_params": [transport_params(sid)],
                    "activation": {"mode": None, "requested_time": None,
                                   "activation_time": None}}
            conn[_snd(shm, niveau)] = {"staged": etat, "active": json.loads(json.dumps(etat))}
    return {"sources": srcs, "flows": flows, "senders": senders, "connection": conn}


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Events API (REST)
# ══════════════════════════════════════════════════════════════════════════════════════════════

def _par_id():
    return {_sid(shm, niveau): (shm, idx, niveau, nom)
            for shm, idx, niveau, nom in _sources()}


def etat_source(sid):
    """Message STATE d'une Source, dans la forme exacte d'IS-07 § Message types."""
    cible = _par_id().get(sid)
    if not cible:
        return None
    shm, idx, niveau, _nom = cible
    horo = _ts()
    return {
        "identity": {"source_id": sid, "flow_id": _fid(shm, niveau)},
        "event_type": TYPE_EVENEMENT,
        "timing": {"creation_timestamp": horo, "origin_timestamp": horo},
        "payload": {"value": _valeur(idx, niveau)},
        "message_type": "state",
    }


def enregistrer(bp):
    def _ferme():
        return jsonify({"code": 501, "error": "IS-07 désactivé (réglage `nmos_is07`)",
                        "debug": ""}), 501

    @bp.route("/x-nmos/events/", methods=["GET"])
    def is07_racine():
        return jsonify([VERSION + "/"])

    @bp.route(BASE + "/", methods=["GET"])
    def is07_v_racine():
        if not actif():
            return _ferme()
        return jsonify(["sources/"])

    @bp.route(BASE + "/sources", methods=["GET"])
    @bp.route(BASE + "/sources/", methods=["GET"])
    def is07_sources():
        if not actif():
            return _ferme()
        return jsonify(["%s/" % s for s in _par_id()])

    @bp.route(BASE + "/sources/<sid>", methods=["GET"])
    @bp.route(BASE + "/sources/<sid>/", methods=["GET"])
    def is07_source(sid):
        if not actif():
            return _ferme()
        if sid not in _par_id():
            return jsonify({"code": 404, "error": "source inconnue", "debug": sid}), 404
        return jsonify(["state/", "type/"])

    @bp.route(BASE + "/sources/<sid>/type", methods=["GET"])
    @bp.route(BASE + "/sources/<sid>/type/", methods=["GET"])
    def is07_type(sid):
        if not actif():
            return _ferme()
        if sid not in _par_id():
            return jsonify({"code": 404, "error": "source inconnue", "debug": sid}), 404
        # IS-07 § Event and tally rest api : « the type definition object ».
        return jsonify({"type": TYPE_EVENEMENT, "values": [
            {"label": v, "value": v} for v in VALEURS]})

    @bp.route(BASE + "/sources/<sid>/state", methods=["GET"])
    @bp.route(BASE + "/sources/<sid>/state/", methods=["GET"])
    def is07_state(sid):
        if not actif():
            return _ferme()
        e = etat_source(sid)
        if e is None:
            return jsonify({"code": 404, "error": "source inconnue", "debug": sid}), 404
        return jsonify(e)


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Transport WebSocket (IS-07 § Transport - Websocket)
# ══════════════════════════════════════════════════════════════════════════════════════════════
# Serveur écrit à la main, comme celui d'IS-12 et pour la même raison : Waitress ne sait pas faire
# l'upgrade WebSocket, et aucune bibliothèque n'est installée. Le cadrage est celui de RFC 6455 ;
# côté serveur les trames sortantes ne sont PAS masquées, les entrantes DOIVENT l'être.

GUID_WS = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
TAILLE_MAX = 1 << 18

_serveur = None
_sessions = set()
_sessions_lock = threading.RLock()
_arret = threading.Event()


def ws_actif():
    from . import _setting
    return (actif()
            and str(_setting("nmos_is07_ws", "0")).strip().lower() in ("1", "true", "on", "yes"))


def _port():
    from . import _setting
    try:
        return int(_setting("nmos_is07_port", PORT_DEFAUT) or PORT_DEFAUT)
    except (TypeError, ValueError):
        return PORT_DEFAUT


def href():
    from . import _get_host_address
    return "ws://%s:%d" % (_get_host_address(), _port())


class _Session:
    """Un client connecté : ses abonnements et la date de son dernier battement."""

    def __init__(self, conn):
        self.conn = conn
        self.sources = set()
        self.sante = time.monotonic()
        self.lock = threading.Lock()

    def envoyer(self, message):
        charge = json.dumps(message).encode("utf-8")
        n = len(charge)
        entete = bytearray([0x81])
        if n < 126:
            entete.append(n)
        elif n < (1 << 16):
            entete.append(126); entete += struct.pack("!H", n)
        else:
            entete.append(127); entete += struct.pack("!Q", n)
        with self.lock:
            self.conn.sendall(bytes(entete) + charge)


class _Handler(socketserver.BaseRequestHandler):

    def handle(self):
        import base64
        import hashlib
        try:
            entete = self._lire_entete()
        except Exception:
            return
        cle = ""
        for ligne in entete.split("\r\n"):
            if ligne.lower().startswith("sec-websocket-key:"):
                cle = ligne.split(":", 1)[1].strip()
        if not cle:
            self.request.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            return
        accept = base64.b64encode(
            hashlib.sha1((cle + GUID_WS).encode("ascii")).digest()).decode("ascii")
        self.request.sendall(
            ("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
             "Sec-WebSocket-Accept: %s\r\n\r\n" % accept).encode("ascii"))

        sess = _Session(self.request)
        with _sessions_lock:
            _sessions.add(sess)
        try:
            self._boucle(sess)
        except Exception:
            pass
        finally:
            with _sessions_lock:
                _sessions.discard(sess)

    def _lire_entete(self):
        tampon = b""
        self.request.settimeout(5)
        while b"\r\n\r\n" not in tampon:
            bloc = self.request.recv(4096)
            if not bloc:
                raise IOError("fermé")
            tampon += bloc
            if len(tampon) > 65536:
                raise IOError("en-tête déraisonnable")
        self._reste = tampon.split(b"\r\n\r\n", 1)[1]
        return tampon.split(b"\r\n\r\n", 1)[0].decode("latin-1")

    def _lire_exactement(self, n):
        while len(self._reste) < n:
            bloc = self.request.recv(65536)
            if not bloc:
                raise IOError("fermé")
            self._reste += bloc
        out, self._reste = self._reste[:n], self._reste[n:]
        return out

    def _boucle(self, sess):
        self.request.settimeout(2)
        while not _arret.is_set():
            try:
                b1, b2 = struct.unpack("!BB", self._lire_exactement(2))
            except socket.timeout:
                # ⚠ C'est ICI que la spec est chiffrée : 12 s sans battement → on efface les
                # abonnements et on ferme. Garder une session muette ouverte ferait croire à une
                # supervision en place alors que plus personne n'écoute.
                if time.monotonic() - sess.sante > SANTE_TIMEOUT_S:
                    log.info("IS-07 : session sans battement depuis %ds — fermée", SANTE_TIMEOUT_S)
                    return
                continue
            opcode, masque, longueur = b1 & 0x0F, bool(b2 & 0x80), b2 & 0x7F
            if longueur == 126:
                longueur = struct.unpack("!H", self._lire_exactement(2))[0]
            elif longueur == 127:
                longueur = struct.unpack("!Q", self._lire_exactement(8))[0]
            if longueur > TAILLE_MAX or opcode == 0x8:
                return
            if not masque:
                return          # RFC 6455 §5.1 : trame client non masquée → on ferme
            k = self._lire_exactement(4)
            charge = bytearray(self._lire_exactement(longueur))
            for i in range(longueur):
                charge[i] ^= k[i & 3]
            if opcode not in (0x1, 0x2):
                continue
            try:
                msg = json.loads(bytes(charge).decode("utf-8"))
            except Exception:
                continue
            self._traiter(sess, msg)

    def _traiter(self, sess, msg):
        cmd = msg.get("command")
        if cmd == "health":
            sess.sante = time.monotonic()
            sess.envoyer({"timing": {"origin_timestamp": msg.get("timestamp") or _ts(),
                                     "creation_timestamp": _ts()},
                          "message_type": "health"})
        elif cmd == "subscription":
            connues = set(_par_id())
            demandees = set(msg.get("sources") or [])
            sess.sources = demandees & connues
            sess.sante = time.monotonic()
            # « Each time a client submits its subscriptions list … the server will resend all the
            # current states » — sans ce renvoi, un abonné resterait aveugle jusqu'au prochain
            # changement, qui peut ne jamais venir.
            for sid in sess.sources:
                e = etat_source(sid)
                if e:
                    sess.envoyer(e)


class _Serveur(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


def _pousser(sids):
    """Envoie l'état courant de ces sources à tous les abonnés concernés."""
    with _sessions_lock:
        sessions = list(_sessions)
    for sid in sids:
        e = etat_source(sid)
        if not e:
            continue
        for s in sessions:
            if sid in s.sources:
                try:
                    s.envoyer(e)
                except Exception:
                    pass


def _veille():
    """Pousse sur CHANGEMENT, en s'accrochant au signal du service TSL — le même que celui qui
    réveille son propre distributeur. Interroger en boucle fabriquerait de la latence là où la
    donnée est déjà événementielle."""
    dernier = {}
    while not _arret.is_set():
        try:
            from services import tsl
            tsl._tally_dirty.wait(timeout=0.5)
            change = []
            for sid, (_shm, idx, niveau, _nom) in _par_id().items():
                v = _valeur(idx, niveau)
                if dernier.get(sid) != v:
                    dernier[sid] = v
                    change.append(sid)
            if change:
                _pousser(change)
        except Exception as e:                                       # pragma: no cover
            log.debug("IS-07 : veille (%s)", e)
            _arret.wait(1)


def demarrer():
    """Ouvre le serveur WebSocket si `nmos_is07_ws` est posé. Idempotent."""
    global _serveur
    if not ws_actif() or _serveur is not None:
        return False
    _arret.clear()
    try:
        _serveur = _Serveur(("0.0.0.0", _port()), _Handler)
    except Exception as e:
        log.warning("IS-07 : serveur WebSocket non démarré (%s)", e)
        _serveur = None
        return False
    threading.Thread(target=_serveur.serve_forever, daemon=True, name="is07-ws").start()
    threading.Thread(target=_veille, daemon=True, name="is07-veille").start()
    log.info("IS-07 : transport WebSocket sur %s", href())
    return True


def arreter():
    global _serveur
    _arret.set()
    if _serveur is not None:
        try:
            _serveur.shutdown()
            _serveur.server_close()
        except Exception:
            pass
        _serveur = None
    with _sessions_lock:
        _sessions.clear()


def etat_ws():
    with _sessions_lock:
        return {"actif": ws_actif(), "href": href() if ws_actif() else None,
                "sessions": len(_sessions),
                "abonnements": sum(len(s.sources) for s in _sessions)}
