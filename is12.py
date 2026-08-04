# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""AMWA IS-12 « NMOS Control Protocol » — transport WebSocket du modèle MS-05-02.

Quatre pièces, séparées à dessein : `ncp.py` porte le framework, `monitors.py` la supervision
BCP-008, `modele.py` le modèle d'appareil et son cycle de vie (partagé avec IS-14), et ce
fichier le TRANSPORT — poignée de main RFC 6455, découpage en trames, et la
boucle de messages IS-12 (Command / CommandResponse / Notification / Subscription /
SubscriptionResponse / Error).

POURQUOI UN SERVEUR À PART, SUR SON PROPRE PORT
L'orchestrateur est servi par Waitress, qui ne sait pas passer une connexion HTTP en WebSocket
(pas de `wsgi.websocket`, et `flask-sock` réclame Werkzeug/gunicorn/gevent). Changer de serveur
WSGI pour une fonctionnalité de supervision serait un très mauvais échange. IS-12 n'a de toute
façon rien à faire sur le port de l'UI : la spec l'annonce par une URL `ws://` dans le tableau
`controls` du Device IS-04, qui peut désigner n'importe quel port. Même motif que
`app/pxe_server.py`, déjà servi à côté pour une raison analogue.

Nous implémentons donc RFC 6455 côté serveur, sans dépendance ajoutée : la poignée de main est
un SHA-1 et un base64, le découpage en trames tient en une centaine de lignes, et l'usage est
étroit (trames texte, un contrôleur ou deux). Ajouter `websockets`/`simple-websocket` à un
`requirements.txt` de dix-sept lignes, dans un produit qui s'installe hors ligne, coûtait plus.

SÉCURITÉ — à lire avant d'exposer le port
Ce point d'accès n'est PAS authentifié, comme le reste du provider NMOS de ce projet (les
endpoints IS-04/IS-05 ne le sont pas davantage) : il vit sur le réseau de contrôle interne.
Il est en LECTURE SEULE au sens opérationnel — le modèle publié n'expose aucun objet capable de
router, de démarrer ou d'arrêter quoi que ce soit. Les seules écritures acceptées sont les deux
réglages que la BCP-008 impose de rendre modifiables (`statusReportingDelay`,
`autoResetCountersAndMessages`) et la remise à zéro des compteurs. BCP-003-02 (TLS + jeton
d'autorisation) reste à faire le jour où ce port sortira du réseau de contrôle.
"""
import base64
import hashlib
import json
import logging
import queue
import socket
import socketserver
import struct
import threading
import time

from . import modele, ncp

log = logging.getLogger(__name__)

PORT_DEFAUT   = 5010
CHEMIN        = "/x-nmos/ncp/v1.0"     # chemin annoncé ; le serveur accepte n'importe lequel
TYPE_CONTROL  = "urn:x-nmos:control:ncp/v1.0"
GUID_WS       = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"   # RFC 6455 §1.3
TAILLE_MAX    = 1 << 20                # 1 Mio par message — au-delà, la session est fermée
SESSIONS_MAX  = 16                     # « Controllers SHOULD NOT open an excessive number… »

# Types de message IS-12
CMD, CMD_RESP, NOTIF, SUB, SUB_RESP, ERR = 0, 1, 2, 3, 4, 5

_lock = threading.RLock()
_serveur = None
_thread_serveur = None
_thread_sampler = None
_arret = threading.Event()
_sessions = set()
_etat = {"port": None, "demarre_unix": None}


# ═════════════════════════════════════════════════════════════════════════════════════════════
# Transport WebSocket (RFC 6455, côté serveur)
# ═════════════════════════════════════════════════════════════════════════════════════════════

class _FermetureWS(Exception):
    """Fin de session demandée par le pair ou imposée par un protocole invalide."""

    def __init__(self, code=1000, raison=""):
        super().__init__(raison or str(code))
        self.code = code
        self.raison = raison


class Session:
    """Une connexion de contrôleur, avec sa file de sortie et son thread d'écriture.

    ★ POURQUOI UNE FILE plutôt qu'un simple verrou d'écriture : un contrôleur peut rester
    connecté sans jamais lire (process figé, machine suspendue). Le tampon TCP se remplit, et un
    `sendall` direct bloque — depuis le thread d'échantillonnage, qui est PARTAGÉ par tous les
    monitors. Un seul client mal en point gèlerait alors la supervision de tout le parc, en
    silence et sans que rien ne paraisse anormal. Ici, l'émission est découplée : quand la file
    d'un client déborde, c'est CE client qu'on ferme, et les autres n'en savent rien."""

    FILE_MAX = 512

    def __init__(self, sock, adresse):
        self.sock = sock
        self.adresse = adresse
        self.fermee = False
        self._file = queue.Queue(maxsize=self.FILE_MAX)
        self._ecrivain = threading.Thread(target=self._boucle_ecriture,
                                          name="is12-tx", daemon=True)
        self._ecrivain.start()

    def envoyer(self, message):
        self.envoyer_trame(json.dumps(message, separators=(",", ":")).encode("utf-8"), 0x1)

    def envoyer_trame(self, charge, opcode):
        if self.fermee:
            return
        n = len(charge)
        if n < 126:
            entete = struct.pack("!BB", 0x80 | opcode, n)
        elif n < (1 << 16):
            entete = struct.pack("!BBH", 0x80 | opcode, 126, n)
        else:
            entete = struct.pack("!BBQ", 0x80 | opcode, 127, n)
        try:
            self._file.put_nowait(entete + charge)
        except queue.Full:
            log.warning("IS-12 : contrôleur %s ne consomme plus ses notifications — session "
                        "fermée (%d messages en attente)", self.adresse[0], self.FILE_MAX)
            self.fermee = True
            try:
                self.sock.shutdown(socket.SHUT_RDWR)      # débloque la boucle de lecture
            except OSError:
                pass

    def _boucle_ecriture(self):
        while True:
            trame = self._file.get()
            if trame is None:
                return
            try:
                self.sock.sendall(trame)
            except OSError:
                self.fermee = True
                return

    def fermer(self, code=1000, raison=""):
        if not self.fermee:
            self.envoyer_trame(struct.pack("!H", code) + raison.encode("utf-8")[:123], 0x8)
        self.fermee = True
        try:
            self._file.put_nowait(None)
        except queue.Full:
            pass


def _cle_acceptation(cle):
    return base64.b64encode(hashlib.sha1((cle + GUID_WS).encode("ascii")).digest()).decode("ascii")


class _Handler(socketserver.BaseRequestHandler):

    def setup(self):
        self.request.settimeout(None)
        self.fichier = self.request.makefile("rb")
        self.session = None

    def handle(self):
        try:
            entetes = self._lire_requete()
        except Exception:
            return
        if entetes is None:
            return
        cle = entetes.get("sec-websocket-key")
        if (entetes.get("upgrade", "").lower() != "websocket"
                or "upgrade" not in entetes.get("connection", "").lower()
                or not cle or entetes.get("sec-websocket-version") != "13"):
            self._refuser(400, "IS-12 attend une connexion WebSocket (RFC 6455, version 13).")
            return
        with _lock:
            trop = len(_sessions) >= SESSIONS_MAX
        if trop:
            self._refuser(503, "Trop de sessions IS-12 ouvertes sur cet appareil.")
            log.warning("IS-12 : session refusée (%d déjà ouvertes) depuis %s",
                        SESSIONS_MAX, self.client_address[0])
            return
        self.request.sendall(
            b"HTTP/1.1 101 Switching Protocols\r\n"
            b"Upgrade: websocket\r\nConnection: Upgrade\r\n"
            b"Sec-WebSocket-Accept: " + _cle_acceptation(cle).encode("ascii") + b"\r\n\r\n")
        self.session = Session(self.request, self.client_address)
        with _lock:
            _sessions.add(self.session)
        log.info("IS-12 : session ouverte depuis %s", self.client_address[0])
        try:
            self._boucle()
        except _FermetureWS as f:
            self.session.fermer(f.code, f.raison)
        except (OSError, ConnectionError):
            pass
        except Exception:
            log.exception("IS-12 : session %s interrompue", self.client_address[0])
        finally:
            self._quitter()

    def _quitter(self):
        if self.session is None:
            return
        with _lock:
            _sessions.discard(self.session)
        app = modele.appareil()
        if app is not None:
            app.desabonner(self.session)
        self.session.fermee = True
        log.info("IS-12 : session fermée (%s)", self.client_address[0])

    _RAISONS = {400: "Bad Request", 503: "Service Unavailable"}

    def _refuser(self, code, texte):
        corps = texte.encode("utf-8")
        self.request.sendall(
            ("HTTP/1.1 %d %s\r\nContent-Type: text/plain; charset=utf-8\r\n"
             "Content-Length: %d\r\nConnection: close\r\n\r\n"
             % (code, self._RAISONS.get(code, "Error"), len(corps))).encode("ascii") + corps)

    def _lire_requete(self):
        """En-têtes HTTP en minuscules, ou None si la requête est illisible."""
        ligne = self.fichier.readline(8192)
        if not ligne:
            return None
        entetes = {}
        for _ in range(64):
            l = self.fichier.readline(8192)
            if not l or l in (b"\r\n", b"\n"):
                break
            if b":" in l:
                k, v = l.split(b":", 1)
                entetes[k.strip().decode("latin-1").lower()] = v.strip().decode("latin-1")
        return entetes

    # ─── Découpage en trames ─────────────────────────────────────────────────────────────────
    def _lire_exactement(self, n):
        data = self.fichier.read(n)
        if data is None or len(data) < n:
            raise _FermetureWS(1001, "connexion interrompue")
        return data

    def _lire_trame(self):
        b1, b2 = struct.unpack("!BB", self._lire_exactement(2))
        fin, opcode = bool(b1 & 0x80), b1 & 0x0F
        masque, longueur = bool(b2 & 0x80), b2 & 0x7F
        if longueur == 126:
            longueur = struct.unpack("!H", self._lire_exactement(2))[0]
        elif longueur == 127:
            longueur = struct.unpack("!Q", self._lire_exactement(8))[0]
        if not masque:
            # RFC 6455 §5.1 : une trame client non masquée DOIT faire échouer la connexion.
            raise _FermetureWS(1002, "trame client non masquée")
        if longueur > TAILLE_MAX:
            raise _FermetureWS(1009, "message trop volumineux")
        cle = self._lire_exactement(4)
        charge = bytearray(self._lire_exactement(longueur))
        for i in range(longueur):
            charge[i] ^= cle[i & 3]
        return fin, opcode, bytes(charge)

    def _boucle(self):
        morceaux, opcode_msg = [], None
        taille = 0
        while not _arret.is_set():
            fin, opcode, charge = self._lire_trame()
            if opcode == 0x8:
                raise _FermetureWS(1000, "")
            if opcode == 0x9:                       # ping → pong avec la même charge
                self.session.envoyer_trame(charge, 0xA)
                continue
            if opcode == 0xA:                       # pong non sollicité : rien à faire
                continue
            if opcode == 0x0:
                if opcode_msg is None:
                    raise _FermetureWS(1002, "trame de continuation sans début de message")
            elif opcode in (0x1, 0x2):
                if opcode_msg is not None:
                    raise _FermetureWS(1002, "nouveau message avant la fin du précédent")
                opcode_msg = opcode
            else:
                raise _FermetureWS(1003, "opcode %d non pris en charge" % opcode)
            morceaux.append(charge)
            taille += len(charge)
            if taille > TAILLE_MAX:
                raise _FermetureWS(1009, "message trop volumineux")
            if not fin:
                continue
            complet, morceaux, opcode_courant = b"".join(morceaux), [], opcode_msg
            opcode_msg, taille = None, 0
            if opcode_courant != 0x1:
                # IS-12 §Transport : les messages sont du JSON encodé en UTF-8, donc des trames
                # TEXTE. Une trame binaire n'est pas un message IS-12 mal formé, c'est autre chose.
                raise _FermetureWS(1003, "IS-12 n'accepte que des trames texte")
            self._traiter(complet)

    # ─── Boucle de messages IS-12 ────────────────────────────────────────────────────────────
    def _traiter(self, brut):
        try:
            msg = json.loads(brut.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            self.session.envoyer(_message_erreur(ncp.BAD_COMMAND_FORMAT, "JSON invalide : %s" % e))
            return
        if not isinstance(msg, dict) or not isinstance(msg.get("messageType"), int):
            self.session.envoyer(_message_erreur(ncp.BAD_COMMAND_FORMAT, "messageType absent"))
            return
        t = msg["messageType"]
        if t == CMD:
            self._commandes(msg)
        elif t == SUB:
            self._abonnement(msg)
        else:
            # Les autres types vont de l'appareil VERS le contrôleur : les recevoir est une erreur.
            self.session.envoyer(_message_erreur(
                ncp.INVALID_REQUEST, "messageType %d n'est pas émis par un contrôleur" % t))

    def _commandes(self, msg):
        commandes = msg.get("commands")
        if not isinstance(commandes, list) or not commandes:
            self.session.envoyer(_message_erreur(ncp.BAD_COMMAND_FORMAT, "commands absent ou vide"))
            return
        reponses = []
        for c in commandes:
            if not isinstance(c, dict) or not isinstance(c.get("handle"), int):
                # Sans handle, aucune réponse ne peut être appariée : c'est le cas que le type de
                # message Error est fait pour couvrir.
                self.session.envoyer(_message_erreur(ncp.BAD_COMMAND_FORMAT,
                                                     "commande sans handle exploitable"))
                continue
            reponses.append({"handle": c["handle"], "result": _executer(c)})
        if reponses:
            self.session.envoyer({"messageType": CMD_RESP, "responses": reponses})

    def _abonnement(self, msg):
        oids = msg.get("subscriptions")
        if not isinstance(oids, list):
            self.session.envoyer(_message_erreur(ncp.BAD_COMMAND_FORMAT, "subscriptions absent"))
            return
        app = modele.appareil()
        retenus = app.abonner(self.session, oids) if app else []
        self.session.envoyer({"messageType": SUB_RESP, "subscriptions": retenus})


def _message_erreur(status, message):
    return {"messageType": ERR, "status": status, "errorMessage": message}


def _executer(commande):
    app = modele.appareil()
    if app is None:
        return ncp.erreur(ncp.NOT_READY, "modèle d'appareil non initialisé")
    oid = commande.get("oid")
    mid = commande.get("methodId")
    if not isinstance(oid, int) or not isinstance(mid, dict) \
            or not isinstance(mid.get("level"), int) or not isinstance(mid.get("index"), int):
        return ncp.erreur(ncp.BAD_COMMAND_FORMAT, "oid ou methodId absent ou mal formé")
    args = commande.get("arguments")
    if args is not None and not isinstance(args, dict):
        return ncp.erreur(ncp.PARAMETER_ERROR, "arguments doit être un objet")
    return app.commander(oid, (mid["level"], mid["index"]), args or {})


class _Serveur(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True

    def handle_error(self, request, client_address):
        log.debug("IS-12 : erreur de session %s", client_address, exc_info=True)


# ═════════════════════════════════════════════════════════════════════════════════════════════
# Cycle de vie
# ═════════════════════════════════════════════════════════════════════════════════════════════

def href(host=None):
    """URL annoncée dans le tableau `controls` du Device IS-04."""
    from . import _get_host_address
    from app.database import db_get_setting
    port = int(db_get_setting("nmos_is12_port", PORT_DEFAUT) or PORT_DEFAUT)
    return "ws://{}:{}{}".format(host or _get_host_address(), port, CHEMIN)


def actif():
    return _serveur is not None


def start():
    """Démarre le serveur IS-12. Idempotent. Le MODÈLE, lui, appartient à `modele.py` — IS-12
    l'acquiert, IS-14 peut l'acquérir aussi, et il n'en existe qu'un."""
    global _serveur, _thread_serveur
    from app.database import db_get_setting
    with _lock:
        if _serveur is not None:
            return True
        port = int(db_get_setting("nmos_is12_port", PORT_DEFAUT) or PORT_DEFAUT)
        modele.acquerir("is12")
        try:
            _serveur = _Serveur(("0.0.0.0", port), _Handler)
        except OSError as e:
            modele.liberer("is12")
            log.error("IS-12 : impossible d'écouter sur le port %d (%s) — service non démarré",
                      port, e)
            return False
        _arret.clear()
        _thread_serveur = threading.Thread(target=_serveur.serve_forever, name="is12-ws",
                                           daemon=True)
        _thread_serveur.start()
        _etat.update({"port": port, "demarre_unix": int(time.time())})
    log.info("IS-12 : à l'écoute sur %s", href())
    return True


def stop():
    global _serveur, _thread_serveur
    with _lock:
        srv, _serveur = _serveur, None
        sessions = list(_sessions)
        deja_arrete = srv is None
    _arret.set()
    for s in sessions:
        try:
            s.fermer(1001, "arrêt du service")
            s.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
    if srv is not None:
        srv.shutdown()
        srv.server_close()
    if _thread_serveur is not None:
        _thread_serveur.join(timeout=3)
    with _lock:
        _sessions.clear()
        _thread_serveur = None
        _etat.update({"port": None, "demarre_unix": None})
    if not deja_arrete:
        modele.liberer("is12")


def status_dict():
    rx, tx, ignores = modele.nb_monitors()
    with _lock:
        return {
            "actif": _serveur is not None,
            "port": _etat.get("port"),
            "href": href() if _serveur is not None else None,
            "sessions": len(_sessions),
            "monitors_rx": rx,
            "monitors_tx": tx,
            "sans_monitor": ignores,
            "depuis_unix": _etat.get("demarre_unix"),
        }


# Conservé comme façade : l'UI et les routes existantes appellent `is12.etat_monitors()`.
etat_monitors = modele.etat_monitors
sync_model = modele.sync_model
