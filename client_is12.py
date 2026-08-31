"""Client IS-12 — recevoir les NOTIFICATIONS d'un appareil tiers, et le commander.

Complément de `client_ncp.py` (IS-14, en REST). Ce que IS-12 apporte en plus, et que le REST ne
peut pas donner : **être PRÉVENU** qu'une propriété a changé, sans interroger en boucle.

═══ L'usage qui justifie ce module ═══════════════════════════════════════════════════════════

**Les statuts BCP-008 d'un appareil arrivent par notification IS-12.** Un `NcReceiverMonitor` ne
« répond » pas : il PUBLIE un changement de `connectionStatus`, `streamStatus`, `linkStatus`…
Sans client IS-12, on ne peut connaître la santé d'un flux tiers qu'en interrogeant périodiquement
— c'est-à-dire en la découvrant en retard, et en manquant les transitions brèves, qui sont
justement celles qu'un exploitant veut voir.

C'est aussi ce que l'AMWA met en scène à IBC 2026 : des démonstrations d'interopérabilité
BCP-008 multi-éditeurs. Recevoir le statut d'un autre éditeur, c'est ce module.

═══ Pourquoi écrit à la main ═════════════════════════════════════════════════════════════════

Aucune bibliothèque WebSocket n'est installée, et notre SERVEUR IS-12 a justement été écrit à la
main pour ne pas en ajouter une. Un client est plus simple qu'un serveur : pas de validation de
poignée de main entrante, pas de démasquage. La seule asymétrie de RFC 6455 à ne pas rater est
que **le client DOIT masquer ses trames** (§5.3) — un serveur conforme ferme la connexion sinon.
"""

import base64
import hashlib
import json
import logging
import os
import socket
import struct
import threading
import time
from urllib.parse import urlparse

log = logging.getLogger(__name__)

GUID_WS = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"          # RFC 6455 §1.3
TAILLE_MAX = 1 << 20
CMD, CMD_RESP, NOTIF, SUB, SUB_RESP, ERR = 0, 1, 2, 3, 4, 5

OP_TEXTE, OP_BINAIRE, OP_FERMETURE, OP_PING, OP_PONG = 0x1, 0x2, 0x8, 0x9, 0xA


class ErreurIS12(Exception):
    """Le pair a répondu, mais mal — à distinguer d'une panne réseau."""


class Client:
    """Session IS-12 vers un appareil tiers.

    Usage :
        with Client("ws://192.0.2.9:5010/x-nmos/ncp/v1.0") as c:
            c.abonner([1, 42])
            for n in c.notifications(duree_s=30):
                ...

    ⚠ NON THREAD-SAFE en écriture : une instance = un fil d'exécution. Deux threads qui
    commandent sur la même socket entrelaceraient leurs trames, et le pair fermerait la session
    pour trame malformée — panne difficile à lire.
    """

    def __init__(self, url, timeout=8):
        self.url = url
        self.timeout = timeout
        self.sock = None
        self._reste = b""
        self._handle = 0
        self._lock = threading.Lock()

    # ── Connexion ─────────────────────────────────────────────────────────────────────────
    def __enter__(self):
        self.connecter()
        return self

    def __exit__(self, *a):
        self.fermer()

    def connecter(self):
        u = urlparse(self.url)
        if u.scheme not in ("ws", "wss"):
            raise ErreurIS12("schéma non géré : %r (attendu ws:// ou wss://)" % u.scheme)
        port = u.port or (443 if u.scheme == "wss" else 80)
        chemin = u.path or "/"
        try:
            s = socket.create_connection((u.hostname, port), timeout=self.timeout)
        except Exception as e:
            raise ErreurIS12("connexion impossible : %s" % e)
        if u.scheme == "wss":
            import ssl
            # Le pair est un équipement de production, pas un site web : son certificat est
            # rarement signé par une AC publique. On chiffre, on ne prétend pas authentifier —
            # et on le DIT, plutôt que de laisser croire à une vérification qui n'a pas lieu.
            ctx = ssl._create_unverified_context()
            log.info("client IS-12 : TLS SANS vérification du certificat du pair (%s)", u.hostname)
            s = ctx.wrap_socket(s, server_hostname=u.hostname)
        cle = base64.b64encode(os.urandom(16)).decode("ascii")
        requete = (
            "GET %s HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n"
            "Sec-WebSocket-Protocol: ncp\r\n\r\n" % (chemin, u.hostname, port, cle)
        )
        s.sendall(requete.encode("ascii"))
        entete = self._lire_entete(s)
        if " 101 " not in entete.split("\r\n")[0]:
            s.close()
            raise ErreurIS12("le pair a refusé la bascule WebSocket : %s"
                             % entete.split("\r\n")[0])
        attendu = base64.b64encode(
            hashlib.sha1((cle + GUID_WS).encode("ascii")).digest()).decode("ascii")
        if attendu.lower() not in entete.lower():
            s.close()
            # Ce contrôle n'est pas décoratif : sans lui, un serveur qui ne parle pas WebSocket
            # mais répond 101 nous ferait interpréter du HTTP comme des trames.
            raise ErreurIS12("Sec-WebSocket-Accept absent ou faux — le pair ne parle pas RFC 6455")
        self.sock = s
        return self

    def _lire_entete(self, s):
        tampon = b""
        s.settimeout(self.timeout)
        while b"\r\n\r\n" not in tampon:
            bloc = s.recv(4096)
            if not bloc:
                raise ErreurIS12("le pair a fermé pendant la poignée de main")
            tampon += bloc
            if len(tampon) > 65536:
                raise ErreurIS12("en-tête de poignée de main déraisonnable")
        entete, _, self._reste = tampon.partition(b"\r\n\r\n")
        return entete.decode("latin-1")

    def fermer(self):
        if self.sock:
            try:
                self._envoyer_trame(b"", OP_FERMETURE)
            except Exception:
                pass
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    # ── Cadrage RFC 6455 ──────────────────────────────────────────────────────────────────
    def _envoyer_trame(self, charge, opcode=OP_TEXTE):
        n = len(charge)
        entete = bytearray([0x80 | opcode])
        # ⚠ Bit de masque OBLIGATOIRE côté client (RFC 6455 §5.3). Un serveur conforme ferme la
        # connexion sur une trame client non masquée — le nôtre le fait explicitement.
        if n < 126:
            entete.append(0x80 | n)
        elif n < (1 << 16):
            entete.append(0x80 | 126); entete += struct.pack("!H", n)
        else:
            entete.append(0x80 | 127); entete += struct.pack("!Q", n)
        cle = os.urandom(4)
        entete += cle
        masquee = bytearray(charge)
        for i in range(n):
            masquee[i] ^= cle[i & 3]
        self.sock.sendall(bytes(entete) + bytes(masquee))

    def _lire_exactement(self, n):
        while len(self._reste) < n:
            bloc = self.sock.recv(65536)
            if not bloc:
                raise ErreurIS12("connexion fermée par le pair")
            self._reste += bloc
        out, self._reste = self._reste[:n], self._reste[n:]
        return out

    def _lire_trame(self):
        b1, b2 = struct.unpack("!BB", self._lire_exactement(2))
        fin, opcode = bool(b1 & 0x80), b1 & 0x0F
        masque, longueur = bool(b2 & 0x80), b2 & 0x7F
        if longueur == 126:
            longueur = struct.unpack("!H", self._lire_exactement(2))[0]
        elif longueur == 127:
            longueur = struct.unpack("!Q", self._lire_exactement(8))[0]
        if longueur > TAILLE_MAX:
            raise ErreurIS12("message trop volumineux (%d o)" % longueur)
        cle = self._lire_exactement(4) if masque else None
        charge = bytearray(self._lire_exactement(longueur))
        if cle:
            for i in range(longueur):
                charge[i] ^= cle[i & 3]
        return fin, opcode, bytes(charge)

    def _lire_message(self, echeance):
        """Message JSON complet, ou None à l'échéance. Réassemble les trames fragmentées et
        répond aux pings — un pair qui ping et n'obtient rien finit par nous fermer la porte."""
        morceaux = []
        while True:
            restant = echeance - time.monotonic()
            if restant <= 0:
                return None
            self.sock.settimeout(max(0.2, min(restant, 5)))
            try:
                fin, opcode, charge = self._lire_trame()
            except socket.timeout:
                continue
            if opcode == OP_FERMETURE:
                raise ErreurIS12("le pair a fermé la session")
            if opcode == OP_PING:
                self._envoyer_trame(charge, OP_PONG)
                continue
            if opcode == OP_PONG:
                continue
            morceaux.append(charge)
            if fin:
                brut = b"".join(morceaux)
                try:
                    return json.loads(brut.decode("utf-8"))
                except Exception as e:
                    raise ErreurIS12("message illisible : %s" % e)

    # ── Protocole IS-12 ───────────────────────────────────────────────────────────────────
    def _envoyer(self, message):
        with self._lock:
            self._envoyer_trame(json.dumps(message).encode("utf-8"))

    def commander(self, oid, method_id, arguments=None, timeout=None):
        """Envoie une Command et rend le NcMethodResult. `method_id` = (level, index).

        ★ Le résultat porte son verdict DANS le corps : `status` 200 = OK. Une réponse reçue
        n'est pas une commande réussie — confondre les deux ferait passer un refus pour un
        succès, exactement comme en IS-14."""
        self._handle += 1
        h = self._handle
        self._envoyer({"messageType": CMD, "commands": [{
            "handle": h, "oid": oid,
            "methodId": {"level": method_id[0], "index": method_id[1]},
            "arguments": arguments or {}}]})
        echeance = time.monotonic() + (timeout or self.timeout)
        while True:
            msg = self._lire_message(echeance)
            if msg is None:
                raise ErreurIS12("pas de réponse à la commande (handle %d)" % h)
            if msg.get("messageType") == ERR:
                raise ErreurIS12("erreur de protocole : %s" % msg.get("errorMessage"))
            if msg.get("messageType") != CMD_RESP:
                continue          # une notification peut arriver AVANT notre réponse
            for r in msg.get("responses") or []:
                if r.get("handle") == h:
                    res = r.get("result") or {}
                    if res.get("status") not in (None, 200):
                        raise ErreurIS12("commande refusée : status=%s %s"
                                         % (res.get("status"), res.get("errorMessage") or ""))
                    return res

    def abonner(self, oids, timeout=None):
        """S'abonne aux notifications de ces oids. Rend la liste RETENUE par le pair — qui peut
        être plus courte que la demande : c'est lui qui décide, et le vérifier évite d'attendre
        indéfiniment des notifications qui ne viendront jamais."""
        self._envoyer({"messageType": SUB, "subscriptions": list(oids)})
        echeance = time.monotonic() + (timeout or self.timeout)
        while True:
            msg = self._lire_message(echeance)
            if msg is None:
                raise ErreurIS12("pas de réponse à l'abonnement")
            if msg.get("messageType") == ERR:
                raise ErreurIS12("abonnement refusé : %s" % msg.get("errorMessage"))
            if msg.get("messageType") == SUB_RESP:
                return msg.get("subscriptions") or []

    def notifications(self, duree_s=30):
        """Générateur des notifications reçues pendant `duree_s`. Chaque élément est un
        NcPropertyChangedEventData enrichi de son `oid`."""
        echeance = time.monotonic() + duree_s
        while time.monotonic() < echeance:
            try:
                msg = self._lire_message(echeance)
            except ErreurIS12:
                return
            if msg is None:
                return
            if msg.get("messageType") != NOTIF:
                continue
            for n in msg.get("notifications") or []:
                d = dict(n.get("eventData") or {})
                d["oid"] = n.get("oid")
                yield d
