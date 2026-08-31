# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Client IS-07 — CONSOMMER le tally d'un émetteur tiers.

═══ Pourquoi un fichier à part ═════════════════════════════════════════════════════════════════

`is07.py` est le sens SORTANT : nos Sources, et un serveur WebSocket auquel des contrôleurs
s'abonnent. Ici c'est l'inverse — nous sommes le client, nous nous connectons chez quelqu'un
d'autre et nous écoutons. Les deux partagent la RFC 6455 et à peu près rien d'autre : les rôles y
sont dissymétriques (c'est le client qui masque ses trames, le serveur jamais), la boucle de vie
est différente (le client reconnecte, le serveur attend), et les pannes n'ont pas le même sens.
Les mêler dans un fichier ferait deux machines d'état enlacées dans un seul jeu de fonctions.

═══ Ce que ce client fait, et ce qu'il ne fait pas ═════════════════════════════════════════════

Il prend une URL, une liste de Sources et un NIVEAU, et il alimente ce niveau. Il ne décide de
rien : ni de l'URL (c'est l'activation IS-05 qui la donne), ni du niveau (c'est un réglage), ni de
la correspondance vers un index de tally (c'est l'appelant). Ce découpage est délibéré : il rend
le client testable en boucle sur NOTRE PROPRE serveur IS-07, sans réseau ni tiers.

═══ Trois choix qui ne se devinent pas ═════════════════════════════════════════════════════════

★ **Perdre la connexion ÉTEINT ce que ce client affirmait.** Un tally qu'on n'entend plus n'est pas
un tally qui persiste : garder le dernier état connu laisserait un rouge allumé sur un plateau
pendant que la liaison est morte, ce qui est exactement le pire des deux. On retire donc toute la
contribution de cette source à la déconnexion — les autres écrivains du niveau ne sont pas touchés
(cf. `services/tsl:poser_tally`).

★ **Le battement de cœur est à NOTRE charge.** IS-07 fait envoyer `health` par le client, toutes
les 5 s ; un serveur conforme ferme la session après 12 s de silence. Un client qui se contente
d'écouter est donc déconnecté au bout de douze secondes, sans erreur visible — il ne reçoit
simplement plus rien.

★ **La reconnexion a des PALIERS.** Une boucle de reconnexion sans palier fuit des descripteurs et
finit par tuer le nœud — c'est arrivé ici, sur un autre chemin (9,7 M de fd). On plafonne, et on ne
tient qu'une socket à la fois.
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

log = logging.getLogger(__name__)

GUID_WS = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
TAILLE_MAX = 1 << 20

PERIODE_SANTE_S = 5.0        # IS-07 : le client bat toutes les 5 s
RECONNEXION_MIN_S = 1.0
RECONNEXION_MAX_S = 30.0     # palier : cf. l'incident des descripteurs
LECTURE_TIMEOUT_S = 2.0

# Valeurs que nous savons interpréter. Un émetteur tiers a SA propre énumération (IS-07 laisse le
# contenu des enums au constructeur) : tout ce qu'on ne reconnaît pas est signalé une fois puis
# traité comme « off », jamais deviné.
VALEURS = ("off", "red", "green", "amber")


def _url(uri):
    """(hôte, port, chemin) d'une URL `ws://` ou `wss://`. `wss` est refusé, faute de TLS ici."""
    from urllib.parse import urlparse
    u = urlparse(uri or "")
    if u.scheme not in ("ws", "http"):
        raise ValueError("schéma non géré : %r (wss/TLS n'est pas implémenté)" % (u.scheme,))
    port = u.port or 80
    chemin = u.path or "/"
    if u.query:
        chemin += "?" + u.query
    if not u.hostname:
        raise ValueError("hôte absent dans %r" % (uri,))
    return u.hostname, port, chemin


class ClientIS07:
    """Une connexion sortante vers un émetteur IS-07. Un fil, une socket.

    `sur_etat(source_id, valeur)` est appelée à chaque état reçu, dans le fil du client. Elle doit
    être brève : elle bloque la lecture de la socket."""

    def __init__(self, uri, sources, sur_etat, nom=None):
        self.uri = uri
        self.sources = list(sources or [])
        self.sur_etat = sur_etat
        self.nom = nom or uri
        self._stop = threading.Event()
        self._fil = None
        self._sock = None
        self._reste = b""
        # Diagnostic : sans ça, « ça ne s'allume pas » n'a aucune réponse observable.
        self.connecte = False
        self.derniere_erreur = ""
        self.connecte_depuis = None
        self.dernier_etat = None       # ts du dernier message d'état reçu
        self.recus = 0
        self.tentatives = 0

    # ── Cycle de vie ─────────────────────────────────────────────────────────
    def demarrer(self):
        if self._fil and self._fil.is_alive():
            return
        self._stop.clear()
        self._fil = threading.Thread(target=self._boucle, daemon=True,
                                     name="is07-client-%s" % (self.nom or "?"))
        self._fil.start()

    def arreter(self):
        self._stop.set()
        s = self._sock
        if s:
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except OSError:
                pass
        if self._fil and self._fil.is_alive():
            self._fil.join(timeout=3)
        self._fil = None

    def etat(self):
        return {"uri": self.uri, "nom": self.nom, "sources": list(self.sources),
                "connecte": self.connecte, "erreur": self.derniere_erreur,
                "recus": self.recus, "tentatives": self.tentatives,
                "connecte_depuis_s": (round(time.monotonic() - self.connecte_depuis, 1)
                                      if self.connecte_depuis else None),
                "dernier_etat_il_y_a_s": (round(time.monotonic() - self.dernier_etat, 1)
                                          if self.dernier_etat else None)}

    def _boucle(self):
        attente = RECONNEXION_MIN_S
        while not self._stop.is_set():
            self.tentatives += 1
            try:
                self._session()
                attente = RECONNEXION_MIN_S      # une session réussie remet le palier à plat
            except Exception as e:
                self.derniere_erreur = str(e)
                log.debug("IS-07 client %s : %s", self.nom, e)
            finally:
                self._fermer()
            if self._stop.wait(timeout=attente):
                break
            attente = min(RECONNEXION_MAX_S, attente * 2)

    def _fermer(self):
        # ★ ON ÉTEINT CE QU'ON AFFIRMAIT. Un tally qu'on n'entend plus n'est pas un tally qui
        # persiste : garder le dernier état laisserait un rouge sur un plateau pendant que la
        # liaison est morte.
        if self.connecte:
            try:
                self.sur_etat(None, None)        # None/None = « je ne sais plus rien »
            except Exception:
                pass
        self.connecte = False
        self.connecte_depuis = None
        s, self._sock = self._sock, None
        self._reste = b""
        if s:
            try:
                s.close()
            except OSError:
                pass

    # ── RFC 6455, côté client ────────────────────────────────────────────────
    def _session(self):
        hote, port, chemin = _url(self.uri)
        self._sock = socket.create_connection((hote, port), timeout=5)
        self._sock.settimeout(5)
        cle = base64.b64encode(os.urandom(16)).decode("ascii")
        self._sock.sendall(
            ("GET %s HTTP/1.1\r\nHost: %s:%d\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
             "Sec-WebSocket-Key: %s\r\nSec-WebSocket-Version: 13\r\n\r\n"
             % (chemin, hote, port, cle)).encode("ascii"))
        entete = self._lire_entete()
        if "101" not in entete.split("\r\n")[0]:
            raise IOError("refus de la poignée de main : %s" % entete.split("\r\n")[0])
        attendu = base64.b64encode(
            hashlib.sha1((cle + GUID_WS).encode("ascii")).digest()).decode("ascii")
        # ⚠ ON VÉRIFIE L'ACCEPT. Sans ça, n'importe quel serveur répondant « 101 » passerait pour
        # un pair WebSocket, et on lirait ses octets comme des trames — du bruit interprété.
        if attendu.lower() not in entete.lower():
            raise IOError("Sec-WebSocket-Accept invalide")

        self.connecte = True
        self.connecte_depuis = time.monotonic()
        self.derniere_erreur = ""
        self._envoyer({"command": "subscription", "sources": list(self.sources)})
        self._sock.settimeout(LECTURE_TIMEOUT_S)
        dernier_battement = time.monotonic()
        while not self._stop.is_set():
            maintenant = time.monotonic()
            if maintenant - dernier_battement >= PERIODE_SANTE_S:
                # ★ SANS CE BATTEMENT, la session est fermée d'en face au bout de 12 s. Un client
                # qui écoute sans battre ne voit aucune erreur : il ne reçoit plus rien.
                self._envoyer({"command": "health", "timestamp": _horodatage()})
                dernier_battement = maintenant
            try:
                msg = self._lire_message()
            except socket.timeout:
                continue
            if msg is None:
                raise IOError("connexion fermée par le pair")
            self._traiter(msg)

    def _envoyer(self, message):
        charge = json.dumps(message).encode("utf-8")
        n = len(charge)
        entete = bytearray([0x81])
        # RFC 6455 §5.1 : une trame CLIENT est toujours masquée — un serveur conforme ferme
        # la connexion sinon, et le nôtre le fait.
        if n < 126:
            entete.append(0x80 | n)
        elif n < (1 << 16):
            entete.append(0x80 | 126)
            entete += struct.pack("!H", n)
        else:
            entete.append(0x80 | 127)
            entete += struct.pack("!Q", n)
        k = os.urandom(4)
        entete += k
        masquee = bytearray(charge)
        for i in range(n):
            masquee[i] ^= k[i & 3]
        self._sock.sendall(bytes(entete) + bytes(masquee))

    def _lire_entete(self):
        tampon = b""
        while b"\r\n\r\n" not in tampon:
            bloc = self._sock.recv(4096)
            if not bloc:
                raise IOError("fermé pendant la poignée de main")
            tampon += bloc
            if len(tampon) > 65536:
                raise IOError("en-tête déraisonnable")
        tete, self._reste = tampon.split(b"\r\n\r\n", 1)
        return tete.decode("latin-1")

    def _lire_exactement(self, n):
        while len(self._reste) < n:
            bloc = self._sock.recv(65536)
            if not bloc:
                raise IOError("fermé")
            self._reste += bloc
        out, self._reste = self._reste[:n], self._reste[n:]
        return out

    def _lire_message(self):
        b1, b2 = struct.unpack("!BB", self._lire_exactement(2))
        opcode, masque, longueur = b1 & 0x0F, bool(b2 & 0x80), b2 & 0x7F
        if longueur == 126:
            longueur = struct.unpack("!H", self._lire_exactement(2))[0]
        elif longueur == 127:
            longueur = struct.unpack("!Q", self._lire_exactement(8))[0]
        if longueur > TAILLE_MAX:
            raise IOError("trame de %d octets : au-delà du raisonnable" % longueur)
        k = self._lire_exactement(4) if masque else b""
        charge = bytearray(self._lire_exactement(longueur))
        if masque:
            for i in range(longueur):
                charge[i] ^= k[i & 3]
        if opcode == 0x8:
            return None                      # close
        if opcode == 0x9:                    # ping → pong, sinon le pair nous coupe
            self._trame_brute(0x8A, bytes(charge))
            return {}
        if opcode not in (0x1, 0x2):
            return {}
        try:
            return json.loads(bytes(charge).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return {}

    def _trame_brute(self, premier_octet, charge):
        n = len(charge)
        entete = bytearray([premier_octet, 0x80 | n])
        k = os.urandom(4)
        entete += k
        m = bytearray(charge)
        for i in range(n):
            m[i] ^= k[i & 3]
        self._sock.sendall(bytes(entete) + bytes(m))

    def _traiter(self, msg):
        if not isinstance(msg, dict) or not msg:
            return
        if msg.get("message_type") == "health":
            return                           # écho de notre battement
        ident = msg.get("identity") or {}
        sid = ident.get("source_id")
        if not sid:
            return
        valeur = ((msg.get("payload") or {}).get("value"))
        if valeur not in VALEURS:
            # On ne DEVINE pas. Un émetteur tiers a sa propre énumération, et lire « PGM » comme
            # un rouge serait inventer une convention qu'il n'a pas déclarée.
            if valeur is not None:
                log.warning("IS-07 client %s : valeur inconnue %r sur %s — ignorée (traitée "
                            "comme « off »)", self.nom, valeur, sid)
            valeur = "off"
        self.recus += 1
        self.dernier_etat = time.monotonic()
        try:
            self.sur_etat(sid, valeur)
        except Exception as e:
            log.warning("IS-07 client %s : traitement de l'état refusé (%s)", self.nom, e)


def _horodatage():
    t = time.time()
    return "%d:%09d" % (int(t), int((t - int(t)) * 1e9))
