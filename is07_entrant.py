# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Le tally qui ENTRE : de l'activation IS-05 d'un Receiver jusqu'à un niveau alimenté.

═══ Ce que ce module tient, et pourquoi il est séparé ══════════════════════════════════════════

`is07_client.py` sait parler à un émetteur ; il ne sait pas QUI écouter, ni POUR QUI. C'est ici
qu'on garde ce lien, parce qu'il vient de trois endroits qui n'ont rien à voir :

  · l'URL et la Source à écouter → d'un **contrôleur externe**, par un PATCH IS-05 ;
  · le NIVEAU alimenté          → d'un **réglage de site** (Réglages → NMOS), qui ne peut pas
    venir d'un contrôleur : c'est notre plan de tally, pas le sien ;
  · l'INDEX du signal           → de notre **correspondance TSL**, la même que partout ailleurs.

★ POURQUOI LE NIVEAU N'EST PAS DANS LE PATCH. IS-05 décrit une connexion, pas une intention
d'exploitation. Laisser un contrôleur choisir le niveau reviendrait à le laisser décider sur
QUELLE chaîne de destination il allume nos lampes — donc à lui donner la main sur une autre
production que la sienne. Il dit d'où vient le signal ; nous disons ce qu'il alimente.

★ CHAQUE RECEIVER EST SA PROPRE SOURCE D'ÉCRITURE (`is07:<rid>`). Deux contrôleurs peuvent donc
alimenter le même niveau : ils se CUMULENT au lieu de s'écraser (cf. `services/tsl:poser_tally`),
ce qui est le cas voulu — un tally externe qui complète le nôtre, pas qui le remplace.

★ UNE ACTIVATION QUI RETOMBE ÉTEINT SA CONTRIBUTION. `master_enable` à faux, une URL retirée, une
liaison perdue : dans les trois cas ce qui était affirmé disparaît. Un tally qu'on n'entend plus
n'est pas un tally qui persiste — le garder allumerait un rouge sur un plateau que plus personne
ne surveille.
"""
import logging
import threading

from app import tally as _tally

log = logging.getLogger(__name__)

# rid → ClientIS07 vivant. Un seul par connexion : réactiver remplace, jamais n'empile.
_clients = {}
_lock = threading.RLock()


def _conn_de(rid):
    """La connexion IS-07 entrante que sert ce Receiver, ou None."""
    from app.database import db_get_is07_connections
    from .is07 import _rid_conn
    for c in db_get_is07_connections():
        if _rid_conn(c["id"]) == rid:
            return c
    return None


def _eteindre(rid):
    """Retire tout ce que ce Receiver affirmait, sans toucher aux autres écrivains."""
    try:
        from app import tally
        tally.poser_tally("is07:%s" % rid, {})
    except Exception as e:
        log.debug("IS-07 entrant : extinction de %s impossible (%s)", rid, e)



def activer(rid, actif, connection_uri=None, source_ids=None):
    """Appelée par l'activation IS-05. Démarre ou arrête l'écoute de cette connexion.

    ★ LES SOURCES ÉCOUTÉES VIENNENT DE LA CORRESPONDANCE, pas du PATCH. C'est le pendant exact de
    TSL : une connexion écoute tout ce que la table lui associe, et le contrôleur ne dit que
    l'ADRESSE de l'émetteur. `source_ids` reste accepté (un contrôleur peut restreindre), mais la
    correspondance fait foi pour savoir ce que chaque Source désigne chez nous.

    Renvoie un court diagnostic, journalisé : une activation qui ne prend pas doit DIRE pourquoi.
    Sans ça, un contrôleur voit un 200, l'exploitant voit une lampe éteinte, et rien ne relie les
    deux."""
    from .is07_client import ClientIS07
    from app.database import db_get_is07_mapping
    with _lock:
        ancien = _clients.pop(rid, None)
    if ancien:
        ancien.arreter()
    # ⚠ DEUX CHEMINS D'EXTINCTION, ET C'EST VOULU. Le client annonce déjà « je ne sais plus rien »
    # en se fermant (`_fermer` → `sur_etat(None, None)`), donc celui-ci fait souvent double
    # emploi. Il reste parce qu'il couvre le cas que l'autre ne couvre pas : désactiver une
    # connexion dont AUCUN client ne tourne. Sans lui, cette lampe-là ne s'éteint jamais.
    _eteindre(rid)

    if not actif:
        return "arrêté"
    c = _conn_de(rid)
    if not c:
        return "connexion inconnue"
    if not connection_uri:
        # On REFUSE de deviner l'URL. Elle vient du contrôleur, et un repli inventé
        # connecterait à un émetteur que personne n'a désigné.
        log.warning("IS-07 entrant : « %s » activée SANS connection_uri — rien à écouter",
                    c.get("name"))
        return "sans connection_uri"
    niveau = c.get("level_uuid")
    if not niveau:
        log.warning("IS-07 entrant : « %s » activée mais AUCUN NIVEAU affecté — le tally reçu "
                    "n'irait nulle part (Réglages → NMOS)", c.get("name"))
        return "sans niveau affecté"

    # Source de l'émetteur → NOTRE FLUX. Résolu une fois à l'activation.
    #
    # ★ Il n'y a plus d'index ici. IS-07 désigne une Source, nous désignons un flux, et le
    # modèle de tally s'adresse par flux : la chaîne est directe. L'ancienne version traduisait
    # en index TSL et REFUSAIT toute source qui n'en avait pas — un signal parfaitement désigné
    # de bout en bout était jeté parce qu'un protocole tiers, absent du chemin, ne le connaissait
    # pas. Le tally d'un flux n'a pas à dépendre d'un protocole qu'on n'emploie pas.
    table = {}
    for m in db_get_is07_mapping(c["id"]):
        shm = (m.get("source_shm") or "").strip()
        if not shm:
            continue
        table[str(m["source_id"])] = _tally.resolve_ref(shm) or shm
    if not table:
        log.warning("IS-07 entrant : « %s » activée mais sa correspondance est vide — aucune "
                    "Source de l'émetteur ne désigne un de nos signaux (page Labels)",
                    c.get("name"))
        return "correspondance vide"

    # ★ CETTE CONNEXION TIENT SON PROPRE ÉTAT. `poser_tally` remplace la contribution ENTIÈRE
    # d'une source : poser la seule case qui vient de changer éteindrait toutes les autres à
    # chaque message reçu. On garde donc ici ce que cette connexion affirme, et on le repose en
    # entier — c'est un dictionnaire de quelques entrées, relu par un seul fil.
    affirme = {}

    def _sur_etat(source_id, valeur):
        # `(None, None)` = le client a perdu la liaison et ne sait plus rien.
        if source_id is None:
            affirme.clear()
            _eteindre(rid)
            return
        cible = table.get(str(source_id))
        if not cible:
            return                       # une Source à laquelle on ne s'est pas abonné
        cle = (cible, niveau)
        if valeur in (None, "off"):
            affirme.pop(cle, None)
        else:
            affirme[cle] = valeur
        from app import tally
        tally.poser_tally("is07:%s" % rid, dict(affirme))

    ecoutees = [s for s in (source_ids or []) if s in table] or list(table)
    cl = ClientIS07(connection_uri, ecoutees, _sur_etat, nom=c.get("name") or rid[:8])
    with _lock:
        _clients[rid] = cl
    cl.demarrer()
    log.info("IS-07 entrant : « %s » écoute %s — %d Source(s) → niveau %s",
             c.get("name"), connection_uri, len(ecoutees), niveau)
    return "en écoute"


def etat():
    """Ce que fait chaque connexion entrante — pour l'interface et le diagnostic."""
    from app.database import db_get_is07_connections
    from .is07 import _rid_conn
    with _lock:
        clients = dict(_clients)
    out = {}
    for c in db_get_is07_connections():
        rid = _rid_conn(c["id"])
        cl = clients.get(rid)
        out[rid] = dict(cl.etat() if cl else {"connecte": False},
                        id=c["id"], nom=c.get("name"), niveau=c.get("level_uuid"),
                        active=bool(c.get("enabled")))
    return out


def arreter_tout():
    with _lock:
        clients = dict(_clients)
        _clients.clear()
    for rid, c in clients.items():
        c.arreter()
        _eteindre(rid)
