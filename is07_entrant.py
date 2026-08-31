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

log = logging.getLogger(__name__)

# rid → ClientIS07 vivant. Un seul par Receiver : réactiver remplace, jamais n'empile.
_clients = {}
_lock = threading.RLock()

# Réglage : `is07_niveaux` = {receiver_id: uuid de niveau}
CLE_REGLAGE = "is07_niveaux"


def niveaux():
    """{receiver_id: uuid de niveau} — l'affectation, telle qu'un exploitant l'a posée."""
    try:
        from app.database import db_get_setting
        v = db_get_setting(CLE_REGLAGE)
        return dict(v) if isinstance(v, dict) else {}
    except Exception:
        return {}


def affecter(rid, level_uuid):
    """Affecte (ou retire, avec None) le niveau qu'alimente ce Receiver.

    Changer le niveau d'un Receiver ACTIF déplace sa contribution : on éteint l'ancienne avant de
    poser la nouvelle, sinon la lampe précédente resterait allumée sans plus personne pour la
    mettre à jour."""
    from app.database import db_get_setting, db_set_setting
    with _lock:
        m = db_get_setting(CLE_REGLAGE)
        m = dict(m) if isinstance(m, dict) else {}
        if level_uuid:
            m[str(rid)] = str(level_uuid)
        else:
            m.pop(str(rid), None)
        db_set_setting(CLE_REGLAGE, m)
        c = _clients.get(rid)
    _eteindre(rid)
    if c:
        # Le client tourne toujours ; sa prochaine réception écrira sur le nouveau niveau.
        log.info("IS-07 entrant : Receiver %s réaffecté au niveau %s", rid, level_uuid)


def _eteindre(rid):
    """Retire tout ce que ce Receiver affirmait, sans toucher aux autres écrivains."""
    try:
        from services import tsl
        tsl.poser_tally("is07:%s" % rid, {})
    except Exception as e:
        log.debug("IS-07 entrant : extinction de %s impossible (%s)", rid, e)


def _index_de(shm, level_uuid):
    """Index de tally de ce flux CHEZ LE PORTEUR du niveau, ou None.

    ⚠ CHEZ LE PORTEUR, pas dans une table à plat : deux porteurs peuvent employer le même index
    pour des sources différentes, et se tromper de table allume un rouge sur le mauvais signal.
    C'est la même résolution que le distributeur — on l'appelle, on ne la refait pas."""
    try:
        from app.database import db_get_tsl_connections, db_get_tsl_mapping
        from services.tsl import resolve_ref
        cible = resolve_ref(shm) or shm
        for c in db_get_tsl_connections():
            if c.get("level_uuid") != level_uuid:
                continue
            for m in db_get_tsl_mapping(c["id"]):
                ref = (m.get("source_shm") or "").strip()
                if ref and (resolve_ref(ref) or ref) == cible:
                    return int(m.get("tsl_index") or 0)
    except Exception as e:
        log.debug("IS-07 entrant : index de %s indéterminable (%s)", shm, e)
    return None


def _shm_du_groupe(rid):
    """Le flux vidéo du groupe de sortie que ce Receiver accompagne, ou None.

    Le tally reçu porte sur le SIGNAL du groupe — pas sur une essence : la vidéo, l'audio et l'ANC
    d'un même signal partagent son tally. On prend donc le flux vidéo du groupe comme représentant,
    celui-là même que la correspondance TSL adresse."""
    from . import _senders, _send_state, GROUPHINT_TAG
    from .is07 import _rid_recv
    # ⚠ LE FLUX N'EST PAS DANS LES ÉTIQUETTES. Un Sender IS-04 ne porte pas son nom de shm — il
    # est dans l'état IS-05 (`_send_state[sid]["shm"]`), qui est le seul endroit où on le range.
    # Le déduire de l'étiquette de groupe reviendrait à ré-dériver ce que le module tient déjà.
    for sid, s in list(_senders.items()):
        for gh in (s.get("tags") or {}).get(GROUPHINT_TAG, []):
            if _rid_recv(str(gh).split(":", 1)[0]) != rid:
                continue
            st = _send_state.get(sid) or {}
            # La VIDÉO représente le signal : elle, l'audio et l'ANC partagent son tally, et
            # c'est elle que la correspondance TSL adresse.
            if st.get("shm") and st.get("essence", "video") == "video":
                return st["shm"]
    return None


def activer(rid, actif, connection_uri=None, source_ids=None):
    """Appelée par l'activation IS-05. Démarre ou arrête l'écoute de ce Receiver.

    Renvoie un court diagnostic, journalisé : une activation qui ne prend pas doit DIRE pourquoi.
    Sans ça, un contrôleur voit un 200, l'exploitant voit une lampe éteinte, et rien ne relie les
    deux."""
    from .is07_client import ClientIS07
    with _lock:
        ancien = _clients.pop(rid, None)
    if ancien:
        ancien.arreter()
    # ⚠ DEUX CHEMINS D'EXTINCTION, ET C'EST VOULU. Le client annonce déjà « je ne sais plus rien »
    # en se fermant (`_fermer` → `sur_etat(None, None)`), donc celui-ci fait souvent double
    # emploi — une mutation l'a montré. Il reste parce qu'il couvre le cas que l'autre ne couvre
    # pas : désactiver un Receiver dont AUCUN client ne tourne (activation refusée plus tôt, ou
    # contribution laissée par une exécution précédente). Sans lui, cette lampe-là ne s'éteint
    # jamais.
    _eteindre(rid)

    if not actif:
        return "arrêté"
    if not connection_uri:
        # On REFUSE de deviner l'URL. Elle vient du contrôleur, et un repli inventé
        # connecterait à un émetteur que personne n'a désigné.
        log.warning("IS-07 entrant : Receiver %s activé SANS connection_uri — rien à écouter", rid)
        return "sans connection_uri"
    niveau = niveaux().get(str(rid))
    if not niveau:
        log.warning("IS-07 entrant : Receiver %s activé mais AUCUN NIVEAU affecté — le tally "
                    "reçu n'irait nulle part (Réglages → NMOS)", rid)
        return "sans niveau affecté"
    shm = _shm_du_groupe(rid)
    if not shm:
        log.warning("IS-07 entrant : Receiver %s — groupe de sortie introuvable", rid)
        return "groupe introuvable"
    index = _index_de(shm, niveau)
    if index is None:
        log.warning("IS-07 entrant : Receiver %s — le flux %s n'a pas d'index chez le porteur du "
                    "niveau : le tally reçu ne serait adressable par personne", rid, shm)
        return "flux sans index de tally"

    def _sur_etat(source_id, valeur):
        # `(None, None)` = le client a perdu la liaison et ne sait plus rien.
        if source_id is None:
            _eteindre(rid)
            return
        from services import tsl
        tsl.poser_tally("is07:%s" % rid,
                        {} if valeur in (None, "off") else {(index, niveau): valeur})

    c = ClientIS07(connection_uri, source_ids or [], _sur_etat, nom="rx-%s" % rid[:8])
    with _lock:
        _clients[rid] = c
    c.demarrer()
    log.info("IS-07 entrant : Receiver %s écoute %s (source %s) → index %s, niveau %s",
             rid, connection_uri, (source_ids or ["?"])[0], index, niveau)
    return "en écoute"


def etat():
    """Ce que fait chaque Receiver entrant — pour l'interface et le diagnostic."""
    with _lock:
        clients = dict(_clients)
    aff = niveaux()
    return {rid: dict(c.etat(), niveau=aff.get(str(rid))) for rid, c in clients.items()}


def arreter_tout():
    with _lock:
        clients = dict(_clients)
        _clients.clear()
    for rid, c in clients.items():
        c.arreter()
        _eteindre(rid)
