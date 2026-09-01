# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Le modèle d'appareil MS-05-02 de l'orchestrateur — construit une fois, servi par plusieurs
protocoles.

IS-12 (WebSocket, `is12.py`) et IS-14 (REST, `is14.py`) exposent LE MÊME modèle : mêmes objets,
mêmes oids, mêmes statuts BCP-008. Ce module en détient le cycle de vie pour qu'aucun des deux ne
soit propriétaire de l'autre — activer IS-14 seul doit marcher, activer les deux ne doit pas
construire deux modèles qui divergeraient sous les yeux d'un contrôleur lisant les deux.

D'où le compteur de références : chaque protocole `acquiert` le modèle au démarrage et le `libère`
à l'arrêt ; il n'est bâti qu'à la première acquisition et démonté qu'à la dernière libération.
"""
import json
import logging
import threading
import time

from . import monitors, ncp

log = logging.getLogger(__name__)

PERIODE_ECH_S = 1.0                    # cadence d'échantillonnage des monitors

_lock = threading.RLock()
_sync_lock = threading.Lock()
_arret = threading.Event()
_appareil = None
_bloc_rx = None
_bloc_tx = None
_bloc_plugins = None
_moniteurs = {}          # resource_id (IS-04) → moniteur
_thread_sampler = None
_refs = set()            # protocoles qui tiennent le modèle : "is12", "is14"
_etat = {"ignores": 0}


def appareil():
    return _appareil


def actif():
    return _appareil is not None


def references():
    with _lock:
        return sorted(_refs)


def nb_monitors():
    with _lock:
        rx = sum(1 for m in _moniteurs.values() if m.ctx.sens == "rx")
        return rx, len(_moniteurs) - rx, _etat.get("ignores", 0)


def acquerir(nom):
    """Construit le modèle si besoin et enregistre `nom` comme utilisateur. Idempotent."""
    global _appareil, _bloc_rx, _bloc_tx, _bloc_plugins, _thread_sampler
    with _lock:
        deja = bool(_refs)
        _refs.add(nom)
        if deja and _appareil is not None:
            return True
        ncp.registre()                   # échoue tôt et bruyamment si les modèles manquent
        _appareil = _construire_appareil()
        _bloc_rx = _appareil.bloc("receivers", "Monitors des Receivers (BCP-008-01)")
        _bloc_tx = _appareil.bloc("senders", "Monitors des Senders (BCP-008-02)")
        # Nos classes non standard doivent être au registre AVANT toute instanciation :
        # `NcObject.__init__` y lit les descripteurs de propriétés de sa classe.
        from . import plugins_ncp as _pncp
        _pncp.enregistrer_classes()
        _bloc_plugins = _appareil.bloc("plugins", "Paramètres pilotables des plugins")
        _arret.clear()
        _thread_sampler = threading.Thread(target=_sampler_loop, name="nc-sampler", daemon=True)
        _thread_sampler.start()
    sync_model()
    log.info("Modèle de contrôle bâti (demandé par %s)", nom)
    return True


def liberer(nom):
    """Retire `nom` ; démonte le modèle quand plus personne ne le tient."""
    global _appareil, _bloc_rx, _bloc_tx, _bloc_plugins, _thread_sampler
    with _lock:
        _refs.discard(nom)
        if _refs or _appareil is None:
            return
        _arret.set()
        t, _thread_sampler = _thread_sampler, None
    if t is not None:
        t.join(timeout=3)
    with _lock:
        _moniteurs.clear()
        _appareil = _bloc_rx = _bloc_tx = _bloc_plugins = None
        _etat["ignores"] = 0
    log.info("Modèle de contrôle démonté (dernier utilisateur : %s)", nom)


# ═════════════════════════════════════════════════════════════════════════════════════════════
# Modèle d'appareil : construction et synchronisation avec le modèle NMOS
# ═════════════════════════════════════════════════════════════════════════════════════════════

def _construire_appareil():
    from app.database import db_get_setting
    from . import asset_info, _nom_instance
    # MÊME source que les tags BCP-002-02 de l'IS-04 (`nmos.asset_info`) : un contrôleur qui lit
    # les deux protocoles doit y trouver la même identité, pas deux versions du même appareil.
    a = asset_info()
    # Chaque valeur dans SON champ, sans concaténation ni emprunt (MS-05-02 §NcProduct) :
    #   · brandName = « brand name under which product is sold » → la marque du PRODUIT. Elle
    #     portait le nom de l'entreprise CLIENTE : un contrôleur voyait « Bobi.Studio » vendu sous
    #     la marque du client. Corrigé le 2026-09-01.
    #   · uuid = « unique UUID of product (NOT product instance) » → constant. Il portait l'UUID de
    #     l'installation, que `serialNumber` publie déjà juste en dessous : deux déploiements
    #     annonçaient deux produits distincts, donc impossibles à regrouper.
    #   · deviceName = « instance name, NOT product name » → le nom du système, pas « Bobi.Studio ».
    # `userInventoryCode` reste NUL, et volontairement : il est écrivable par un contrôleur
    # (« asset tracking identifier, user specified ») — c'est le code d'inventaire de l'exploitant.
    # Y poser notre nom d'entreprise l'écraserait à chaque démarrage. L'organisation et le lieu
    # partent en tags IS-04 propriétaires (cf. nmos.TAG_ORGANISATION / TAG_LOCATION).
    produit = {
        "name": a["product"],
        "key": "bobistudio-orchestrateur",
        "revisionLevel": str(db_get_setting("app_version", "") or "0"),
        "brandName": a["brand"],
        "uuid": a["product_uuid"],
        "description": "Orchestrateur de production ST 2110 sur bus MXL",
    }
    fabricant = {"name": a["manufacturer"], "organizationId": None, "website": None}
    app = ncp.Appareil(produit, fabricant, serial=a["instance_id"],
                       device_name=_nom_instance())
    return app


def _type_conteneur(vmid):
    from app.database import db_get_container
    try:
        c = db_get_container(vmid) or {}
        dc = c.get("deploy_config")
        dc = json.loads(dc) if isinstance(dc, str) else (dc or {})
        return dc.get("type")
    except Exception:
        return None


def _cible_receiver(rid):
    """{vmid, essence, idx} du flux RX servant ce receiver IS-04, ou None."""
    from . import _recv_state
    st = _recv_state.get(rid)
    if not st or st.get("recv_idx") is None:
        return None
    return {"vmid": st.get("vmid"), "essence": st.get("essence") or "video",
            "idx": st.get("recv_idx")}


def _cible_sender(sid):
    """{vmid, essence, idx, sub_idx} du slot TX servant ce sender IS-04, ou None.

    Seuls les slots du moteur bi-rôle (`tx_idx`) sont supervisables : ce sont les seuls dont la
    télémétrie remonte par flux. Un sender sans `tx_idx` n'a pas de mesure derrière lui — on
    préfère ne pas lui donner de monitor plutôt que de lui en donner un qui invente."""
    from . import _send_state
    st = _send_state.get(sid)
    if not st or st.get("tx_idx") is None:
        return None
    essence = st.get("essence") or "video"
    return {"vmid": st.get("vmid"), "essence": essence, "idx": st.get("tx_idx"),
            "sub_idx": st.get("audio_idx") if essence == "audio" else None}


def _actif_receiver(rid):
    from . import _recv_state
    return bool(((_recv_state.get(rid) or {}).get("active") or {}).get("master_enable"))


def _actif_sender(sid):
    """Un sender est ACTIF quand IS-05 l'a activé *et* que le moteur est configuré pour émettre.

    `master_enable` vaut True par défaut sur nos senders (le flux existe dès que le slot est
    déclaré) : s'y fier seul ferait passer pour « en panne » un slot déclaré mais volontairement
    au repos. La cadence nominale du slot est ce qui exprime l'intention d'émettre."""
    from . import _send_state
    from app import metrics
    st = _send_state.get(sid) or {}
    if not (st.get("active") or {}).get("master_enable"):
        return False
    cible = _cible_sender(sid)
    if not cible:
        return False
    flux = metrics.etat_flux(cible["vmid"], "tx", cible["essence"], cible["idx"],
                             cible.get("sub_idx"))
    if flux is None:
        return False
    nominal = flux.get("fps_nominal")
    return True if nominal is None else float(nominal or 0) > 0


def sync_model():
    """Aligne l'arbre des monitors sur le modèle NMOS courant. Idempotent, appelable à chaud."""
    if _appareil is None:
        return
    from . import _receivers, _senders
    voulus = {}
    ignores = 0
    for rid, res in list(_receivers.items()):
        cible = _cible_receiver(rid)
        if not cible or _type_conteneur(cible["vmid"]) != "2110_io":
            ignores += 1
            continue
        voulus[rid] = ("rx", res.get("label") or rid, cible)
    for sid, res in list(_senders.items()):
        cible = _cible_sender(sid)
        if not cible or _type_conteneur(cible["vmid"]) != "2110_io":
            ignores += 1
            continue
        voulus[sid] = ("tx", res.get("label") or sid, cible)

    # `_sync_lock` sérialise les synchronisations concurrentes (rebuild_model est appelé depuis
    # plusieurs threads). `_lock`, lui, ne protège que le dictionnaire : les mutations de l'ARBRE
    # se font en dehors, parce qu'elles notifient les sessions abonnées — et notifier sous le verrou
    # d'état ferait dépendre l'ouverture d'une nouvelle session de la vitesse d'un client existant.
    with _sync_lock:
        with _lock:
            a_retirer = [(r, _moniteurs.pop(r)) for r in list(_moniteurs) if r not in voulus]
            a_creer = [(r, v) for r, v in voulus.items() if r not in _moniteurs]
            relabel = [(_moniteurs[r], v[1]) for r, v in voulus.items() if r in _moniteurs]
            _etat["ignores"] = ignores
        for _rid, mon in a_retirer:
            _appareil.retirer(_bloc_rx if mon.ctx.sens == "rx" else _bloc_tx, mon)
        for mon, label in relabel:
            mon.poser("userLabel", label)          # le libellé NMOS peut être renommé à tout moment
        for rid, (sens, label, _cible) in a_creer:
            if sens == "rx":
                ctx = monitors.Contexte("rx", lambda rid=rid: _cible_receiver(rid),
                                        lambda rid=rid: _actif_receiver(rid))
                mon = monitors.MoniteurReceiver(_appareil, _appareil.oid_libre(),
                                                "receiver_%s" % rid, _bloc_rx.oid, rid, label, ctx)
                _appareil.ajouter(_bloc_rx, mon)
            else:
                ctx = monitors.Contexte("tx", lambda rid=rid: _cible_sender(rid),
                                        lambda rid=rid: _actif_sender(rid))
                mon = monitors.MoniteurSender(_appareil, _appareil.oid_libre(),
                                              "sender_%s" % rid, _bloc_tx.oid, rid, label, ctx)
                _appareil.ajouter(_bloc_tx, mon)
            with _lock:
                _moniteurs[rid] = mon
        # Paramètres de plugin (MS-05-02) — best-effort : le monitoring BCP-008 est du chemin
        # de supervision, il ne doit pas tomber parce qu'un plugin a un param_tree douteux.
        try:
            from . import plugins_ncp as _pncp
            _pncp.sync(_appareil, _bloc_plugins)
        except Exception as e:
            log.warning("MS-05-02 : synchronisation des plugins échouée : %s", e)

        if a_creer or a_retirer:
            # Journalisé seulement quand l'ensemble CHANGE : `rebuild_model` est appelé à chaque
            # changement d'état de conteneur, une ligne par appel noierait le journal.
            with _lock:
                rx = sum(1 for m in _moniteurs.values() if m.ctx.sens == "rx")
                total = len(_moniteurs)
            log.info("IS-12 : %d monitors (%d RX, %d TX) — %+d ; %d ressources IS-04 sans "
                     "télémétrie 2110 et donc sans monitor",
                     total, rx, total - rx, len(a_creer) - len(a_retirer), ignores)


def _sampler_loop():
    """Recalcule les statuts et laisse les monitors émettre leurs PropertyChanged.

    Aucune I/O réseau ici : tout vient des caches remplis par la boucle de surveillance. La
    cadence est plus rapide que celle de la surveillance (5 s) pour que les temporisations de la
    BCP (`statusReportingDelay`, 3 s par défaut) restent justes à la seconde."""
    while not _arret.wait(PERIODE_ECH_S):
        try:
            with _lock:
                lot = list(_moniteurs.values())
            maintenant = time.monotonic()
            for mon in lot:
                try:
                    mon.echantillonner(maintenant)
                except Exception:
                    log.exception("IS-12 : échantillonnage du monitor %s", mon.role)
        except Exception:
            log.exception("IS-12 : boucle d'échantillonnage")




def etat_monitors():
    """Vue à plat des monitors pour l'UI de diagnostic (page Réglages → NMOS)."""
    with _lock:
        lot = list(_moniteurs.items())
    out = []
    for rid, m in lot:
        v = m._vals
        out.append({
            "resource_id": rid, "sens": m.ctx.sens, "role": m.role, "oid": m.oid,
            "label": v.get("userLabel"),
            "overall": v.get("overallStatus"), "message": v.get("overallStatusMessage"),
            "link": v.get("linkStatus"), "sync": v.get("externalSynchronizationStatus"),
            "connection": v.get("connectionStatus"), "stream": v.get("streamStatus"),
            "transmission": v.get("transmissionStatus"), "essence": v.get("essenceStatus"),
        })
    out.sort(key=lambda r: (r["sens"], r["label"] or ""))
    return out
