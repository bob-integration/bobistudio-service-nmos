"""Raccordement : les statuts BCP-008 d'un appareil TIERS arrivent dans NOS alertes.

C'est le bout de la chaîne. `client_is12.py` sait recevoir les notifications d'un pair ;
`registre.py` sait quels pairs existent. Ici on les branche : pour chaque Node tiers enregistré,
on découvre ses monitors BCP-008, on s'y abonne, et **un changement de statut chez lui devient une
alerte chez nous** — la même que pour nos propres flux, dans la même page.

Sans ce module, on aurait deux supervisions côte à côte : la nôtre dans l'UI, celle des tiers dans
un client NMOS qu'il faudrait regarder à part. Un exploitant ne surveille pas deux écrans.

═══ Ce qui est traduit, et ce qui ne l'est pas ═══════════════════════════════════════════════

On ne remonte que **`overallStatus`**, la synthèse que BCP-008 définit précisément pour ça. Les
statuts de domaine (connexion, flux, lien, synchronisation) sont dans le MESSAGE joint, pas en
alertes séparées : les remonter tous ferait quatre lignes pour un seul incident, et l'exploitant
apprendrait à ne plus les lire.

═══ Deux règles qui viennent de nos propres erreurs ══════════════════════════════════════════

**On compare à l'INTENTION.** Un monitor `Inactive` n'est PAS en panne : il décrit une ressource
qu'on n'a pas demandé d'utiliser. Le remonter en alerte fabriquerait du bruit permanent sur tout
appareil ayant des receivers libres — c'est-à-dire sur tous.

**On ne remonte que les CHANGEMENTS.** Les notifications IS-12 le sont déjà par nature ; mais à la
reconnexion on relit l'état courant, et sans mémoire on ré-alerterait sur chaque reprise de
liaison. Une alerte qui se répète à chaque hoquet réseau finit par masquer les vraies.
"""

import logging
import threading
import time

log = logging.getLogger(__name__)

# NcOverallStatus (feature set Monitoring). Miroir de `monitors.py` — mêmes rangs des deux côtés.
INACTIVE, HEALTHY, PARTIALLY_HEALTHY, UNHEALTHY = 0, 1, 2, 3
_NOM = {INACTIVE: "inactif", HEALTHY: "sain",
        PARTIALLY_HEALTHY: "partiellement dégradé", UNHEALTHY: "en panne"}
# Niveau d'alerte par statut. `Inactive` et `Healthy` ne produisent RIEN : voir l'entête.
_NIVEAU = {PARTIALLY_HEALTHY: "warning", UNHEALTHY: "error"}

CLASSE_MONITOR_RX = [1, 2, 2, 1]
CLASSE_MONITOR_TX = [1, 2, 2, 2]
# `overallStatus` est publié au niveau 3, index 1 du NcStatusMonitor (BCP-008 § 3p1).
PROP_OVERALL = {"level": 3, "index": 1}
PROP_MESSAGE = {"level": 3, "index": 2}

RECONNEXION_S = 15          # palier fixe : voir _boucle_pair

_fils = {}                  # url → thread
_etat = {}                  # url → dict lisible
_dernier = {}               # (url, oid) → statut déjà signalé
_arret = threading.Event()
_lock = threading.RLock()


def actif():
    """Réglage `nmos_supervision_tiers` — FERMÉ par défaut. Ouvrir des sessions WebSocket
    permanentes vers des équipements tiers n'est pas un défaut raisonnable."""
    from . import _setting
    return str(_setting("nmos_supervision_tiers", "0")).strip().lower() in ("1", "true", "on", "yes")


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Découverte des pairs
# ══════════════════════════════════════════════════════════════════════════════════════════════

def pairs():
    """[(label, url ws)] des Nodes du registre qui annoncent un point de contrôle IS-12.

    On lit le REGISTRE, pas une liste configurée à la main : un appareil qui s'est enregistré est
    un appareil qui existe, et qui a dit lui-même où le joindre."""
    from . import registre
    from .client_ncp import TYPE_IS12, point_de_controle
    out = []
    for dev in registre.ressources("device"):
        href = point_de_controle(dev, TYPE_IS12)
        if href:
            out.append((dev.get("label") or dev.get("id"), href))
    return out


def _monitors(client):
    """[(oid, role, sens)] des monitors BCP-008 du pair, par leur classId NORMALISÉ.

    On ne devine pas au nom du rôle : un éditeur nomme ses blocs comme il veut, et filtrer sur
    « receiver » dans le rôle marcherait chez nous et nulle part ailleurs."""
    membres = (client.commander(1, (2, 1), {"recurse": True}) or {}).get("value") or []
    out = []
    for m in membres:
        cid = m.get("classId")
        if cid == CLASSE_MONITOR_RX:
            out.append((m.get("oid"), m.get("role"), "rx"))
        elif cid == CLASSE_MONITOR_TX:
            out.append((m.get("oid"), m.get("role"), "tx"))
    return out


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Traduction en alertes
# ══════════════════════════════════════════════════════════════════════════════════════════════

def _signaler(url, label, oid, role, sens, statut, message):
    """Une alerte par CHANGEMENT de statut, et seulement pour les statuts qui en méritent une."""
    cle = (url, oid)
    with _lock:
        if _dernier.get(cle) == statut:
            return False
        _dernier[cle] = statut
    niveau = _NIVEAU.get(statut)
    if niveau is None:
        # Retour à la normale : on ne crée pas d'alerte « tout va bien », mais on l'a mémorisé
        # ci-dessus pour que la prochaine dégradation soit bien vue comme un changement.
        log.info("supervision tiers : %s/%s revenu à « %s »", label, role, _NOM.get(statut, statut))
        return False
    try:
        from app.database import db_add_alert
        db_add_alert("Appareil tiers « %s » — %s %s : %s%s"
                     % (label, "receiver" if sens == "rx" else "sender", role,
                        _NOM.get(statut, statut), (" (%s)" % message) if message else ""),
                     niveau, kind="signal")
    except Exception as e:
        log.warning("supervision tiers : alerte non écrite (%s)", e)
    return True


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Boucle par pair
# ══════════════════════════════════════════════════════════════════════════════════════════════

def _boucle_pair(label, url):
    from .client_is12 import Client, ErreurIS12
    while not _arret.is_set():
        try:
            with Client(url, timeout=8) as c:
                mons = _monitors(c)
                par_oid = {oid: (role, sens) for oid, role, sens in mons}
                if not par_oid:
                    _maj(url, label, "aucun monitor BCP-008 publié par ce pair", 0)
                    # Rien à surveiller : on ne martèle pas sa session pour autant.
                    _arret.wait(60)
                    continue
                retenus = c.abonner(list(par_oid))
                # Le pair décide ce qu'il retient. Le vérifier évite d'attendre indéfiniment des
                # notifications qui ne viendront jamais, et de croire la supervision en place.
                ignores = set(par_oid) - set(retenus)
                if ignores:
                    log.warning("supervision tiers %s : %d monitor(s) NON retenus à l'abonnement",
                                label, len(ignores))
                _maj(url, label, "abonné à %d monitor(s)" % len(retenus), len(retenus))

                # État initial : une notification ne dit que les CHANGEMENTS. Sans cette lecture,
                # un pair déjà en panne au moment où l'on se connecte resterait silencieux.
                for oid in retenus:
                    try:
                        st = (c.commander(oid, (1, 1), {"id": PROP_OVERALL}) or {}).get("value")
                        msg = (c.commander(oid, (1, 1), {"id": PROP_MESSAGE}) or {}).get("value")
                        role, sens = par_oid[oid]
                        _signaler(url, label, oid, role, sens, st, msg)
                    except ErreurIS12 as e:
                        log.info("supervision tiers %s : état initial de %s illisible (%s)",
                                 label, oid, e)

                while not _arret.is_set():
                    for n in c.notifications(duree_s=30):
                        if n.get("propertyId") != PROP_OVERALL:
                            continue
                        oid = n.get("oid")
                        if oid not in par_oid:
                            continue
                        role, sens = par_oid[oid]
                        _signaler(url, label, oid, role, sens, n.get("value"), None)
        except ErreurIS12 as e:
            _maj(url, label, "injoignable : %s" % str(e)[:80], 0)
        except Exception as e:                                       # pragma: no cover
            log.warning("supervision tiers %s : %s", label, e)
            _maj(url, label, "erreur : %s" % str(e)[:80], 0)
        # ⚠ PALIER FIXE, pas de backoff exponentiel. Un équipement de production qui redémarre
        # revient en quelques secondes ; un backoff qui monterait à plusieurs minutes nous ferait
        # rater sa remontée, et la supervision serait aveugle précisément après un incident.
        _arret.wait(RECONNEXION_S)


def _maj(url, label, message, n):
    with _lock:
        _etat[url] = {"label": label, "url": url, "message": message,
                      "monitors": n, "vu": time.strftime("%H:%M:%S")}


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Cycle de vie
# ══════════════════════════════════════════════════════════════════════════════════════════════

def demarrer():
    """Ouvre une session par pair connu. Idempotent : rappelable quand le registre bouge."""
    if not actif():
        return 0
    _arret.clear()
    n = 0
    for label, url in pairs():
        with _lock:
            vivant = _fils.get(url)
            if vivant is not None and vivant.is_alive():
                continue
            t = threading.Thread(target=_boucle_pair, args=(label, url), daemon=True,
                                 name="nmos-tiers-%s" % (label or url)[:20])
            _fils[url] = t
        t.start()
        n += 1
    if n:
        log.info("supervision tiers : %d session(s) IS-12 ouverte(s)", n)
    return n


def arreter():
    _arret.set()
    with _lock:
        _fils.clear()
        _etat.clear()
        _dernier.clear()


def etat():
    """Vue lisible pour l'UI et le diagnostic."""
    with _lock:
        return {"actif": actif(), "pairs": list(_etat.values())}


def enregistrer(bp):
    """Vue interne — ce que la supervision voit, pour l'UI et le diagnostic. Ce n'est pas du NMOS :
    d'où le préfixe /api/, comme l'inventaire du registre."""
    from flask import jsonify
    from app.auth import require_login

    @bp.route("/api/nmos/supervision", methods=["GET"])
    @require_login
    def api_supervision_tiers():
        return jsonify(etat())
