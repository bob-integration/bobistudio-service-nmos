"""IS-09 — System Parameters : les constantes globales de l'installation.

Une seule ressource, et pourtant elle règle un problème récurrent : **sur quel domaine PTP est-on
d'accord ?** Dans une installation ST 2110, tous les équipements doivent partager le même
`domainNumber`. Aujourd'hui cela se configure appareil par appareil, à la main ; un désaccord se
manifeste par des symptômes qui ressemblent à tout sauf à un désaccord de domaine.

IS-09 permet à l'installation de le publier une fois, et aux équipements conformes de le lire.

═══ Deux directions, deux valeurs ════════════════════════════════════════════════════════════

**SERVEUR** — nous publions. Utile quand Bobi.Studio est le contrôleur de l'installation : un
appareil tiers démarre, découvre `_nmos-system._tcp`, et apprend notre domaine PTP et notre
collecteur syslog. C'est un service POUR LES AUTRES : il n'apporte rien tant qu'il n'y a pas
d'autres.

**CLIENT** — nous lisons celle de l'installation d'accueil. Utile quand Bobi arrive chez quelqu'un
d'autre : un plugfest, ou un client déjà équipé en NMOS.

═══ Pourquoi on ne CONFIGURE PAS automatiquement ═════════════════════════════════════════════

La spécification dit qu'un Node **récupère** la configuration globale. Elle ne dit pas ce qu'il
doit en faire — et c'est heureux, parce qu'appliquer un domaine PTP n'est pas un réglage : c'est
reconfigurer `ptp4l` sur chaque nœud, donc une opération sur le chemin de production. Se la faire
imposer par une annonce réseau serait exactement ce qu'on ne veut pas subir en direct.

Le client fait donc un **CONTRÔLE DE COHÉRENCE** : il lit, il compare, et il ALERTE en cas
d'écart. La décision d'aligner reste humaine — mais l'écart, lui, cesse d'être invisible.
"""

import json
import logging

from flask import jsonify

log = logging.getLogger(__name__)

VERSION = "v1.0"
BASE = "/x-nmos/system/" + VERSION
TYPE_DNSSD = "nmos-system"

# IS-09 : « Values 0 to 99 correspond to an active NMOS System API ». Même prudence que pour le
# registre : 100 = développement, à abaisser sciemment (cf. `_mdns_pri` dans __init__).
PRI_DEFAUT = 100


def actif():
    from . import _setting
    return str(_setting("nmos_is09", "0")).strip().lower() in ("1", "true", "on", "yes")


def _pri():
    from . import _setting
    try:
        return max(0, min(255, int(_setting("nmos_is09_pri", PRI_DEFAUT))))
    except (TypeError, ValueError):
        return PRI_DEFAUT


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Serveur : la ressource globale
# ══════════════════════════════════════════════════════════════════════════════════════════════

def domaine_ptp():
    """Domaine PTP DE L'INSTALLATION — le réglage global, pas celui d'un nœud."""
    from app import settings as st
    try:
        return max(0, min(127, int(st.get("ptp_domain") or 0)))
    except (TypeError, ValueError):
        return 0


def desaccords_internes():
    """[(nœud, domaine)] de nos propres nœuds qui divergent du domaine publié.

    Publier une valeur que nos propres machines n'appliquent pas serait pire que ne rien publier :
    un tiers s'alignerait sur une consigne que nous ne tenons pas nous-mêmes."""
    from app import settings as st
    from app.database import db_get_nodes
    voulu = domaine_ptp()
    out = []
    for n in db_get_nodes():
        try:
            d = int(st.setting_for("ptp_domain", n["id"]) or 0)
        except (TypeError, ValueError):
            continue
        if d != voulu:
            out.append((n["name"], d))
    return out


def _syslog():
    """Bloc `syslog` d'IS-09, ou None. On le publie SEULEMENT s'il est configuré : un bloc vide
    enverrait les journaux d'un tiers vers nulle part."""
    from app import settings as st
    hote = str(st.get("alerting_syslog_host", "") or "").strip()
    if not hote:
        return None
    try:
        port = max(1, min(65535, int(st.get("alerting_syslog_port", 514) or 514)))
    except (TypeError, ValueError):
        port = 514
    return {"hostname": hote, "port": port}


def globale():
    """La ressource `global` d'IS-09."""
    from . import _stable_uuid, _tai_version, HEARTBEAT_S, _setting
    res = {
        "id": _stable_uuid("is09:global"),
        "version": _tai_version(),
        "label": _setting("nmos_node_label", "Bobi.Studio"),
        "is04": {"heartbeat_interval": int(HEARTBEAT_S)},
        "ptp": {
            "domain_number": domaine_ptp(),
            # IS-09 borne cette valeur entre 2 et 10. On publie la valeur par défaut d'IEEE 1588
            # tant que le produit ne l'expose pas : annoncer un chiffre qu'on ne configure pas
            # serait moins faux que de ne rien annoncer — le champ est REQUIS.
            "announce_receipt_timeout": 3,
        },
    }
    sl = _syslog()
    if sl:
        res["syslog"] = sl
    return res


def enregistrer(bp):
    def _ferme():
        return jsonify({"code": 501, "error": "IS-09 désactivé (réglage `nmos_is09`)",
                        "debug": ""}), 501

    @bp.route("/x-nmos/system/", methods=["GET"])
    def is09_racine():
        return jsonify([VERSION + "/"])

    @bp.route(BASE + "/", methods=["GET"])
    def is09_v_racine():
        if not actif():
            return _ferme()
        return jsonify(["global/"])

    @bp.route(BASE + "/global", methods=["GET"])
    @bp.route(BASE + "/global/", methods=["GET"])
    def is09_global():
        if not actif():
            return _ferme()
        return jsonify(globale())


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Client : lire la System API de l'installation d'accueil, et COMPARER
# ══════════════════════════════════════════════════════════════════════════════════════════════

def decouvrir(duree_s=6):
    """[(nom, url, pri)] des System API annoncées sur le réseau, triées par priorité.

    On lit `pri` et on écarte 100+ : IS-09 les réserve au développement, et s'aligner sur une
    System API de test serait pire que de n'en trouver aucune."""
    try:
        from zeroconf import ServiceBrowser, Zeroconf
    except Exception as e:
        log.info("nmos/is09 : zeroconf indisponible (%s)", e)
        return []
    import time
    trouves = []

    class _L:
        def add_service(self, zc, type_, name):
            i = zc.get_service_info(type_, name, timeout=3000)
            if not i or not i.addresses:
                return
            txt = {(k.decode() if isinstance(k, bytes) else k):
                   ((v or b"").decode() if isinstance(v, bytes) else v)
                   for k, v in (i.properties or {}).items()}
            try:
                pri = int(txt.get("pri", 100))
            except (TypeError, ValueError):
                pri = 100
            proto = txt.get("api_proto", "http")
            import socket as _s
            ip = _s.inet_ntoa(i.addresses[0])
            trouves.append((name.split(".")[0], "%s://%s:%d%s" % (proto, ip, i.port, BASE), pri))

        def update_service(self, *a):
            pass

        def remove_service(self, *a):
            pass

    zc = Zeroconf()
    try:
        ServiceBrowser(zc, "_%s._tcp.local." % TYPE_DNSSD, _L())
        time.sleep(duree_s)
    finally:
        zc.close()
    return sorted([t for t in trouves if t[2] < 100], key=lambda t: t[2])


def lire(url):
    """Ressource `global` d'une System API tierce. Lève sur erreur."""
    import requests
    r = requests.get(url.rstrip("/") + "/global", timeout=8)
    r.raise_for_status()
    return r.json()


def verifier(url=None):
    """CONTRÔLE DE COHÉRENCE — lit la System API de l'installation et compare à nos réglages.

    N'applique RIEN : voir l'entête. Renvoie un dict lisible, et lève une alerte en cas d'écart —
    un désaccord de domaine PTP se manifeste sinon par des symptômes qui ne lui ressemblent pas."""
    if url is None:
        trouvees = decouvrir()
        if not trouvees:
            return {"verdict": "aucune System API annoncée sur le réseau"}
        url = trouvees[0][1]
    try:
        g = lire(url)
    except Exception as e:
        return {"verdict": "System API injoignable", "url": url, "detail": str(e)[:120]}

    leur = (g.get("ptp") or {}).get("domain_number")
    notre = domaine_ptp()
    res = {"url": url, "leur_domaine_ptp": leur, "notre_domaine_ptp": notre,
           "verdict": "concordant" if leur == notre else "ÉCART"}
    if leur != notre:
        try:
            from app.database import db_add_alert
            db_add_alert(
                "PTP : l'installation annonce le domaine %s (IS-09), nos nœuds sont réglés sur %s"
                % (leur, notre), "warning", kind="ptp")
        except Exception as e:
            log.warning("nmos/is09 : alerte non écrite (%s)", e)
    interne = desaccords_internes()
    if interne:
        res["noeuds_divergents"] = interne
    return res


def resume():
    return json.dumps(verifier(), ensure_ascii=False, indent=1)
