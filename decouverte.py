"""IS-04 — DÉCOUVERTE du registre de l'installation d'accueil (DNS-SD).

Ce qui manquait. Nous ANNONCIONS `_nmos-register._tcp` quand notre registre embarqué est ouvert,
mais nous ne l'avons jamais CHERCHÉ : il fallait saisir `nmos_registry_url` à la main. Un Node qui
ne sait pas trouver le registre de la salle où on le branche ne peut pas « apparaître » — c'est
exactement le scénario d'un plugfest, et c'est la cause unique des six échecs `test_04` à `test_11`
de la suite AMWA IS-04-01.

═══ Trois règles, et elles comptent toutes les trois ═════════════════════════════════════════

**1. LE RÉGLAGE EXPLICITE GAGNE TOUJOURS.** Si l'exploitant a saisi une URL, on ne la contredit
pas. Une découverte qui écraserait une décision humaine transformerait un réglage en suggestion.

**2. ON S'EXCLUT NOUS-MÊMES — PAR (IP, PORT), PAS PAR IP.** Notre propre registre s'annonce sur le
même LAN. S'y enregistrer serait une boucle : nous serions notre propre registre, nos ressources
apparaîtraient chez nous, et le tout aurait l'air de marcher.

⚠ Mais exclure toute NOTRE IP est trop large, et ça s'est vu : un registre tiers hébergé sur la
même machine (l'orchestrateur et le registre d'un intégrateur cohabitant sur un serveur, ou
simplement un banc de test) était écarté à tort. Notre registre est celui de NOTRE port ; un autre
port sur la même adresse est quelqu'un d'autre. Ce défaut-là n'apparaît qu'en cohabitation — en
exploitation classique le registre est ailleurs, et il serait resté invisible.

**3. `pri` ≥ 100 EST ÉCARTÉ.** IS-04 : « Values 0 to 99 correspond to an active NMOS Registration
API (zero being the highest priority). Values 100+ are reserved for development work to avoid
colliding with a live system. » S'enregistrer dans un registre de développement, c'est disparaître
de l'installation réelle sans qu'aucune erreur ne soit levée.

═══ Ce que ça ne fait PAS ════════════════════════════════════════════════════════════════════

Pas de bascule instantanée : la boucle réévalue à intervalle, elle ne surveille pas le registre en
temps réel. Un registre qui tombe est remplacé au tour suivant, pas à la seconde. C'est assumé —
un mécanisme de bascule nerveux ferait plus de dégâts qu'une reprise tranquille, et nous avons
déjà payé cher une reconnexion sans palier sur ce produit.
"""

import logging
import socket
import threading
import time

log = logging.getLogger(__name__)

TYPE_DNSSD = "nmos-register"
PRI_DEVELOPPEMENT = 100          # IS-04 : 100+ = développement, à ne jamais choisir
# DEUX cadences, et la distinction n'est pas cosmétique :
#  · SANS registre, nous sommes INVISIBLES de l'installation. Il faut chercher souvent — et une
#    installation d'accueil qui allume son registre doit nous voir apparaître en quelques secondes,
#    pas au bout de deux minutes. (La suite AMWA laisse 30 s après son annonce ; au-delà, elle
#    conclut que le Node ne sait pas découvrir. Elle a raison de le conclure.)
#  · AVEC un registre qui répond, rien ne presse : réévaluer souvent, c'est risquer de basculer
#    d'un registre sain vers un autre au moindre hoquet. Ce produit a déjà perdu un nœud sur une
#    reconnexion sans palier — on ne rejoue pas ça pour gagner une minute.
PERIODE_ACQUISITION_S = 10.0     # tant qu'on n'a PAS de registre
PERIODE_S = 120.0                # quand on en a un qui répond
DUREE_BROWSE_S = 4.0

_thread = None
_running = False
_courant = {"url": None, "origine": None, "pri": None}


def actif():
    from . import _setting
    return str(_setting("nmos_decouverte", "0")).strip().lower() in ("1", "true", "on", "yes")


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Nos propres adresses — pour ne pas se découvrir soi-même
# ══════════════════════════════════════════════════════════════════════════════════════════════

NOTRE_PORT = 5000                # port de notre Node API, donc de notre registre embarqué


def _nos_adresses():
    """IP par lesquelles NOTRE orchestrateur peut être atteint.

    On ratisse large à dessein : rater une de nos adresses nous ferait nous enregistrer chez
    nous-mêmes, et cette panne-là est silencieuse — tout répond, tout paraît juste. Le port, lui,
    est ce qui NOUS distingue d'un tiers hébergé sur la même machine (cf. règle 2 de l'entête)."""
    ips = set()
    try:
        from . import _get_host_address
        h = _get_host_address()
        if h:
            ips.add(str(h))
    except Exception:
        pass
    ips.add("127.0.0.1")
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.add(info[4][0])
    except Exception:
        pass
    try:                                     # toutes les IPv4 des interfaces up
        import netifaces                     # présent via les dépendances zeroconf
        for iface in netifaces.interfaces():
            for a in netifaces.ifaddresses(iface).get(netifaces.AF_INET, []):
                if a.get("addr"):
                    ips.add(a["addr"])
    except Exception:
        pass
    return ips


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Découverte
# ══════════════════════════════════════════════════════════════════════════════════════════════

def decouvrir(duree_s=DUREE_BROWSE_S):
    """[{nom, url, pri, ip, port}] des Registration API annoncées, triées par priorité croissante.

    Écarte `pri` ≥ 100 et nos propres annonces. Liste vide si zeroconf est absent : on ne devine
    pas un registre, on constate qu'on n'en a pas trouvé."""
    try:
        from zeroconf import ServiceBrowser, Zeroconf
    except Exception as e:
        log.info("nmos/découverte : zeroconf indisponible (%s)", e)
        return []

    nôtres = _nos_adresses()
    trouves, vus = [], set()

    class _L:
        def add_service(self, zc, type_, name):
            try:
                i = zc.get_service_info(type_, name, timeout=3000)
            except Exception:
                return
            if not i or not i.addresses:
                return
            txt = {(k.decode() if isinstance(k, bytes) else k):
                   ((v or b"").decode() if isinstance(v, bytes) else v)
                   for k, v in (i.properties or {}).items()}
            try:
                pri = int(txt.get("pri", PRI_DEVELOPPEMENT))
            except (TypeError, ValueError):
                pri = PRI_DEVELOPPEMENT
            ip = socket.inet_ntoa(i.addresses[0])
            port = int(i.port or 0)
            if (ip, port) in vus:
                return
            vus.add((ip, port))
            if ip in nôtres and port == NOTRE_PORT:
                log.debug("nmos/découverte : %s:%d ignoré, c'est NOTRE registre", ip, port)
                return
            if pri >= PRI_DEVELOPPEMENT:
                log.info("nmos/découverte : registre %s:%d écarté — pri=%d, réservé au "
                         "développement par IS-04", ip, port, pri)
                return
            proto = txt.get("api_proto", "http")
            ver = txt.get("api_ver", "")
            from . import IS04_VERSION
            if ver and IS04_VERSION not in [v.strip() for v in ver.split(",")]:
                log.info("nmos/découverte : registre %s:%d écarté — annonce api_ver=%r, nous "
                         "parlons %s", ip, port, ver, IS04_VERSION)
                return
            # ⚠ `url` est l'ORIGINE NUE, pas l'URL de l'API. C'est ce qu'attend `start()` →
            # `_register_all(reg_base)`, qui construit lui-même
            # `{base}/x-nmos/registration/{ver}/resource`. Rendre l'URL complète ici produisait un
            # chemin DOUBLÉ (`…/x-nmos/registration/v1.3/x-nmos/registration/v1.3/resource`) et un
            # 404 à chaque POST — mesuré le 2026-08-31, et invisible sans regarder le registre d'en
            # face : de notre côté, seule une alerte d'échec d'enregistrement remontait.
            trouves.append({"nom": name.split(".")[0], "ip": ip, "port": port, "pri": pri,
                            "url": "%s://%s:%d" % (proto, ip, port)})

        def update_service(self, *a):
            pass

        def remove_service(self, *a):
            pass

    zc = Zeroconf()
    try:
        ServiceBrowser(zc, "_%s._tcp.local." % TYPE_DNSSD, _L())
        time.sleep(duree_s)
    finally:
        try:
            zc.close()
        except Exception:
            pass
    return sorted(trouves, key=lambda t: (t["pri"], t["ip"]))


def joignable(base):
    """Le registre répond-il ? `base` est l'ORIGINE ; on sonde la racine de sa Registration API.

    Un `GET`, jamais un `POST` : on vérifie qu'il est là, on ne s'enregistre pas pour le savoir."""
    if not base:
        return False
    import requests
    from . import IS04_VERSION
    try:
        r = requests.get("%s/x-nmos/registration/%s/" % (base.rstrip("/"), IS04_VERSION), timeout=4)
        return r.status_code < 500
    except Exception:
        return False


def resoudre():
    """(url, origine) du registre à utiliser. origine = 'réglage' | 'découverte' | None."""
    from app.database import db_get_setting
    explicite = (db_get_setting("nmos_registry_url", "") or "").strip()
    if explicite:
        return explicite.rstrip("/"), "réglage"
    if not actif():
        return None, None
    for r in decouvrir():
        if joignable(r["url"]):
            return r["url"], "découverte"
        log.info("nmos/découverte : %s annoncé mais injoignable — on passe au suivant", r["url"])
    return None, None


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Boucle
# ══════════════════════════════════════════════════════════════════════════════════════════════

def etat():
    return dict(_courant, actif=actif())


def _boucle():
    from app.database import db_add_alert
    while _running:
        try:
            from app.database import db_get_setting
            explicite = (db_get_setting("nmos_registry_url", "") or "").strip()
            if explicite:
                # Règle 1 : on ne contredit pas une décision humaine. On ne cherche même pas.
                _courant.update({"url": explicite.rstrip("/"), "origine": "réglage", "pri": None})
            elif actif():
                from . import _state, start
                actuel = _state.get("registry_url")
                if not actuel or not joignable(actuel):
                    url, origine = resoudre()
                    if url and url != actuel:
                        log.info("nmos/découverte : registre retenu %s (%s)", url, origine)
                        db_add_alert("alert.nmos.registre_decouvert", "info", kind="nmos",
                                     params={"url": url})
                        _courant.update({"url": url, "origine": origine})
                        start(url)
                    elif not url and actuel:
                        # On avait un registre, il ne répond plus, et on n'en trouve pas d'autre.
                        # On le DIT : disparaître d'un registre sans un mot est exactement le
                        # genre de panne qu'on ne remarque qu'au moment de s'en servir.
                        log.warning("nmos/découverte : registre %s injoignable, aucun autre "
                                    "annoncé sur le réseau", actuel)
                        db_add_alert("alert.nmos.registre_perdu", "warning", kind="nmos",
                                     params={"url": actuel})
                        _courant.update({"url": None, "origine": None})
        except Exception as e:
            log.warning("nmos/découverte : tour ignoré (%s)", e)
        # Cadence choisie sur l'ÉTAT, pas sur une constante : sans registre on cherche vite.
        try:
            from . import _state as _s
            attente = PERIODE_S if _s.get("registry_url") else PERIODE_ACQUISITION_S
        except Exception:
            attente = PERIODE_ACQUISITION_S
        for _ in range(int(attente)):
            if not _running:
                return
            time.sleep(1)


def demarrer():
    global _thread, _running
    if _running:
        return
    _running = True
    _thread = threading.Thread(target=_boucle, daemon=True, name="nmos-decouverte")
    _thread.start()
    log.info("nmos/découverte : boucle démarrée (actif=%s, période %ds)", actif(), int(PERIODE_S))


def arreter():
    global _running
    _running = False
