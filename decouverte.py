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

═══ ÉCOUTER, PAS SONDER ══════════════════════════════════════════════════════════════════════

Première version : un `Zeroconf()` neuf toutes les N secondes, une écoute de 4 s, puis fermeture.
Ça ne marche pas, et la suite AMWA l'a montré — nous détections bien la mort d'un registre, nous
l'abandonnions correctement, et nous ne trouvions **jamais** son remplaçant. Deux raisons :

  · une annonce qui apparaît ENTRE deux sondages est simplement manquée ;
  · chaque sondage repart d'un cache mDNS FROID et dépend de la réponse de l'annonceur dans la
    fenêtre. Un service qui a déjà fait son annonce spontanée peut ne pas se re-signaler à temps.

D'où un `ServiceBrowser` PERMANENT, vivant pour la durée du processus, qui tient à jour la carte
des registres annoncés. La réaction devient ÉVÉNEMENTIELLE : une annonce qui apparaît nous réveille,
une annonce qui disparaît aussi. Il n'y a plus de cadence d'acquisition à arbitrer — le dilemme
« chercher souvent / ne pas basculer nerveusement » se dissout, parce que ce ne sont plus les mêmes
mécanismes : on ÉCOUTE en permanence, et on ne BASCULE que sur un événement réel.

La boucle périodique subsiste, réduite à ce qu'elle sait faire : vérifier que le registre courant
répond encore (une annonce mDNS peut survivre à un service mort).
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

# Carte VIVANTE des registres annoncés : nom de service → {ip, port, pri, url}. Tenue à jour par
# l'écouteur, jamais reconstruite — c'est tout l'intérêt.
_annonces = {}
_annonces_lock = threading.Lock()
_zc = None
_browser = None


def _retenir(name, info):
    """Range une annonce dans la carte, ou l'écarte en DISANT pourquoi."""
    from . import IS04_VERSION
    if not info or not info.addresses:
        return
    txt = {(k.decode() if isinstance(k, bytes) else k):
           ((v or b"").decode() if isinstance(v, bytes) else v)
           for k, v in (info.properties or {}).items()}
    try:
        pri = int(txt.get("pri", PRI_DEVELOPPEMENT))
    except (TypeError, ValueError):
        pri = PRI_DEVELOPPEMENT
    ip, port = socket.inet_ntoa(info.addresses[0]), int(info.port or 0)
    if ip in _nos_adresses() and port == NOTRE_PORT:
        log.debug("nmos/découverte : %s:%d ignoré, c'est NOTRE registre", ip, port)
        return
    if pri >= PRI_DEVELOPPEMENT:
        log.info("nmos/découverte : registre %s:%d écarté — pri=%d, réservé au développement "
                 "par IS-04", ip, port, pri)
        return
    ver = txt.get("api_ver", "")
    if ver and IS04_VERSION not in [v.strip() for v in ver.split(",")]:
        log.info("nmos/découverte : registre %s:%d écarté — annonce api_ver=%r, nous parlons %s",
                 ip, port, ver, IS04_VERSION)
        return
    proto = txt.get("api_proto", "http")
    with _annonces_lock:
        nouveau = name not in _annonces
        _annonces[name] = {"nom": name.split(".")[0], "ip": ip, "port": port, "pri": pri,
                           "url": "%s://%s:%d" % (proto, ip, port)}
    if nouveau:
        log.info("nmos/découverte : registre annoncé — %s://%s:%d (pri=%d)", proto, ip, port, pri)
        _reveiller()


class _Ecouteur:
    """Écouteur mDNS permanent. Les trois méthodes sont le contrat de zeroconf."""

    def add_service(self, zc, type_, name):
        try:
            _retenir(name, zc.get_service_info(type_, name, timeout=3000))
        except Exception as e:
            log.debug("nmos/découverte : annonce %s illisible (%s)", name, e)

    def update_service(self, zc, type_, name):
        self.add_service(zc, type_, name)

    def remove_service(self, zc, type_, name):
        with _annonces_lock:
            parti = _annonces.pop(name, None)
        if not parti:
            return
        log.info("nmos/découverte : registre RETIRÉ du réseau — %s", parti["url"])
        # ★ C'est l'événement qui compte. Un registre qui retire son annonce s'en va : ne pas
        # réagir ici, c'est rester accroché à un mort jusqu'au prochain échec de battement.
        from . import _state
        if _state.get("registry_url") == parti["url"]:
            _reveiller(bascule_depuis=parti["url"])


def _reveiller(bascule_depuis=None):
    """Réévalue MAINTENANT, sans attendre le tour de boucle. Toujours dans un fil : `start()`
    arrête la boucle d'enregistrement, et un écouteur zeroconf ne doit jamais bloquer."""
    def _go():
        try:
            from . import _state, start
            actuel = _state.get("registry_url")
            if bascule_depuis and actuel and actuel != bascule_depuis:
                return                            # quelqu'un a déjà bougé
            if actuel and not bascule_depuis:
                return                            # on en a un qui va bien : on ne touche à rien
            url, origine = resoudre()
            if url and url != actuel:
                log.info("nmos/découverte : registre retenu %s (%s)", url, origine)
                start(url)
        except Exception as e:
            log.warning("nmos/découverte : réveil sans effet (%s)", e)
    threading.Thread(target=_go, daemon=True, name="nmos-decouverte-reveil").start()


def _demarrer_ecoute():
    """Ouvre l'écouteur permanent. Idempotent."""
    global _zc, _browser
    if _browser is not None:
        return True
    try:
        from zeroconf import ServiceBrowser, Zeroconf
    except Exception as e:
        log.info("nmos/découverte : zeroconf indisponible (%s)", e)
        return False
    _zc = Zeroconf()
    _browser = ServiceBrowser(_zc, "_%s._tcp.local." % TYPE_DNSSD, _Ecouteur())
    log.info("nmos/découverte : écoute mDNS permanente de _%s._tcp", TYPE_DNSSD)
    return True


def _arreter_ecoute():
    global _zc, _browser
    try:
        if _browser is not None:
            _browser.cancel()
    except Exception:
        pass
    try:
        if _zc is not None:
            _zc.close()
    except Exception:
        pass
    _browser = _zc = None


def decouvrir(duree_s=DUREE_BROWSE_S):
    """[{nom, url, pri, ip, port}] des registres CONNUS, triés par priorité croissante.

    Lit la carte tenue par l'écouteur — ce n'est plus un sondage. `duree_s` ne sert qu'au premier
    appel, quand l'écoute vient d'être ouverte et que les annonces n'ont pas encore afflué."""
    _demarrer_ecoute()
    with _annonces_lock:
        out = list(_annonces.values())
    if not out and duree_s:
        # ⚠ REPLI PAR SONDAGE, et il n'est pas décoratif. MESURÉ le 2026-08-31 : un `Zeroconf`
        # permanent, dans le processus serveur, ne recevait AUCUNE annonce venue d'un autre
        # processus — pendant qu'un `Zeroconf` neuf et éphémère, lui, les recevait. La cause
        # n'est pas élucidée (socket multicast tenue trop tôt ? interaction avec l'annonceur ?),
        # et tant qu'elle ne l'est pas, s'en remettre au seul écouteur permanent serait parier
        # sur un mécanisme qu'on a vu échouer en conditions réelles.
        #
        # Les deux chemins ne font donc pas double emploi : l'écouteur donne la RÉACTIVITÉ
        # (une annonce qui apparaît nous réveille), le sondage donne la CERTITUDE au moment où
        # l'on en a besoin — c'est-à-dire quand on n'a rien.
        out = _sonder(duree_s)
    return sorted(out, key=lambda t: (t["pri"], t["ip"]))


def _sonder(duree_s):
    """Sondage PONCTUEL : un `Zeroconf` neuf, une écoute bornée, puis fermeture.

    Alimente la même carte que l'écouteur permanent — les deux chemins ne divergent donc jamais
    sur ce qu'ils retiennent ni sur ce qu'ils écartent."""
    try:
        from zeroconf import ServiceBrowser, Zeroconf
    except Exception:
        return []
    zc = Zeroconf()
    try:
        ServiceBrowser(zc, "_%s._tcp.local." % TYPE_DNSSD, _Ecouteur())
        time.sleep(duree_s)
    except Exception as e:
        log.warning("nmos/découverte : sondage échoué (%s)", e)
    finally:
        try:
            zc.close()
        except Exception:
            pass
    with _annonces_lock:
        return list(_annonces.values())


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
                    # `resoudre()` → `decouvrir()` → sondage si la carte est vide : c'est ici que
                    # le repli reprend la main quand l'écouteur permanent reste muet.
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
        # ⚠ Cette boucle n'est PLUS le moyen de découvrir : les annonces arrivent par l'écouteur.
        # Elle ne sert qu'à un cas que mDNS ne signale pas — un registre dont l'annonce est
        # toujours là mais dont le service ne répond plus. On garde donc les deux cadences : vite
        # tant qu'on n'a pas de registre, lentement quand on en a un qui va bien.
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
    """Ouvre l'écoute mDNS permanente et la boucle de surveillance."""
    global _thread, _running
    if _running:
        return
    _running = True
    if actif():
        _demarrer_ecoute()
        _reveiller()                 # une annonce peut déjà être là : on ne l'attend pas
    _thread = threading.Thread(target=_boucle, daemon=True, name="nmos-decouverte")
    _thread.start()
    log.info("nmos/découverte : démarrée (actif=%s, surveillance toutes les %ds ; les annonces "
             "sont ÉCOUTÉES en continu, pas sondées)", actif(), int(PERIODE_S))


def arreter():
    global _running
    _running = False
    _arreter_ecoute()
