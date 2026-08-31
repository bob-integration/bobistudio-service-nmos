"""L'orchestrateur en CLIENT MS-05-02 — lire et piloter le modèle d'un appareil TIERS (IS-14).

C'est l'autre sens, et c'est là qu'est la promesse du chantier : jusqu'ici nous SERVONS un modèle
de contrôle (`plugins_ncp.py` + `is12.py`/`is14.py`). Ici nous en LISONS un, chez quelqu'un
d'autre. Une media function d'un autre éditeur devient pilotable sans que nous connaissions son
manifeste, son type, ni son implémentation — c'est précisément ce qu'aucun `derive_wiring` ne peut
faire, puisqu'il lit NOTRE `plugin.json`.

═══ IS-14 d'abord, IS-12 ensuite — et pourquoi ═══════════════════════════════════════════════

IS-14 est du REST : aucune dépendance nouvelle, et il donne la lecture ET l'écriture du modèle.
IS-12 est un protocole à état sur WebSocket ; ce qu'il apporte EN PLUS, ce sont les
**notifications** — être prévenu qu'une propriété a changé sans interroger. C'est précieux, mais
ce n'est pas ce qui débloque le pilotage d'un tiers. On commence donc par IS-14.

═══ Découverte ═══════════════════════════════════════════════════════════════════════════════

Un appareil n'expose pas son point de contrôle au hasard : il l'ANNONCE dans le tableau
`controls` de son Device IS-04, avec un `type` normalisé. On y cherche
`urn:x-nmos:control:configuration/v1.0` (IS-14) — exactement ce que nous annonçons nous-mêmes,
donc le chemin de découverte est symétrique.

═══ Ce qui rend ce module éprouvable dès aujourd'hui ═════════════════════════════════════════

**Notre propre serveur IS-14 est un pair conforme.** Le client peut donc être exercé contre notre
orchestrateur, sans attendre un tiers — et c'est le seul moyen honnête de savoir s'il marche
avant de le brancher sur le matériel de quelqu'un d'autre.
"""

import json
import logging

log = logging.getLogger(__name__)

TYPE_IS14 = "urn:x-nmos:control:configuration/v1.0"
TYPE_IS12 = "urn:x-nmos:control:ncp/v1.0"

TIMEOUT_S = 8


class ErreurTiers(Exception):
    """Le pair a répondu, mais mal. Distinguée d'une panne réseau : un appareil qui refuse une
    écriture n'est pas un appareil injoignable, et confondre les deux ferait chercher au mauvais
    endroit."""


def _http(methode, url, corps=None):
    import requests
    try:
        r = requests.request(methode, url, json=corps, timeout=TIMEOUT_S)
    except Exception as e:
        raise ErreurTiers("injoignable : %s" % e)
    if r.status_code >= 400:
        raise ErreurTiers("HTTP %s sur %s : %s" % (r.status_code, url, r.text[:200]))
    try:
        return r.json()
    except Exception:
        return r.text


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Découverte
# ══════════════════════════════════════════════════════════════════════════════════════════════

def point_de_controle(device, type_=TYPE_IS14):
    """`href` du point de contrôle annoncé par un Device IS-04, ou None.

    On cherche le `type` NORMALISÉ, jamais une URL devinée : un appareil qui n'annonce pas son
    point de contrôle n'en a pas, et fabriquer l'adresse à sa place produirait des 404 qu'on
    prendrait pour des pannes."""
    for c in (device or {}).get("controls") or []:
        if c.get("type") == type_ and c.get("href"):
            return c["href"].rstrip("/")
    return None


def points_de_controle_du_node(base_node, type_=TYPE_IS14):
    """[(device_id, href)] pour tous les Devices d'un Node IS-04 joignable à `base_node`."""
    devices = _http("GET", "%s/x-nmos/node/v1.3/devices" % base_node.rstrip("/"))
    out = []
    for d in devices or []:
        h = point_de_controle(d, type_)
        if h:
            out.append((d.get("id"), h))
    return out


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Lecture du modèle
# ══════════════════════════════════════════════════════════════════════════════════════════════

def chemins(base):
    """Les `rolePaths` publiés par le pair. C'est l'index du modèle."""
    return [str(x).rstrip("/") for x in (_http("GET", "%s/rolePaths/" % base) or [])]


def descripteur(base, chemin):
    return _http("GET", "%s/rolePaths/%s/descriptor" % (base, chemin))


def proprietes(base, chemin):
    """Identifiants de propriétés d'un objet, sous la forme publiée par le pair (« 3p1 »)."""
    return [str(x).rstrip("/") for x in (_http("GET", "%s/rolePaths/%s/properties/"
                                               % (base, chemin)) or [])]


def lire(base, chemin, pid):
    """Valeur d'une propriété. Le corps NMOS est un NcMethodResult : la valeur est sous `value`."""
    r = _http("GET", "%s/rolePaths/%s/properties/%s/value" % (base, chemin, pid))
    return r.get("value") if isinstance(r, dict) and "value" in r else r


def ecrire(base, chemin, pid, valeur):
    """Écrit une propriété (PUT). Lève `ErreurTiers` si le pair refuse — un refus DOIT remonter :
    avaler l'erreur ferait croire à une consigne appliquée."""
    r = _http("PUT", "%s/rolePaths/%s/properties/%s/value" % (base, chemin, pid),
              {"value": valeur})
    _verifier(r, "écriture %s sur %s" % (pid, chemin))
    return r


def invoquer(base, chemin, mid, arguments=None):
    """Invoque une méthode (PATCH). `mid` sous la forme publiée par le pair (« 3m1 »)."""
    r = _http("PATCH", "%s/rolePaths/%s/methods/%s" % (base, chemin, mid),
              {"arguments": arguments or {}})
    _verifier(r, "invocation %s sur %s" % (mid, chemin))
    return r


def _verifier(r, quoi):
    """Un NcMethodResult porte son verdict DANS le corps : `status` 200 = OK. Un 200 HTTP avec un
    `status` d'erreur dedans est le piège classique de MS-05-02 — le transport a réussi, la
    commande a échoué. Ne regarder que le code HTTP ferait passer un refus pour un succès."""
    if isinstance(r, dict) and r.get("status") not in (None, 200):
        raise ErreurTiers("%s refusée par le pair : status=%s %s"
                          % (quoi, r.get("status"), r.get("errorMessage") or ""))


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Inventaire lisible
# ══════════════════════════════════════════════════════════════════════════════════════════════

def inventaire(base, max_objets=200):
    """Parcourt le modèle d'un pair et rend une vue lisible : par chemin de rôle, sa classe et ses
    propriétés avec leur valeur. Destiné au diagnostic et à l'UI — pas au pilotage."""
    out = []
    for chemin in chemins(base)[:max_objets]:
        try:
            d = descripteur(base, chemin) or {}
            d = d.get("value", d)
            props = {}
            for pid in proprietes(base, chemin):
                try:
                    props[pid] = lire(base, chemin, pid)
                except ErreurTiers as e:
                    props[pid] = "<%s>" % e
            out.append({"rolePath": chemin,
                        "classId": d.get("classId"), "name": d.get("name"),
                        "proprietes": props})
        except ErreurTiers as e:
            out.append({"rolePath": chemin, "erreur": str(e)})
    return out


def resume(base):
    """Une ligne par objet — pour un journal ou un rapport."""
    return "\n".join(
        "%-40s %-22s %s" % (o.get("rolePath"), o.get("name") or "?",
                            json.dumps(o.get("proprietes") or {}, ensure_ascii=False)[:110])
        for o in inventaire(base))
