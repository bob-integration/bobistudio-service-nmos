"""IS-07 — le tally publié en NMOS (Event & Tally Specification, v1.0.1).

Notre tally circule aujourd'hui en **TSL 5.0 UMD**, qui est ce que parlent les vrais pupitres.
IS-07, c'est ce que parlerait un contrôleur NMOS. Les deux ne s'opposent pas : le même état est
publié sur deux transports, pour deux publics.

═══ Ce qui est publié, et comment il est adressé ═════════════════════════════════════════════

Notre tally est adressé **par FLUX** : la table de correspondance TSL associe
`(connexion, source_shm) → tsl_index`, et l'état vit sous `(tsl_index, niveau)`. On publie donc
**une Source IS-07 par (flux, niveau)** — ce qui s'aligne exactement sur les flux MXL déjà exposés
en BCP-007-03, sans inventer une seconde façon de désigner les mêmes signaux.

Les niveaux LH / RH / TT viennent du modèle TSL. On les publie **tels quels**, dans le libellé :
les traduire en « program » et « preview » serait une convention de site, et la présumer ferait
mentir l'étiquette chez qui ne l'applique pas.

═══ Le type d'événement : une ÉNUMÉRATION, pas un booléen ════════════════════════════════════

Notre tally vaut `off`, `red`, `green` ou `amber`. IS-07 prévoit exactement ce cas :
« Enum: `{base}/enum/{Name}` ». On publie donc `string/enum/Tally`.

Le réduire à un booléen aurait demandé de décider quelle couleur signifie « à l'antenne » — une
décision d'exploitation qui n'appartient pas à ce module, et qui aurait effacé l'ambre et le vert.

═══ Ce que ce module NE fait PAS encore ══════════════════════════════════════════════════════

**Il ne publie pas de Sender.** Un Sender IS-07 annonce un transport (WebSocket ou MQTT) sur
lequel les états sont POUSSÉS ; nous ne servons pas encore ce transport. Annoncer un Sender sans
lui, ce serait promettre un abonnement qui n'arriverait jamais — exactement le genre de panne
muette qu'on refuse ailleurs. On publie donc les **Sources et les Flows** en IS-04, plus l'Events
API en REST : un contrôleur lit l'état courant par interrogation. Le transport WebSocket est la
suite, et c'est lui qui apportera la notification.
"""

import logging
import time

from flask import jsonify

log = logging.getLogger(__name__)

VERSION = "v1.0"
BASE = "/x-nmos/events/" + VERSION

# IS-07 § Event types : « Enum: {base}/enum/{Name} ».
TYPE_EVENEMENT = "string/enum/Tally"
VALEURS = ("off", "red", "green", "amber")

# Niveaux TSL, tels que le service les range (`tally_base`, +1, +2).
NIVEAUX = ((0, "LH"), (1, "RH"), (2, "TT"))


def actif():
    """Réglage `nmos_is07` — FERMÉ par défaut, comme toute surface externe."""
    from . import _setting
    return str(_setting("nmos_is07", "0")).strip().lower() in ("1", "true", "on", "yes")


def _sid(shm, niveau):
    """Identité d'une Source IS-07. Dérivée du FLUX et du niveau — donc stable tant que le flux
    porte le même nom, et indépendante de l'index TSL, qui est une adresse de pupitre et peut être
    réattribué par une simple modification de table."""
    from . import _stable_uuid
    return _stable_uuid("is07:source:%s:%d" % (shm, niveau))


def _fid(shm, niveau):
    from . import _stable_uuid
    return _stable_uuid("is07:flow:%s:%d" % (shm, niveau))


def _sources():
    """[(shm, tsl_index, niveau, nom_niveau)] — un par (flux tallyé, niveau)."""
    try:
        from app.database import db_get_tsl_mappings_all
        mappings = db_get_tsl_mappings_all() or []
    except Exception as e:
        log.warning("nmos/is07 : table de correspondance TSL illisible (%s)", e)
        return []
    vus, out = set(), []
    for m in mappings:
        shm = m["source_shm"]
        idx = m["tsl_index"]
        # Un même flux peut être mappé sur PLUSIEURS connexions TSL (deux pupitres). Le tally reste
        # le même signal : on ne le publie qu'une fois, sinon un contrôleur verrait des doublons
        # qui changent ensemble sans qu'il puisse savoir lequel fait foi.
        if shm in vus:
            continue
        vus.add(shm)
        for niveau, nom in NIVEAUX:
            out.append((shm, idx, niveau, nom))
    return out


def _valeur(tsl_index, niveau):
    try:
        from services import tsl
        return tsl.get_tally_level(tsl_index, niveau)
    except Exception:
        return "off"


def _ts():
    """Horodatage IS-07 « secondes:nanosecondes ». Même forme que les versions de ressource."""
    t = time.time()
    return "%d:%09d" % (int(t), int((t - int(t)) * 1e9))


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Ressources IS-04
# ══════════════════════════════════════════════════════════════════════════════════════════════

def ressources(device_id, version):
    """{"sources": [...], "flows": [...]} à verser au modèle IS-04. Pas de Sender : cf. l'entête."""
    if not actif():
        return {"sources": [], "flows": []}
    srcs, flows = [], []
    for shm, _idx, niveau, nom in _sources():
        sid, fid = _sid(shm, niveau), _fid(shm, niveau)
        lbl = "Tally %s — %s" % (nom, shm)
        srcs.append({
            "id": sid, "version": version, "label": lbl,
            "description": "Tally %s du flux %s (IS-07)" % (nom, shm),
            "tags": {"urn:x-mxl:shm": [shm], "urn:x-mxl:tally-level": [nom]},
            "device_id": device_id, "parents": [],
            "format": "urn:x-nmos:format:data",
            "caps": {}, "clock_name": None,
            "event_type": TYPE_EVENEMENT,
        })
        flows.append({
            "id": fid, "version": version, "label": lbl,
            "description": "Flow d'événements tally (IS-07)",
            "tags": {"urn:x-mxl:shm": [shm]},
            "device_id": device_id, "source_id": sid, "parents": [],
            "format": "urn:x-nmos:format:data",
            "media_type": "application/json",
            "event_type": TYPE_EVENEMENT,
        })
    return {"sources": srcs, "flows": flows}


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Events API (REST)
# ══════════════════════════════════════════════════════════════════════════════════════════════

def _par_id():
    return {_sid(shm, niveau): (shm, idx, niveau, nom)
            for shm, idx, niveau, nom in _sources()}


def etat_source(sid):
    """Message STATE d'une Source, dans la forme exacte d'IS-07 § Message types."""
    cible = _par_id().get(sid)
    if not cible:
        return None
    shm, idx, niveau, _nom = cible
    horo = _ts()
    return {
        "identity": {"source_id": sid, "flow_id": _fid(shm, niveau)},
        "event_type": TYPE_EVENEMENT,
        "timing": {"creation_timestamp": horo, "origin_timestamp": horo},
        "payload": {"value": _valeur(idx, niveau)},
        "message_type": "state",
    }


def enregistrer(bp):
    def _ferme():
        return jsonify({"code": 501, "error": "IS-07 désactivé (réglage `nmos_is07`)",
                        "debug": ""}), 501

    @bp.route("/x-nmos/events/", methods=["GET"])
    def is07_racine():
        return jsonify([VERSION + "/"])

    @bp.route(BASE + "/", methods=["GET"])
    def is07_v_racine():
        if not actif():
            return _ferme()
        return jsonify(["sources/"])

    @bp.route(BASE + "/sources", methods=["GET"])
    @bp.route(BASE + "/sources/", methods=["GET"])
    def is07_sources():
        if not actif():
            return _ferme()
        return jsonify(["%s/" % s for s in _par_id()])

    @bp.route(BASE + "/sources/<sid>", methods=["GET"])
    @bp.route(BASE + "/sources/<sid>/", methods=["GET"])
    def is07_source(sid):
        if not actif():
            return _ferme()
        if sid not in _par_id():
            return jsonify({"code": 404, "error": "source inconnue", "debug": sid}), 404
        return jsonify(["state/", "type/"])

    @bp.route(BASE + "/sources/<sid>/type", methods=["GET"])
    @bp.route(BASE + "/sources/<sid>/type/", methods=["GET"])
    def is07_type(sid):
        if not actif():
            return _ferme()
        if sid not in _par_id():
            return jsonify({"code": 404, "error": "source inconnue", "debug": sid}), 404
        # IS-07 § Event and tally rest api : « the type definition object ».
        return jsonify({"type": TYPE_EVENEMENT, "values": [
            {"label": v, "value": v} for v in VALEURS]})

    @bp.route(BASE + "/sources/<sid>/state", methods=["GET"])
    @bp.route(BASE + "/sources/<sid>/state/", methods=["GET"])
    def is07_state(sid):
        if not actif():
            return _ferme()
        e = etat_source(sid)
        if e is None:
            return jsonify({"code": 404, "error": "source inconnue", "debug": sid}), 404
        return jsonify(e)
