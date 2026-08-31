"""Registre IS-04 embarqué — Registration API + Query API.

Décision 6 du chantier « NMOS dans les conteneurs » : l'orchestrateur héberge un vrai registre.
C'est ce qui permet à un conteneur d'un AUTRE ÉDITEUR d'apparaître dans notre inventaire sans
qu'on l'ait déclaré en base — la promesse du chantier, celle qu'aucune interrogation unicast de
nos propres conteneurs ne peut tenir : on ne peut pas interroger ce dont on ignore l'existence.

═══ Ce que la spec impose (IS-04 v1.3, « Behaviour - Registration ») ═════════════════════════

- `POST /resource` `{type, data}` → **201** à la création, **200** à la mise à jour, en-tête
  `Location` vers la ressource. 400 = erreur permanente du client, 409 = conflit de version.
- `DELETE /resource/<type>s/<id>` → 204. « Where a DELETE is issued against a parent resource,
  all child resources MUST be removed from the registry immediately. »
- `POST /health/nodes/<id>` → battement de cœur. « Nodes SHOULD perform a heartbeat every 5
  seconds by default. » **404 si le Node est inconnu** — c'est CE code qui dit au Node de se
  ré-enregistrer, et le rendre 200 par complaisance laisserait un Node absent se croire présent.
- Ramasse-miettes : « Registration APIs SHOULD use a garbage collection interval of 12 seconds by
  default (triggered just after two failed heartbeats at the default 5 second interval) », et à
  l'expiration « both the Node and all registered sub-resources SHOULD be removed ».

═══ Deux partis pris qui ne se devinent pas ══════════════════════════════════════════════════

**Le stockage est EN MÉMOIRE, volontairement.** Un registre NMOS n'est pas une base de données :
il décrit qui est vivant MAINTENANT. Persister survivrait au redémarrage en ressuscitant des
Nodes peut-être morts, que plus aucun battement ne viendrait démentir — un registre qui ment est
pire qu'un registre vide. Après un redémarrage, les Nodes se ré-enregistrent : c'est le
comportement prévu par la spec (404 sur le battement → ré-enregistrement).

**Nos propres ressources n'y sont pas INJECTÉES — elles s'y ENREGISTRENT.** L'agrégation a été
tranchée le 2026-08-31, et sans code nouveau : `nmos_registry_url` pointé sur nous-mêmes suffit,
le client d'enregistrement existant nous inscrit ici par le protocole, comme n'importe quel Node.
Un chemin d'injection dédié aurait dupliqué un modèle qui change à chaque rebuild, avec la
divergence pour seule perspective. Vérifié : la Query API rend alors 270 senders / 219 receivers,
identiques au Node API.

⚠ Deux conséquences de ce montage, mesurées et corrigées côté client (cf. `_register_all`) : à
notre échelle l'enregistrement dure ~9 s, donc il faut BATTRE pendant ; et un POST étant un
upsert, il faut EFFACER ce qui a disparu du modèle, sinon le registre garde des ressources mortes
jusqu'à l'expiration du Node entier.
"""

import json
import logging
import threading
import time

from flask import jsonify, request

log = logging.getLogger(__name__)

VERSION = "v1.3"
BASE_REG = "/x-nmos/registration/" + VERSION
BASE_QRY = "/x-nmos/query/" + VERSION

# Ordre PARENT → ENFANT. Sert au ramasse-miettes en cascade et à l'ordre de suppression.
TYPES = ("node", "device", "source", "flow", "sender", "receiver")
_PLURIEL = {t: t + "s" for t in TYPES}
_SINGULIER = {v: k for k, v in _PLURIEL.items()}

GC_DEFAUT_S = 12          # valeur recommandée par IS-04

_verrou = threading.RLock()
# type → {id → {"data": dict}}. On ne range NI le node propriétaire NI un horodatage :
# le rattachement sert au REFUS d'une ressource orpheline à l'enregistrement, et l'expiration se
# décide sur `_sante` seul. Les stocker aurait donné deux sources pour la même information, dont
# une jamais relue — celle qui se met à mentir en silence.
_res = {t: {} for t in TYPES}
_sante = {}               # node_id → monotonic du dernier battement
_reaper = None


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Réglages
# ══════════════════════════════════════════════════════════════════════════════════════════════

def actif():
    """Le registre est-il servi ? NON par défaut : c'est une surface EXTERNE de plus, et on
    n'en ouvre pas une sans que l'exploitant l'ait demandée."""
    from . import _setting
    return str(_setting("nmos_registre", "0")).strip().lower() in ("1", "true", "on", "yes")


def _gc_s():
    from . import _setting
    try:
        v = int(_setting("nmos_registre_gc_s", GC_DEFAUT_S))
    except (TypeError, ValueError):
        return GC_DEFAUT_S
    # Un GC plus court que deux battements ferait expirer des Nodes parfaitement vivants.
    return max(v, 4)


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Modèle
# ══════════════════════════════════════════════════════════════════════════════════════════════

def _node_proprietaire(type_, data):
    """Node auquel rattacher la ressource, pour l'expiration en cascade. None si irrésolu.

    On remonte la chaîne DANS le registre — surtout pas en faisant confiance à un `node_id` que
    la ressource porterait elle-même sans que le Node soit enregistré : une ressource orpheline
    n'expirerait jamais."""
    if type_ == "node":
        return data.get("id")
    if type_ == "device":
        return data.get("node_id")
    did = data.get("device_id")
    dev = _res["device"].get(did)
    return dev["data"].get("node_id") if dev else None


def _valider(type_, data):
    """Contrôles MINIMAUX — un registre n'est pas un validateur de schéma, et refuser trop
    fermement rendrait notre registre incompatible avec des implémentations valides mais
    différentes. On exige donc l'indispensable : de quoi identifier et rattacher."""
    if type_ not in TYPES:
        return "type inconnu : %r" % type_
    if not isinstance(data, dict) or not data.get("id"):
        return "data.id absent"
    if type_ != "node" and not (data.get("node_id") or data.get("device_id")):
        return "ressource sans parent (node_id ou device_id)"
    return None


def _enfants(type_, rid):
    """[(type, id)] des ressources dont `rid` est le parent DIRECT."""
    out = []
    if type_ == "node":
        out += [("device", i) for i, r in _res["device"].items()
                if r["data"].get("node_id") == rid]
    if type_ == "device":
        for t in ("source", "flow", "sender", "receiver"):
            out += [(t, i) for i, r in _res[t].items() if r["data"].get("device_id") == rid]
    return out


def _supprimer(type_, rid):
    """Supprime `rid` ET sa descendance. « all child resources MUST be removed immediately »."""
    for t, i in _enfants(type_, rid):
        _supprimer(t, i)
    _res[type_].pop(rid, None)
    if type_ == "node":
        _sante.pop(rid, None)


def _expirer():
    """Nodes dont le dernier battement dépasse le délai de ramasse-miettes. Renvoie les ids."""
    limite = _gc_s()
    maintenant = time.monotonic()
    morts = [nid for nid, vu in _sante.items() if maintenant - vu > limite]
    for nid in morts:
        _supprimer("node", nid)
    return morts


def _boucle_reaper():
    while True:
        time.sleep(2)
        try:
            with _verrou:
                morts = _expirer()
            for nid in morts:
                log.info("nmos/registre: Node %s expiré (pas de battement depuis %ds) — "
                         "lui et ses sous-ressources sont retirés", nid, _gc_s())
                try:
                    from app.database import db_add_alert
                    db_add_alert("Node NMOS %s expiré du registre (battement perdu)" % nid,
                                 "warning", kind="net")
                except Exception:
                    pass
        except Exception as e:                                   # pragma: no cover
            log.warning("nmos/registre: passe de ramasse-miettes échouée : %s", e)


def _assurer_reaper():
    """Démarre le ramasse-miettes au PREMIER enregistrement, pas au boot : avant ça il n'a rien
    à faire, et un thread qui tourne pour rien sur toutes les installations est un coût gratuit."""
    global _reaper
    if _reaper is None or not _reaper.is_alive():
        _reaper = threading.Thread(target=_boucle_reaper, daemon=True,
                                   name="nmos-registre-gc")
        _reaper.start()


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Lecture interne (pour l'orchestrateur, pas pour le réseau)
# ══════════════════════════════════════════════════════════════════════════════════════════════

def inventaire():
    """Résumé de ce que le registre contient — destiné à l'UI et aux diagnostics."""
    with _verrou:
        maintenant = time.monotonic()
        return {
            "actif": actif(),
            "gc_s": _gc_s(),
            "compte": {t: len(_res[t]) for t in TYPES},
            "nodes": [{
                "id": nid,
                "label": r["data"].get("label") or "",
                "href": r["data"].get("href") or "",
                "depuis_battement_s": round(maintenant - _sante.get(nid, maintenant), 1),
            } for nid, r in _res["node"].items()],
        }


def ressources(type_):
    with _verrou:
        return [r["data"] for r in _res.get(type_, {}).values()]


def vider():
    """Purge complète — réservé aux bancs."""
    with _verrou:
        for t in TYPES:
            _res[t].clear()
        _sante.clear()


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Routes
# ══════════════════════════════════════════════════════════════════════════════════════════════

def enregistrer(bp):
    def _hors_service():
        return jsonify({"code": 501, "error": "registre IS-04 désactivé "
                        "(réglage `nmos_registre`)", "debug": ""}), 501

    # ── Registration API ──────────────────────────────────────────────────────────────────
    @bp.route("/x-nmos/registration/", methods=["GET"])
    def reg_racine():
        return jsonify([VERSION + "/"])

    @bp.route(BASE_REG + "/", methods=["GET"])
    def reg_v_racine():
        return jsonify(["resource/", "health/"])

    @bp.route(BASE_REG + "/resource", methods=["POST"])
    @bp.route(BASE_REG + "/resource/", methods=["POST"])
    def reg_resource():
        if not actif():
            return _hors_service()
        corps = request.get_json(silent=True)
        if not isinstance(corps, dict):
            return jsonify({"code": 400, "error": "corps JSON attendu", "debug": ""}), 400
        type_ = str(corps.get("type") or "").strip().lower()
        data = corps.get("data")
        err = _valider(type_, data)
        if err:
            return jsonify({"code": 400, "error": err, "debug": json.dumps(corps)[:200]}), 400

        with _verrou:
            rid = data["id"]
            node = _node_proprietaire(type_, data)
            if type_ != "node" and node is None:
                # Rattacher une ressource à un Node absent, c'est créer un orphelin que le
                # ramasse-miettes ne pourra jamais reprendre. Mieux vaut le refuser en le disant.
                return jsonify({"code": 400,
                                "error": "parent inconnu du registre : enregistrer le Node et le "
                                         "Device avant leurs ressources",
                                "debug": rid}), 400
            neuf = rid not in _res[type_]
            _res[type_][rid] = {"data": data}
            if type_ == "node":
                _sante[rid] = time.monotonic()
            _assurer_reaper()

        rep = jsonify(data)
        rep.headers["Location"] = "%s/resource/%s/%s" % (BASE_REG, _PLURIEL[type_], rid)
        return rep, (201 if neuf else 200)

    @bp.route(BASE_REG + "/resource/<pluriel>/<rid>", methods=["DELETE"])
    def reg_supprimer(pluriel, rid):
        if not actif():
            return _hors_service()
        type_ = _SINGULIER.get(pluriel)
        if not type_:
            return jsonify({"code": 404, "error": "type inconnu", "debug": pluriel}), 404
        with _verrou:
            if rid not in _res[type_]:
                return jsonify({"code": 404, "error": "ressource inconnue", "debug": rid}), 404
            _supprimer(type_, rid)
        return ("", 204)

    @bp.route(BASE_REG + "/resource/<pluriel>/<rid>", methods=["GET"])
    def reg_lire(pluriel, rid):
        type_ = _SINGULIER.get(pluriel)
        with _verrou:
            r = _res.get(type_, {}).get(rid) if type_ else None
        if not r:
            return jsonify({"code": 404, "error": "ressource inconnue", "debug": rid}), 404
        return jsonify(r["data"])

    @bp.route(BASE_REG + "/health/nodes/<nid>", methods=["POST"])
    def reg_battement(nid):
        if not actif():
            return _hors_service()
        with _verrou:
            if nid not in _res["node"]:
                # 404 VOULU : c'est ce code qui dit au Node de se ré-enregistrer. Répondre 200
                # par complaisance laisserait un Node absent du registre se croire présent.
                return jsonify({"code": 404, "error": "Node non enregistré", "debug": nid}), 404
            _sante[nid] = time.monotonic()
        return jsonify({"health": str(int(time.time()))})

    @bp.route(BASE_REG + "/health/nodes/<nid>", methods=["GET"])
    def reg_sante(nid):
        with _verrou:
            if nid not in _res["node"]:
                return jsonify({"code": 404, "error": "Node non enregistré", "debug": nid}), 404
            reste = max(0, _gc_s() - (time.monotonic() - _sante.get(nid, 0)))
        return jsonify({"health": str(int(time.time())), "expire_dans_s": round(reste, 1)})

    # ── Query API (lecture) ───────────────────────────────────────────────────────────────
    @bp.route("/x-nmos/query/", methods=["GET"])
    def qry_racine():
        return jsonify([VERSION + "/"])

    @bp.route(BASE_QRY + "/", methods=["GET"])
    def qry_v_racine():
        return jsonify([_PLURIEL[t] + "/" for t in TYPES] + ["subscriptions/"])

    def _collection(type_):
        # Filtres RQL basiques de la Query API : `?label=…`, `?id=…`. Le sous-ensemble suffit aux
        # contrôleurs courants ; ce qu'on ne sait pas filtrer est IGNORÉ, jamais appliqué de
        # travers — un filtre mal compris qui rend trop peu est indétectable côté client.
        with _verrou:
            items = [r["data"] for r in _res[type_].values()]
        for cle in ("id", "label"):
            v = request.args.get(cle)
            if v:
                items = [x for x in items if str(x.get(cle, "")) == v]
        return jsonify(items)

    for _t in TYPES:
        def _faire(t):
            def _liste():
                return _collection(t)
            _liste.__name__ = "qry_liste_" + t
            return _liste

        def _faire_un(t):
            def _un(rid):
                with _verrou:
                    r = _res[t].get(rid)
                if not r:
                    return jsonify({"code": 404, "error": "ressource inconnue",
                                    "debug": rid}), 404
                return jsonify(r["data"])
            _un.__name__ = "qry_un_" + t
            return _un

        bp.add_url_rule(BASE_QRY + "/" + _PLURIEL[_t], view_func=_faire(_t), methods=["GET"])
        bp.add_url_rule(BASE_QRY + "/" + _PLURIEL[_t] + "/<rid>", view_func=_faire_un(_t),
                        methods=["GET"])

    @bp.route(BASE_QRY + "/subscriptions", methods=["GET"])
    def qry_subscriptions():
        # Les abonnements WebSocket de la Query API ne sont pas implémentés. On rend une liste
        # VIDE (un contrôleur retombe alors sur l'interrogation périodique) plutôt qu'un 501 qui
        # ferait échouer sa découverte entière pour une capacité optionnelle.
        return jsonify([])

    # ── Vue interne ───────────────────────────────────────────────────────────────────────
    from app.auth import require_login

    @bp.route("/api/nmos/registre", methods=["GET"])
    @require_login
    def api_registre():
        """Vue INTERNE (pas du NMOS) : ce que le registre contient, pour l'UI et le diagnostic."""
        return jsonify(inventaire())
