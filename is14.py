# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""AMWA IS-14 « NMOS Device Configuration » — le même modèle MS-05-02, en HTTP.

IS-14 est le frère REST d'IS-12 : même modèle d'appareil, mêmes objets, mêmes identifiants de
propriétés et de méthodes ; seul le transport change. Un contrôleur qui ne veut pas tenir une
WebSocket ouverte lit ici les mêmes statuts BCP-008, et y trouve en plus la **sauvegarde et la
restauration** du modèle (endpoint `bulkProperties`).

Ce fichier n'est qu'une façade : il traduit une URL en (objet, propriété|méthode) et délègue à
`modele.appareil()`. Toute la logique — dispatch, contrôle de lecture seule, sauvegarde et
restauration — vit dans `ncp.py`, partagée avec IS-12. C'est délibéré : deux protocoles qui
publient le même modèle ne doivent pas pouvoir en publier deux versions.

Contrairement à IS-12, pas de serveur ni de port dédiés — c'est du HTTP ordinaire, servi par le
Flask de l'orchestrateur, comme IS-04 et IS-05.

Adressage : un objet est désigné par son NcRolePath, c'est-à-dire la suite de ses rôles depuis la
racine, jointe par des points (`root.receivers.receiver_<uuid>`). Un identifiant de propriété
s'écrit `{niveau}p{index}` (`1p6` = userLabel), une méthode `{niveau}m{index}` (`1m1` = Get).

SÉCURITÉ : non authentifié, comme le reste du provider NMOS (réseau de contrôle interne). Les
seules écritures que le modèle accepte sont les réglages que la BCP-008 impose de rendre
modifiables ; le reste répond `405 Readonly` — y compris à travers une restauration.
"""
import logging
import re

from flask import jsonify, request

from . import modele, ncp

log = logging.getLogger(__name__)

VERSION      = "v1.0"
BASE         = "/x-nmos/configuration"
RACINE       = "{}/{}".format(BASE, VERSION)
TYPE_CONTROL = "urn:x-nmos:control:configuration/v1.0"

_RE_PROP   = re.compile(r"^(\d+)p(\d+)$")
_RE_METHOD = re.compile(r"^(\d+)m(\d+)$")


def actif():
    from app.database import db_get_setting
    return bool(db_get_setting("nmos_is14_enabled", False))


def href(host=None):
    """URL annoncée dans le tableau `controls` du Device IS-04."""
    from . import _get_host_address
    return "http://{}:5000{}".format(host or _get_host_address(), RACINE)


def status_dict():
    rx, tx, ignores = modele.nb_monitors()
    return {
        "actif": actif(),
        "href": href() if actif() else None,
        "monitors_rx": rx, "monitors_tx": tx, "sans_monitor": ignores,
        "role_paths": len(modele.appareil().chemins()) if modele.appareil() else 0,
    }


def start():
    """Acquiert le modèle partagé. Aucun serveur à monter : les routes sont sur le Flask principal
    et se contentent de refuser tant que le réglage est éteint."""
    modele.acquerir("is14")
    log.info("IS-14 : configuration API servie sur %s", href())
    return True


def stop():
    modele.liberer("is14")


# ═════════════════════════════════════════════════════════════════════════════════════════════
# Traduction URL → modèle
# ═════════════════════════════════════════════════════════════════════════════════════════════

def _err(status_http, status_ms05, message):
    """NcMethodResultError avec le code HTTP qui convient (cf. ConfigurationAPI.raml)."""
    return jsonify(ncp.erreur(status_ms05, message)), status_http


def _appareil_ou_erreur():
    if not actif():
        return None, _err(404, ncp.NOT_READY, "IS-14 n'est pas activé sur cet appareil")
    app = modele.appareil()
    if app is None:
        return None, _err(404, ncp.NOT_READY, "modèle d'appareil non initialisé")
    return app, None


def _objet(app, rolepath):
    """Objet visé par un rolePath d'URL (rôles joints par des points), ou None."""
    return app.par_chemin([p for p in rolepath.split(".") if p != ""])


def _id_propriete(txt):
    m = _RE_PROP.match(txt or "")
    return (int(m.group(1)), int(m.group(2))) if m else None


def _id_methode(txt):
    m = _RE_METHOD.match(txt or "")
    return (int(m.group(1)), int(m.group(2))) if m else None


def _resoudre(rolepath, pid_txt=None, mid_txt=None):
    """(app, obj, cle, err) — `err` est None en cas de succès, sinon une réponse Flask prête.

    L'existence de ce que DÉSIGNE l'URL est vérifiée AVANT d'invoquer quoi que ce soit : un
    chemin, une propriété ou une méthode inconnus sont des 404 HTTP. Le résultat de l'invocation,
    lui, part toujours en 200 avec son `status` MS-05-02 dans le corps — un refus d'écriture sur
    une propriété en lecture seule est une réponse valide, pas une erreur de transport."""
    app, err = _appareil_ou_erreur()
    if err:
        return None, None, None, err
    obj = _objet(app, rolepath)
    if obj is None:
        return None, None, None, _err(404, ncp.BAD_OID,
                                      "chemin de rôles inconnu : %s" % rolepath)
    if pid_txt is not None:
        cle = _id_propriete(pid_txt)
        if cle is None:
            return None, None, None, _err(
                400, ncp.PARAMETER_ERROR,
                "identifiant de propriété mal formé : %s (attendu 1p6)" % pid_txt)
        if cle not in obj._props:
            return None, None, None, _err(
                404, ncp.PROPERTY_NOT_IMPLEMENTED,
                "propriété %s absente de %s" % (pid_txt, obj.__class__.__name__))
        return app, obj, cle, None
    if mid_txt is not None:
        cle = _id_methode(mid_txt)
        if cle is None:
            return None, None, None, _err(
                400, ncp.PARAMETER_ERROR,
                "identifiant de méthode mal formé : %s (attendu 1m1)" % mid_txt)
        if cle not in obj._methodes:
            return None, None, None, _err(
                404, ncp.METHOD_NOT_IMPLEMENTED,
                "méthode %s absente de %s" % (mid_txt, obj.__class__.__name__))
        return app, obj, cle, None
    return app, obj, None, None


def _bool_param(nom, defaut=True):
    """Paramètre de requête booléen. Absent = `defaut` (la spec impose `true` pour les deux
    paramètres de `bulkProperties` quand ils ne sont pas fournis)."""
    v = request.args.get(nom)
    if v is None:
        return defaut
    return str(v).strip().lower() not in ("0", "false", "no", "off")


# ═════════════════════════════════════════════════════════════════════════════════════════════
# Routes
# ═════════════════════════════════════════════════════════════════════════════════════════════

def enregistrer(bp):
    """Greffe les endpoints IS-14 sur le blueprint NMOS (celui qui porte déjà IS-04 et IS-05)."""

    @bp.route(BASE + "/", methods=["GET"])
    def is14_base():
        if not actif():
            return _err(404, ncp.NOT_READY, "IS-14 n'est pas activé sur cet appareil")
        return jsonify(["{}/".format(VERSION)])

    @bp.route(RACINE + "/", methods=["GET"])
    def is14_racine():
        if not actif():
            return _err(404, ncp.NOT_READY, "IS-14 n'est pas activé sur cet appareil")
        return jsonify(["rolePaths/"])

    @bp.route(RACINE + "/rolePaths/", methods=["GET"])
    def is14_role_paths():
        app, err = _appareil_ou_erreur()
        if err:
            return err
        return jsonify(["{}/".format(".".join(c)) for c in app.chemins()])

    @bp.route(RACINE + "/rolePaths/<rolepath>/", methods=["GET"])
    @bp.route(RACINE + "/rolePaths/<rolepath>", methods=["GET"])
    def is14_role_path(rolepath):
        _app, _obj, _c, err = _resoudre(rolepath)
        if err:
            return err
        return jsonify(["bulkProperties/", "descriptor/", "methods/", "properties/"])

    @bp.route(RACINE + "/rolePaths/<rolepath>/descriptor", methods=["GET"])
    @bp.route(RACINE + "/rolePaths/<rolepath>/descriptor/", methods=["GET"])
    def is14_descripteur(rolepath):
        _app, obj, _c, err = _resoudre(rolepath)
        if err:
            return err
        d = ncp.registre().descripteur_classe(obj.class_id, inclure_herite=True)
        if d is None:
            return _err(500, ncp.DEVICE_ERROR, "classe absente du registre")
        return jsonify(ncp.ok(d, avec_valeur=True))

    @bp.route(RACINE + "/rolePaths/<rolepath>/properties/", methods=["GET"])
    def is14_proprietes(rolepath):
        _app, obj, _c, err = _resoudre(rolepath)
        if err:
            return err
        return jsonify(["%dp%d/" % cle for cle in sorted(obj._props)])

    @bp.route(RACINE + "/rolePaths/<rolepath>/properties/<pid>/", methods=["GET"])
    @bp.route(RACINE + "/rolePaths/<rolepath>/properties/<pid>", methods=["GET"])
    def is14_propriete(rolepath, pid):
        _app, _obj, _c, err = _resoudre(rolepath, pid_txt=pid)
        if err:
            return err
        return jsonify(["descriptor/", "value/"])

    @bp.route(RACINE + "/rolePaths/<rolepath>/properties/<pid>/descriptor", methods=["GET"])
    @bp.route(RACINE + "/rolePaths/<rolepath>/properties/<pid>/descriptor/", methods=["GET"])
    def is14_propriete_descripteur(rolepath, pid):
        _app, obj, cle, err = _resoudre(rolepath, pid_txt=pid)
        if err:
            return err
        nom_type = obj._props[cle].get("typeName")
        d = ncp.registre().descripteur_type(nom_type, inclure_herite=True) if nom_type else None
        if d is None:
            # Une propriété peut n'avoir aucun type nommé (valeur libre) : le dire plutôt que
            # d'inventer un descripteur.
            return _err(404, ncp.PARAMETER_ERROR,
                        "aucun descripteur de type pour %s" % (nom_type or "(type libre)"))
        return jsonify(ncp.ok(d, avec_valeur=True))

    @bp.route(RACINE + "/rolePaths/<rolepath>/properties/<pid>/value", methods=["GET"])
    @bp.route(RACINE + "/rolePaths/<rolepath>/properties/<pid>/value/", methods=["GET"])
    def is14_valeur_get(rolepath, pid):
        _app, obj, cle, err = _resoudre(rolepath, pid_txt=pid)
        if err:
            return err
        return jsonify(obj.invoquer((1, 1), {"id": {"level": cle[0], "index": cle[1]}}))

    @bp.route(RACINE + "/rolePaths/<rolepath>/properties/<pid>/value", methods=["PUT"])
    @bp.route(RACINE + "/rolePaths/<rolepath>/properties/<pid>/value/", methods=["PUT"])
    def is14_valeur_put(rolepath, pid):
        _app, obj, cle, err = _resoudre(rolepath, pid_txt=pid)
        if err:
            return err
        corps = request.get_json(silent=True)
        if not isinstance(corps, dict) or "value" not in corps:
            return _err(400, ncp.BAD_COMMAND_FORMAT, "corps attendu : {\"value\": …}")
        return jsonify(obj.invoquer((1, 2), {"id": {"level": cle[0], "index": cle[1]},
                                             "value": corps["value"]}))

    @bp.route(RACINE + "/rolePaths/<rolepath>/methods/", methods=["GET"])
    def is14_methodes(rolepath):
        _app, obj, _c, err = _resoudre(rolepath)
        if err:
            return err
        return jsonify(["%dm%d/" % cle for cle in sorted(obj._methodes)])

    @bp.route(RACINE + "/rolePaths/<rolepath>/methods/<mid>", methods=["PATCH"])
    @bp.route(RACINE + "/rolePaths/<rolepath>/methods/<mid>/", methods=["PATCH"])
    def is14_methode(rolepath, mid):
        _app, obj, cle, err = _resoudre(rolepath, mid_txt=mid)
        if err:
            return err
        corps = request.get_json(silent=True)
        if not isinstance(corps, dict) or not isinstance(corps.get("arguments"), dict):
            return _err(400, ncp.BAD_COMMAND_FORMAT,
                        "corps attendu : {\"arguments\": {…}} (objet vide si la méthode n'en prend pas)")
        return jsonify(obj.invoquer(cle, corps["arguments"]))

    # ─── Sauvegarde et restauration ──────────────────────────────────────────────────────────
    @bp.route(RACINE + "/rolePaths/<rolepath>/bulkProperties", methods=["GET"])
    @bp.route(RACINE + "/rolePaths/<rolepath>/bulkProperties/", methods=["GET"])
    def is14_bulk_get(rolepath):
        app, obj, _c, err = _resoudre(rolepath)
        if err:
            return err
        return jsonify(app.bulk_manager.invoquer((3, 1), {
            "path": obj.chemin(),
            "recurse": _bool_param("recurse"),
            "includeDescriptors": _bool_param("includeDescriptors"),
        }))

    def _bulk_ecriture(rolepath, methode):
        app, obj, _c, err = _resoudre(rolepath)
        if err:
            return err
        corps = request.get_json(silent=True)
        args = (corps or {}).get("arguments") if isinstance(corps, dict) else None
        if not isinstance(args, dict) or not isinstance(args.get("dataSet"), dict):
            return _err(400, ncp.BAD_COMMAND_FORMAT,
                        "corps attendu : {\"arguments\": {\"dataSet\": …, \"recurse\": …, "
                        "\"restoreMode\": …}}")
        return jsonify(app.bulk_manager.invoquer(methode, {
            "path": obj.chemin(),
            "dataSet": args["dataSet"],
            "recurse": args.get("recurse", True),
            "restoreMode": args.get("restoreMode", ncp.RESTORE_MODIFY),
        }))

    @bp.route(RACINE + "/rolePaths/<rolepath>/bulkProperties", methods=["PATCH"])
    @bp.route(RACINE + "/rolePaths/<rolepath>/bulkProperties/", methods=["PATCH"])
    def is14_bulk_valider(rolepath):
        # PATCH = VALIDER. La spec est explicite : aucun changement ne doit être appliqué.
        return _bulk_ecriture(rolepath, (3, 2))

    @bp.route(RACINE + "/rolePaths/<rolepath>/bulkProperties", methods=["PUT"])
    @bp.route(RACINE + "/rolePaths/<rolepath>/bulkProperties/", methods=["PUT"])
    def is14_bulk_restaurer(rolepath):
        return _bulk_ecriture(rolepath, (3, 3))
