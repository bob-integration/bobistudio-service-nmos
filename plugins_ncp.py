"""Paramètres de plugin exposés en MS-05-02 (IS-12 / IS-14).

Étape 3 du chantier « NMOS dans les conteneurs » : rendre nos plugins pilotables par un outil de
contrôle tiers, avec le même modèle d'appareil que celui déjà servi sur IS-12 et IS-14.

═══ La décision de modélisation, et pourquoi elle est celle-là ════════════════════════════════

MS-05-02 est un modèle À PROPRIÉTÉS déclarées PAR CLASSE. La tentation est d'émettre **une classe
par type de plugin**, dont les propriétés seraient les champs de son `param_tree`. C'est plus
idiomatique… et c'est un piège : le `classId` d'une classe non standard doit encoder un index, et
cet index n'a aucune source stable. Le dériver de la liste des types installés le fait GLISSER dès
qu'un plugin est ajouté — or un `classId` publié est un contrat. Un contrôleur qui a mémorisé
`[1,2,0,7]` piloterait soudain autre chose, sans erreur nulle part.

D'où le modèle retenu : **deux classes, aux identifiants fixes à jamais**.

    NcBobiPlugin     [1, 1, <clé>, 1]  dérive NcBlock  — un bloc par conteneur de plugin
      └── NcBobiParametre [1, 2, <clé>, 1] dérive NcWorker — UN OBJET PAR PARAMÈTRE

Un paramètre devient donc un OBJET, pas une propriété de classe. Le contrôleur énumère les membres
du bloc et lit sur chacun `key`, `value`, `minimum`, `maximum`, `step`… — entièrement typé et
auto-descriptif, sans qu'aucune classe ne dépende du jeu de plugins installé. Écrire un paramètre,
c'est un `Set` standard sur `value`.

**Compromis assumé sur le type de `value`** : MS-05-02 n'a pas de type variant, et nos paramètres
sont tantôt nombres, tantôt énumérations, tantôt booléens. `value` est donc publié en `NcString`
(forme canonique) avec `valueType` à côté pour que le contrôleur sache coercer. Publier trois
propriétés typées mutuellement exclusives aurait été pire : deux d'entre elles seraient toujours
nulles, sans que rien ne dise laquelle fait foi.

═══ La clé d'autorité ═════════════════════════════════════════════════════════════════════════

MS-05-02 : « For organizations which own a unique CID or OUI the authority key MUST be the
organization identifier as an integer which MUST be negated. For organizations which do not own a
unique CID or OUI the authority key MUST be 0. »

Un **CID IEEE a été demandé le 2026-08-30**, il n'est pas encore attribué. D'ici là : `0`, ce qui
est légal — le seul risque est la collision avec les classes d'un autre éditeur utilisant aussi 0,
quand un contrôleur parle aux deux.

⚠ **UN SEUL LITTÉRAL DANS TOUT LE PRODUIT** (`CLE_AUTORITE` ci-dessous), exactement comme le PEN
IANA pour SNMP. La clé est EMBARQUÉE dans chaque `classId` publié : la disséminer, c'est se
garantir un parc à moitié migré le jour de l'attribution, et des `classId` incohérents qu'aucun
contrôleur ne signalera.
"""

import json
import logging
import unicodedata

from . import ncp

log = logging.getLogger(__name__)

# ★★★ LA clé d'autorité. Ne JAMAIS en écrire une autre ailleurs — voir l'entête.
# À l'attribution du CID IEEE : remplacer par -<CID> ici, et ICI SEULEMENT.
CLE_AUTORITE = 0

CLASSE_PLUGIN = [1, 1, CLE_AUTORITE, 1]        # dérive NcBlock
CLASSE_PARAMETRE = [1, 2, CLE_AUTORITE, 1]     # dérive NcWorker
CLASSE_ACTION = [1, 2, CLE_AUTORITE, 2]        # dérive NcWorker
CLASSE_SCRIPT = [1, 2, CLE_AUTORITE, 3]        # dérive NcWorker


def _prop(index, nom, type_nom, lecture_seule=True, nullable=False, description=""):
    """NcPropertyDescriptor de niveau 3 — le niveau de nos classes, qui dérivent d'une classe
    standard de profondeur 2 (NcBlock ou NcWorker)."""
    return {
        "id": {"level": 3, "index": index},
        "name": nom,
        "typeName": type_nom,
        "isReadOnly": lecture_seule,
        "isNullable": nullable,
        "isSequence": False,
        "isDeprecated": False,
        "constraints": None,
        "description": description,
    }


DESCRIPTEURS = [
    {
        "description": "Bloc représentant un conteneur de plugin Bobi.Studio",
        "classId": CLASSE_PLUGIN,
        "name": "NcBobiPlugin",
        "fixedRole": None,
        "properties": [
            _prop(1, "pluginType", "NcString", description="Type de plugin (mixer, multiview…)"),
            _prop(2, "vmid", "NcInt32", description="Handle local du conteneur — JAMAIS une "
                                                    "identité stable, cf. CLAUDE.md"),
            _prop(3, "hostname", "NcString", description="Nom d'hôte du conteneur"),
            _prop(4, "instanceUuid", "NcString", nullable=True,
                  description="Identité d'INSTANCE, survit au recreate"),
        ],
        "methods": [],
        "events": [],
    },
    {
        "description": "Un paramètre pilotable d'un plugin Bobi.Studio",
        "classId": CLASSE_PARAMETRE,
        "name": "NcBobiParametre",
        "fixedRole": None,
        "properties": [
            _prop(1, "key", "NcString", description="Clé du paramètre dans le plugin"),
            _prop(2, "groupLabel", "NcString", description="Groupe d'appartenance"),
            # ⚠ SEULE propriété inscriptible du modèle : c'est par elle que passe tout le pilotage.
            _prop(3, "value", "NcString", lecture_seule=False, nullable=True,
                  description="Valeur courante, en forme canonique (cf. valueType)"),
            _prop(4, "valueType", "NcString",
                  description="number | enum | boolean | string — comment coercer `value`"),
            _prop(5, "minimum", "NcFloat32", nullable=True),
            _prop(6, "maximum", "NcFloat32", nullable=True),
            _prop(7, "step", "NcFloat32", nullable=True),
            _prop(8, "defaultValue", "NcString", nullable=True),
        ],
        "methods": [],
        "events": [],
    },
]


DESCRIPTEURS.append({
    "description": "Le SCRIPT d'un conteneur Bobi.Studio — démarrable et arrêtable.",
    "classId": CLASSE_SCRIPT,
    "name": "NcBobiScript",
    "fixedRole": "script",
    "properties": [
        # `enabled` n'est PAS déclarée ici : elle est HÉRITÉE de NcWorker (2p1), et c'est tout
        # l'intérêt. NcBlock.enabled est en LECTURE SEULE dans MS-05-02 ; seul NcWorker.enabled
        # est inscriptible. Le point de contrôle ne pouvait donc pas être le bloc du plugin — il
        # fallait un Worker dédié, dont c'est la raison d'être.
        _prop(1, "scriptPath", "NcString", nullable=True,
              description="Chemin du script déployé, null si le rootfs a été recréé"),
        _prop(2, "lastExit", "NcInt32", nullable=True,
              description="Code de sortie de la dernière exécution"),
    ],
    "methods": [],
    "events": [],
})

DESCRIPTEURS.append({
    "description": "Une action discrète d'un plugin Bobi.Studio (charger, démarrer, rappeler…)",
    "classId": CLASSE_ACTION,
    "name": "NcBobiAction",
    "fixedRole": None,
    "properties": [
        _prop(1, "actionId", "NcString", description="Identifiant de l'action au manifeste"),
        _prop(2, "label", "NcString"),
        # Les champs attendus sont DÉCRITS, pas déclarés en paramètres de méthode : voir Invoke.
        _prop(3, "argumentFields", "NcString",
              description="JSON : [{key, label, type, default, optional, optionsEndpoint}]"),
        _prop(4, "fixedBody", "NcString", nullable=True,
              description="JSON des champs FIXES portés par l'action (non modifiables)"),
    ],
    "methods": [{
        "description": "Déclenche l'action. `argumentsJson` est un objet JSON {clé: valeur} dont "
                       "les clés attendues sont décrites par la propriété argumentFields.",
        "id": {"level": 3, "index": 1},
        "name": "Invoke",
        "resultDatatype": "NcMethodResult",
        "parameters": [{
            "description": "Objet JSON des arguments, ou null si l'action n'en prend pas",
            "name": "argumentsJson",
            "typeName": "NcString",
            "isNullable": True,
            "isSequence": False,
            "constraints": None,
        }],
        "isDeprecated": False,
    }],
    "events": [],
})


def enregistrer_classes():
    """Déclare nos classes non standard au registre MS-05-02.

    Sans ça, `NcClassManager.GetControlClass` répondrait « classe inconnue » et un contrôleur ne
    saurait pas interpréter nos objets — il les verrait sans pouvoir lire leurs propriétés. Les
    modèles AMWA vendorisés ne sont PAS touchés : on ajoute au registre en mémoire."""
    reg = ncp.registre()
    for d in DESCRIPTEURS:
        reg.classes[tuple(d["classId"])] = d


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Objets
# ══════════════════════════════════════════════════════════════════════════════════════════════

def _canon(v):
    """Forme canonique texte d'une valeur de paramètre (cf. le compromis sur `value`)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _appliquer(vmid, spec, valeur):
    """Pousse la valeur au CONTENEUR par le chemin d'écriture existant.

    `macros.exec_post` valide l'endpoint contre la liste blanche `control.endpoints` du manifeste :
    on ne contourne pas cette garde, on s'y branche. Une surface NMOS qui ouvrirait des endpoints
    non déclarés serait un contournement, exactement l'anti-patron « un garde conditionné à QUI
    APPELLE ne protège que celui-là »."""
    from app import macros
    corps = {spec["key"]: valeur}
    if spec.get("wrap"):
        corps = {spec["wrap"]: corps}
    macros.exec_post(vmid, spec.get("endpoint"), corps)


def _feuilles(vmid):
    """[{key, label, group_label, kind, min, max, step, default, options, endpoint, wrap}] d'un
    conteneur, aplaties depuis `macros.param_tree` — la MÊME résolution que l'éditeur de macros,
    caps live comprises. Ne pas re-dériver depuis le manifeste : les bornes viendraient alors des
    déclarations et non du contrat vivant du plugin, et on publierait des bornes fausses."""
    from app import macros
    try:
        arbre = macros.param_tree(vmid) or {}
    except Exception as e:
        log.warning("nmos/plugins: param_tree(%s) illisible : %s", vmid, e)
        return []
    _KIND = {"bool": "boolean", "enum": "enum", "text": "string"}
    out = []
    for el in arbre.get("elements") or []:
        for g in el.get("groups") or []:
            for p in g.get("params") or []:
                out.append({
                    "key": p.get("key"),
                    "label": p.get("label"),
                    "group_label": g.get("label") or (el.get("label") or ""),
                    "element": el.get("id"),
                    "kind": _KIND.get(p.get("type"), "number"),
                    "min": p.get("min"), "max": p.get("max"), "step": p.get("step"),
                    "default": p.get("default"), "options": p.get("options"),
                    "endpoint": p.get("endpoint"), "wrap": p.get("wrap"),
                })
    return out


def _role(el, groupe, cle):
    """Rôle STABLE d'un paramètre dans son bloc. Le rôle est l'adresse d'un objet dans le modèle
    (`GetMemberDescriptors`, chemins IS-14) : il doit dépendre de l'identité du paramètre, jamais
    de son rang dans une liste — un paramètre ajouté au manifeste décalerait tous les autres."""
    brut = "_".join(str(x) for x in (el or "", groupe or "", cle or "") if x != "")
    # ⚠ ASCII STRICT. `str.isalnum()` accepte les accents (« à ».isalnum() est True), et nos
    # libellés de groupes sont en français : un rôle « groupe_à_b » se retrouverait dans un chemin
    # d'URL IS-14 (`/rolePaths/<chemin>`) et dans les recherches par chemin. On translittère donc
    # (é→e) plutôt que de remplacer par « _ » : deux groupes « Réglages » et « Reglages » doivent
    # rester distincts de « Rglages », et un rôle illisible ne s'associe plus à rien pour l'humain.
    brut = unicodedata.normalize("NFKD", brut)
    brut = "".join(c for c in brut if not unicodedata.combining(c))
    return "".join(c if (c.isascii() and (c.isalnum() or c == "_")) else "_"
                   for c in brut).strip("_").lower()


class Parametre(ncp.NcWorker):
    """Un paramètre pilotable. L'écriture de `value` part vers le CONTENEUR, pas en base.

    ★ On n'écrit jamais dans `deploy_config` ici : le plugin est la source de vérité de son état
    courant, et une écriture en base que le conteneur ignorerait serait un pilotage fantôme — le
    contrôleur verrait sa consigne acceptée sans effet."""

    class_id = CLASSE_PARAMETRE

    def __init__(self, appareil, oid, role, owner, vmid, spec):
        super().__init__(appareil, oid, role, owner,
                         user_label=spec.get("label") or spec.get("key"),
                         description="Paramètre %s" % spec.get("key"))
        self.vmid = vmid
        self.spec = spec
        self._vals.update({
            "key": spec.get("key"),
            "groupLabel": spec.get("group_label") or "",
            "value": _canon(spec.get("value", spec.get("default"))),
            "valueType": spec.get("kind") or "string",
            "minimum": spec.get("min"),
            "maximum": spec.get("max"),
            "step": spec.get("step"),
            "defaultValue": _canon(spec.get("default")),
        })

    def _ecrire(self, nom, valeur):
        if nom != "value":
            return super()._ecrire(nom, valeur)
        borne = self._verifier(valeur)
        if borne is not None:
            return ncp.erreur(ncp.PARAMETER_ERROR, borne)
        try:
            _appliquer(self.vmid, self.spec, valeur)
        except Exception as e:
            # Un échec d'application DOIT remonter en erreur : rendre « ok » sur une consigne que
            # le conteneur n'a pas reçue est le pilotage fantôme qu'on refuse.
            log.warning("nmos/plugins: écriture %s sur vmid %s échouée : %s",
                        self.spec.get("key"), self.vmid, e)
            return ncp.erreur(ncp.PARAMETER_ERROR, "le conteneur a refusé la valeur : %s" % e)
        return super()._ecrire(nom, _canon(valeur))

    def _verifier(self, valeur):
        """Message d'erreur si la valeur sort des bornes DÉCLARÉES, sinon None. On vérifie ici
        parce qu'un contrôleur a le droit de faire confiance aux bornes qu'on publie : les lui
        annoncer puis accepter au-delà serait se contredire."""
        kind = self._vals.get("valueType")
        if kind == "enum":
            options = self.spec.get("options") or []
            if options and str(valeur) not in [str(o) for o in options]:
                return "valeur hors énumération %s" % (options,)
            return None
        if kind == "number":
            try:
                v = float(valeur)
            except (TypeError, ValueError):
                return "valeur non numérique"
            mn, mx = self._vals.get("minimum"), self._vals.get("maximum")
            if mn is not None and v < float(mn):
                return "valeur sous le minimum publié (%s)" % mn
            if mx is not None and v > float(mx):
                return "valeur au-dessus du maximum publié (%s)" % mx
        return None


class Action(ncp.NcWorker):
    """Une action discrète, déclenchable par `Invoke`.

    ★ POURQUOI UNE MÉTHODE GÉNÉRIQUE ET NON UNE MÉTHODE TYPÉE PAR ACTION. MS-05-02 déclare les
    paramètres d'une méthode STATIQUEMENT, par classe. Des méthodes typées par action exigeraient
    une classe par action — donc un `classId` encodant un index qui n'a aucune source stable, et
    qui glisserait dès qu'un plugin ajoute une action. C'est exactement le piège évité pour les
    paramètres. On publie donc UNE méthode `Invoke(argumentsJson)` et on DÉCRIT les champs
    attendus dans `argumentFields` : le contrôleur peut construire son formulaire, et le contrat
    de classe ne dépend d'aucun manifeste."""

    class_id = CLASSE_ACTION

    def __init__(self, appareil, oid, role, owner, vmid, action):
        super().__init__(appareil, oid, role, owner,
                         user_label=action.get("label") or action.get("id"),
                         description="Action %s" % action.get("id"))
        self.vmid = vmid
        self.action = action
        champs = [{"key": p.get("key"), "label": p.get("label"), "type": p.get("type") or "text",
                   "default": p.get("default"), "optional": bool(p.get("optional")),
                   # Liste VIVANTE : le contrôleur doit interroger cet endpoint au moment de
                   # composer son formulaire. Figer les options ici les périmerait aussitôt
                   # (fichiers d'un lecteur, presets, sources disponibles…).
                   "optionsEndpoint": p.get("options_endpoint")}
                  for p in (action.get("params") or [])]
        self._vals.update({
            "actionId": action.get("id"),
            "label": action.get("label") or action.get("id"),
            "argumentFields": json.dumps(champs, ensure_ascii=False),
            "fixedBody": json.dumps(action.get("body"), ensure_ascii=False)
                         if action.get("body") else None,
        })

    def _enregistrer_methodes(self):
        super()._enregistrer_methodes()
        self._methodes[(3, 1)] = self._m_invoke

    def _m_invoke(self, args):
        brut = (args or {}).get("argumentsJson")
        params = {}
        if brut not in (None, ""):
            try:
                params = json.loads(brut) if isinstance(brut, str) else brut
            except Exception as e:
                return ncp.erreur(ncp.PARAMETER_ERROR, "argumentsJson n'est pas du JSON : %s" % e)
            if not isinstance(params, dict):
                return ncp.erreur(ncp.PARAMETER_ERROR,
                                  "argumentsJson doit être un OBJET {clé: valeur}")
        # Les clés inconnues sont REFUSÉES plutôt qu'ignorées : une faute de frappe silencieusement
        # avalée ferait croire à l'opérateur que sa consigne est partie, alors que l'action
        # s'exécuterait avec ses valeurs par défaut.
        attendues = {p.get("key") for p in (self.action.get("params") or [])}
        inconnues = set(params) - attendues
        if inconnues:
            return ncp.erreur(ncp.PARAMETER_ERROR,
                              "argument(s) inconnu(s) : %s (attendus : %s)"
                              % (sorted(inconnues), sorted(attendues) or "aucun"))
        try:
            _executer(self.vmid, self.action.get("id"), params)
        except Exception as e:
            log.warning("nmos/plugins: action %s sur vmid %s échouée : %s",
                        self.action.get("id"), self.vmid, e)
            return ncp.erreur(ncp.PARAMETER_ERROR, "l'action a échoué : %s" % e)
        return ncp.ok()


def _executer(vmid, action_id, params):
    """Déclenche l'action par le chemin existant — qui valide l'action contre le manifeste,
    applique les défauts, coerce les types et gère le cas `core: "recall"`."""
    from app import macros
    macros.exec_action(vmid, action_id, params)


class BlocPlugin(ncp.NcBlock):
    """Un conteneur de plugin, avec ses paramètres en membres."""

    class_id = CLASSE_PLUGIN

    def __init__(self, appareil, oid, role, owner, conteneur, type_plugin):
        super().__init__(appareil, oid, role, owner,
                         user_label=conteneur.get("hostname") or role,
                         description="Plugin %s" % type_plugin)
        self._vals.update({
            "pluginType": type_plugin,
            "vmid": int(conteneur.get("vmid") or 0),
            "hostname": conteneur.get("hostname") or "",
            "instanceUuid": conteneur.get("instance_uuid"),
        })


class Script(ncp.NcWorker):
    """Le script d'un conteneur, vu comme un NcWorker : `enabled` le démarre et l'arrête.

    ★ POURQUOI UN OBJET DÉDIÉ. On aurait voulu poser cela sur le bloc du plugin. MS-05-02 l'interdit :
    `NcBlock.enabled` est en LECTURE SEULE (« TRUE if block is functional ») ; seul
    `NcWorker.enabled` est inscriptible. Le contrôle du cycle de vie appartient donc à un Worker.

    ★ LECTURE = CONSTAT, ÉCRITURE = INTENTION. `enabled` se LIT sur l'agent (le script tourne-t-il
    vraiment ?) et s'ÉCRIT à la fois en base et à l'agent. Les deux ne coïncident pas toujours, et
    c'est la seule façon de distinguer « arrêté parce qu'on l'a demandé » de « arrêté parce qu'il
    est tombé ». Rendre l'intention à la place du constat ferait mentir la propriété au moment
    précis où on a besoin qu'elle dise vrai.

    ⚠ Ce qui n'est PAS fait, et qui est assumé : arrêter le script n'arrête pas le CONTENEUR. Le
    conteneur reste up, l'agent répond, la production de flux cesse. Piloter le conteneur lui-même
    demanderait de passer par l'agent-nœud et de décider du sort des ressources réservées — un
    autre sujet, qu'on ne cache pas derrière la même propriété."""

    class_id = CLASSE_SCRIPT

    def __init__(self, appareil, oid, role, owner, vmid, hostname):
        super().__init__(appareil, oid, role, owner,
                         user_label="Script — %s" % (hostname or vmid),
                         description="Script déployé du conteneur %s" % vmid)
        self.vmid = vmid
        self._vals.update({"scriptPath": None, "lastExit": None})

    def _statut(self):
        """`/status` de l'agent, ou {} s'il ne répond pas."""
        from app.database import db_get_container
        from app.deploy import agent_headers, agent_port, agent_session, agent_url
        c = db_get_container(self.vmid) or {}
        ip = c.get("ip")
        if not ip:
            return {}
        try:
            r = agent_session().get(agent_url(ip, "/status", port=agent_port(self.vmid)),
                                    timeout=3, headers=agent_headers(self.vmid))
            return (r.json() or {}) if r.status_code == 200 else {}
        except Exception:
            return {}

    def _lire(self, nom):
        if nom not in ("enabled", "scriptPath", "lastExit"):
            return super()._lire(nom)
        st = self._statut()
        if not st:
            # L'agent ne répond pas : on ne SAIT pas. On rend l'intention plutôt qu'un `false` qui
            # ferait croire à un arrêt volontaire — et `scriptPath` reste null, ce qui dit au
            # contrôleur que le constat manque.
            if nom == "enabled":
                from app.database import db_script_enabled
                return db_script_enabled(self.vmid)
            return None
        if nom == "enabled":
            return bool(st.get("running"))
        if nom == "scriptPath":
            return st.get("path")
        return st.get("last_exit")

    def _ecrire(self, nom, valeur):
        if nom != "enabled":
            return super()._ecrire(nom, valeur)
        from app.database import db_get_container, db_set_script_enabled
        from app.deploy import agent_headers, agent_port, agent_session, agent_url
        voulu = bool(valeur)
        c = db_get_container(self.vmid) or {}
        ip = c.get("ip")
        if not ip:
            return ncp.erreur(ncp.DEVICE_ERROR, "conteneur %s sans IP" % self.vmid)
        # L'INTENTION D'ABORD. Si l'appel à l'agent échoue, l'intention reste posée et le prochain
        # déploiement la respectera : mieux vaut une consigne enregistrée pas encore appliquée
        # qu'une consigne appliquée que rien ne mémorise.
        db_set_script_enabled(self.vmid, voulu)
        try:
            r = agent_session().post(agent_url(ip, "/start" if voulu else "/stop",
                                               port=agent_port(self.vmid)),
                                     timeout=6, headers=agent_headers(self.vmid))
        except Exception as e:
            return ncp.erreur(ncp.DEVICE_ERROR, "agent injoignable : %s" % str(e)[:80])
        if r.status_code != 200:
            return ncp.erreur(ncp.DEVICE_ERROR, "agent a répondu %s" % r.status_code)
        self.notifier("enabled", voulu)
        log.info("MS-05-02 : script du conteneur %s %s par un contrôleur NMOS",
                 self.vmid, "démarré" if voulu else "ARRÊTÉ")
        return ncp.ok()


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Synchronisation avec le parc
# ══════════════════════════════════════════════════════════════════════════════════════════════

_blocs = {}          # vmid → BlocPlugin
_params = {}         # (vmid, role) → Parametre
_actions = {}        # (vmid, role) → Action
_scripts = {}        # vmid → Script


def actif():
    """Le réglage `nmos_plugins_ncp` — FERMÉ par défaut. Publier le modèle, c'est publier un
    CONTRAT que d'autres mémoriseront ; on ne l'ouvre pas sans que l'exploitant l'ait demandé,
    d'autant que la clé d'autorité est encore `0` (CID IEEE en attente)."""
    from . import _setting
    return str(_setting("nmos_plugins_ncp", "0")).strip().lower() in ("1", "true", "on", "yes")


def _actions_du_type(t):
    """Actions déclarées au manifeste du type, hors celles sans endpoint ni `core`."""
    from app import plugins as _plg
    out = []
    for a in ((_plg.get(t) or {}).get("actions") or []):
        if a.get("id") and (a.get("endpoint") or a.get("core")):
            out.append(a)
    return out


def _conteneurs_pilotables():
    """[(conteneur, type)] des conteneurs dont le plugin déclare un `param_tree` OU des actions."""
    import json as _json
    from app import plugins as _plg
    from app.database import db_get_containers
    out = []
    for c in db_get_containers():
        dc = c.get("deploy_config")
        try:
            dc = _json.loads(dc) if isinstance(dc, str) else dc
        except Exception:
            continue
        t = (dc or {}).get("type")
        m = _plg.get(t) or {}
        if t and (m.get("param_tree") or _actions_du_type(t)):
            out.append((c, t))
    return out


def sync(appareil, bloc_parent):
    """Aligne les blocs de plugins sur le parc. Idempotent, appelable à chaud.

    Même discipline que `modele.sync_model` : les mutations de l'arbre notifient les sessions
    abonnées, on ne les fait donc pas sous un verrou d'état."""
    if appareil is None or bloc_parent is None:
        return
    if not actif():
        for vmid in list(_blocs):
            appareil.retirer(bloc_parent, _blocs.pop(vmid))
        _params.clear()
        _actions.clear()
        _scripts.clear()
        return

    voulus = {c["vmid"]: (c, t) for c, t in _conteneurs_pilotables()}

    for vmid in [v for v in _blocs if v not in voulus]:
        appareil.retirer(bloc_parent, _blocs.pop(vmid))
        for cle in [k for k in _params if k[0] == vmid]:
            _params.pop(cle, None)
        for cle in [k for k in _actions if k[0] == vmid]:
            _actions.pop(cle, None)
        _scripts.pop(vmid, None)

    for vmid, (c, t) in voulus.items():
        bloc = _blocs.get(vmid)
        if bloc is None:
            bloc = BlocPlugin(appareil, appareil.oid_libre(), "plugin_%d" % vmid,
                              bloc_parent.oid, c, t)
            appareil.ajouter(bloc_parent, bloc)
            _blocs[vmid] = bloc
        else:
            bloc.poser("userLabel", c.get("hostname") or bloc.role)

        feuilles = {}
        for f in _feuilles(vmid):
            if not f.get("endpoint") or not f.get("key"):
                # Sans endpoint, le paramètre serait LISIBLE mais pas inscriptible. Publier un
                # objet dont l'écriture échouera toujours vaut moins que ne pas le publier.
                continue
            feuilles[_role(f.get("element"), f.get("group_label"), f.get("key"))] = f

        for cle in [k for k in _params if k[0] == vmid and k[1] not in feuilles]:
            appareil.retirer(bloc, _params.pop(cle))
        for role, f in feuilles.items():
            if (vmid, role) in _params:
                continue
            p = Parametre(appareil, appareil.oid_libre(), role, bloc.oid, vmid, f)
            appareil.ajouter(bloc, p)
            _params[(vmid, role)] = p

        # ── Actions ───────────────────────────────────────────────────────────────────────
        # Rôle préfixé `action_` : sans ça, une action et un paramètre de même nom (« fond » sur
        # hello_world, justement) se disputeraient la même adresse dans le bloc.
        actes = {"action_" + _role(None, None, a["id"]): a for a in _actions_du_type(t)}
        for cle in [k for k in _actions if k[0] == vmid and k[1] not in actes]:
            appareil.retirer(bloc, _actions.pop(cle))
        for role, a in actes.items():
            if (vmid, role) in _actions:
                continue
            obj = Action(appareil, appareil.oid_libre(), role, bloc.oid, vmid, a)
            appareil.ajouter(bloc, obj)
            _actions[(vmid, role)] = obj

        # ── Le script, en NcWorker ────────────────────────────────────────────────────────
        # Rôle FIXE « script » (fixedRole du descripteur) : un contrôleur doit pouvoir l'atteindre
        # par chemin sans deviner un nom, et il n'y en a qu'un par plugin.
        if vmid not in _scripts:
            sc = Script(appareil, appareil.oid_libre(), "script", bloc.oid, vmid,
                        c.get("hostname"))
            appareil.ajouter(bloc, sc)
            _scripts[vmid] = sc

    log.info("MS-05-02 : %d plugin(s) pilotable(s), %d paramètre(s), %d action(s) et "
             "%d script(s) publiés", len(_blocs), len(_params), len(_actions), len(_scripts))


def etat():
    """Résumé pour l'UI et les bancs."""
    return {"actif": actif(), "cle_autorite": CLE_AUTORITE,
            "classes": [d["name"] for d in DESCRIPTEURS],
            "blocs": len(_blocs), "parametres": len(_params), "actions": len(_actions)}
