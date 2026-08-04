# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Socle AMWA MS-05-02 (« NMOS Control Framework ») — modèle d'appareil en mémoire.

Ce module ne connaît AUCUN transport : il expose un arbre d'objets (`NcObject` et dérivés) et le
dispatch générique des méthodes. Les transports en sont des façades — IS-12 (WebSocket) dans
`is12.py`, IS-14 (REST) dans `is14.py` — le modèle vivant dans `modele.py` et les monitors
BCP-008 dans `monitors.py`.

★ Le modèle publié EST le modèle exécuté. Les descripteurs de classes et de types sont chargés
depuis `nc_models/*.json` (recopie verbatim des dépôts AMWA, cf. le NOTICE du dossier), et
c'est CE registre qui fournit :
  - la table des propriétés d'un objet (nom, id `level`/`index`, lecture seule, nullable, séquence) ;
  - le contrôle d'accès du getter/setter générique (1m1/1m2) ;
  - la réponse du `NcClassManager` aux commandes de découverte (3m1/3m2).
Autrement dit, un contrôleur qui découvre le modèle et un contrôleur qui l'interroge obtiennent
forcément la même chose. La transcription à la main des identifiants (4p12, 4m3…) est le mode de
panne classique de ces implémentations : elle produit un appareil qui se comprend lui-même et que
personne d'autre ne lit, sans jamais lever d'erreur.

Convention de nommage : les symboles publics du modèle gardent leurs noms AMWA (camelCase), tout
le reste suit la convention française du projet.
"""
import json
import logging
import os
import threading

log = logging.getLogger(__name__)

_MODELES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nc_models")

# ─── NcMethodStatus (MS-05-02) — les seuls codes utilisés ici ────────────────────────────────
OK                      = 200
BAD_COMMAND_FORMAT      = 400
BAD_OID                 = 404
READONLY                = 405
INVALID_REQUEST         = 406
INDEX_OUT_OF_BOUNDS     = 414
PARAMETER_ERROR         = 417
DEVICE_ERROR            = 500
METHOD_NOT_IMPLEMENTED  = 501
PROPERTY_NOT_IMPLEMENTED= 502
NOT_READY               = 503

# NcPropertyChangeType
VALUE_CHANGED        = 0
SEQUENCE_ITEM_ADDED  = 1
SEQUENCE_ITEM_CHANGED= 2
SEQUENCE_ITEM_REMOVED= 3

# NcObject 1e1 PropertyChanged — le seul événement du framework.
EVENT_PROPERTY_CHANGED = {"level": 1, "index": 1}

# Oids fixes de l'arbre (conventions de la spec : le bloc racine est l'oid 1).
OID_ROOT           = 1
OID_DEVICE_MANAGER = 2
OID_CLASS_MANAGER  = 3
OID_BULK_MANAGER   = 4
_OID_PREMIER_LIBRE = 100          # les objets dynamiques commencent au-delà des managers

# NcRestoreMode / NcRestoreValidationStatus / NcPropertyRestoreNoticeType (jeu « device configuration »)
RESTORE_MODIFY   = 0
RESTORE_REBUILD  = 1
RESTORE_OK       = 200
RESTORE_FAILED   = 400
RESTORE_NOT_FOUND= 404
NOTICE_WARNING   = 300
NOTICE_ERROR     = 400


def ok(value=None, avec_valeur=False):
    """NcMethodResult / NcMethodResultPropertyValue. `avec_valeur` force la clé `value` même à None
    (un Get qui rend une propriété nullable DOIT porter `value: null`, pas omettre la clé)."""
    r = {"status": OK}
    if avec_valeur or value is not None:
        r["value"] = value
    return r


def erreur(status, message):
    """NcMethodResultError."""
    return {"status": status, "errorMessage": message}


# ═════════════════════════════════════════════════════════════════════════════════════════════
# Registre des modèles (classes + types) — chargé une fois, au premier usage
# ═════════════════════════════════════════════════════════════════════════════════════════════

class Registre:
    """Descripteurs MS-05-02 + jeux de fonctionnalités, indexés par classId et par nom de type."""

    def __init__(self, fichiers):
        self.classes = {}       # tuple(classId) → NcClassDescriptor
        self.datatypes = {}     # nom → NcDatatypeDescriptor
        self.provenance = []
        for f in fichiers:
            with open(f, "r", encoding="utf-8") as fh:
                doc = json.load(fh)
            self.provenance.append(doc.get("_provenance") or {})
            for c in doc.get("classes") or []:
                self.classes[tuple(c["classId"])] = c
            for d in doc.get("datatypes") or []:
                self.datatypes[d["name"]] = d

    # ─── Héritage ────────────────────────────────────────────────────────────────────────────
    def ancetres(self, class_id):
        """classIds de la classe et de tous ses ancêtres, du plus ancien au plus dérivé.

        Un classId est la LIGNÉE de la classe (`[1,2,2,1]` dérive de `[1,2,2]`), donc les ancêtres
        sont ses préfixes. Les clés d'autorité (entiers négatifs des classes non standard) ne sont
        pas des niveaux d'héritage : le préfixe qui s'arrête sur l'une d'elles est ignoré."""
        cid = tuple(class_id)
        out = []
        for n in range(1, len(cid) + 1):
            prefixe = cid[:n]
            if prefixe[-1] < 0:          # clé d'autorité : pas un niveau à part entière
                continue
            if prefixe in self.classes:
                out.append(prefixe)
        return out

    def descripteur_classe(self, class_id, inclure_herite=True):
        """NcClassDescriptor, avec ou sans les éléments hérités. None si la classe est inconnue."""
        cid = tuple(class_id)
        base = self.classes.get(cid)
        if base is None:
            return None
        if not inclure_herite:
            return base
        props, methodes, events = [], [], []
        for a in self.ancetres(cid):
            d = self.classes[a]
            props.extend(d.get("properties") or [])
            methodes.extend(d.get("methods") or [])
            events.extend(d.get("events") or [])
        fusion = dict(base)
        fusion["properties"], fusion["methods"], fusion["events"] = props, methodes, events
        return fusion

    def descripteur_type(self, nom, inclure_herite=True):
        """NcDatatypeDescriptor. Pour une structure, `inclure_herite` remonte les champs du parent."""
        d = self.datatypes.get(nom)
        if d is None:
            return None
        if not inclure_herite or d.get("type") != 2:      # 2 = Struct
            return d
        champs, parent = [], d.get("parentType")
        vus = {nom}
        while parent and parent not in vus:
            vus.add(parent)
            p = self.datatypes.get(parent)
            if p is None:
                break
            champs = list(p.get("fields") or []) + champs
            parent = p.get("parentType")
        fusion = dict(d)
        fusion["fields"] = champs + list(d.get("fields") or [])
        return fusion

    # ─── Tables de dispatch ──────────────────────────────────────────────────────────────────
    def proprietes(self, class_id):
        """{(level, index) → descripteur} de toutes les propriétés d'une classe, héritage compris."""
        out = {}
        for a in self.ancetres(class_id):
            for p in self.classes[a].get("properties") or []:
                out[(p["id"]["level"], p["id"]["index"])] = p
        return out

    def liste_classes(self):
        return [self.descripteur_classe(c, inclure_herite=False) for c in sorted(self.classes)]

    def liste_types(self):
        return [self.datatypes[n] for n in sorted(self.datatypes)]


_registre = None
_registre_lock = threading.Lock()


def registre():
    """Registre global (chargé au premier appel). Lève si les modèles sont absents/illisibles :
    un socle de contrôle sans son modèle n'est pas dégradé, il est faux — mieux vaut ne pas démarrer."""
    global _registre
    with _registre_lock:
        if _registre is None:
            fichiers = [os.path.join(_MODELES_DIR, f)
                        for f in ("ms-05-02.json", "bcp-008-monitoring.json",
                                  "device-configuration.json")]
            _registre = Registre(fichiers)
            log.info("Modèles de contrôle chargés — %d classes, %d types (%s)",
                     len(_registre.classes), len(_registre.datatypes),
                     ", ".join("{} {}".format(p.get("specification"), (p.get("commit") or "")[:7])
                               for p in _registre.provenance))
        return _registre


# ═════════════════════════════════════════════════════════════════════════════════════════════
# Objets du modèle
# ═════════════════════════════════════════════════════════════════════════════════════════════

class NcObject:
    """Racine de toutes les classes de contrôle (MS-05-02 §NcObject).

    Les valeurs vivent dans `self._vals`, keyées par le NOM de propriété tel que publié par le
    modèle. Une sous-classe qui calcule une propriété surcharge `_lire` ; une sous-classe qui
    réagit à une écriture surcharge `_ecrire`."""

    class_id = [1]

    def __init__(self, appareil, oid, role, owner, user_label=None,
                 touchpoints=None, constant_oid=False, description=None):
        self.appareil = appareil
        self.oid = oid
        self.role = role
        self.owner = owner
        self.constant_oid = constant_oid
        self.description = description
        self._vals = {"userLabel": user_label, "touchpoints": touchpoints}
        self._props = registre().proprietes(self.class_id)
        self._nom_de_pid = {k: v["name"] for k, v in self._props.items()}
        self._pid_de_nom = {v["name"]: k for k, v in self._props.items()}
        self._methodes = {}
        self._enregistrer_methodes()

    # ─── Chemin de rôles (NcRolePath) ────────────────────────────────────────────────────────
    def chemin(self):
        """NcRolePath : la suite des rôles depuis la racine, incluse. C'est l'adresse d'un objet
        en IS-14 (jointe par des points dans l'URL) et l'argument des méthodes de sauvegarde."""
        out, courant, garde = [], self, 0
        while courant is not None and garde < 64:
            out.append(courant.role)
            courant = self.appareil.objet(courant.owner) if courant.owner is not None else None
            garde += 1
        return list(reversed(out))

    # ─── Description en tant que membre d'un bloc ────────────────────────────────────────────
    def descripteur_membre(self):
        """NcBlockMemberDescriptor."""
        return {
            "description": self.description,
            "role": self.role,
            "oid": self.oid,
            "constantOid": self.constant_oid,
            "classId": list(self.class_id),
            "userLabel": self._vals.get("userLabel"),
            "owner": self.owner,
        }

    # ─── Accès aux propriétés ────────────────────────────────────────────────────────────────
    def _lire(self, nom):
        if nom == "classId":
            return list(self.class_id)
        if nom == "oid":
            return self.oid
        if nom == "constantOid":
            return self.constant_oid
        if nom == "owner":
            return self.owner
        if nom == "role":
            return self.role
        if nom == "runtimePropertyConstraints":
            return self._vals.get("runtimePropertyConstraints")
        return self._vals.get(nom)

    def _ecrire(self, nom, valeur):
        """Retourne un NcMethodResult. Le défaut range la valeur et notifie."""
        self._vals[nom] = valeur
        self.notifier(nom, valeur)
        return ok()

    def notifier(self, nom, valeur, change_type=VALUE_CHANGED, index=None):
        """Émet PropertyChanged (1e1) pour cette propriété."""
        pid = self._pid_de_nom.get(nom)
        if pid is None or self.appareil is None:
            return
        self.appareil.emettre(self.oid, {
            "propertyId": {"level": pid[0], "index": pid[1]},
            "changeType": change_type,
            "value": valeur,
            "sequenceItemIndex": index,
        })

    def poser(self, nom, valeur):
        """Écriture INTERNE (télémétrie, pas un contrôleur) : range et notifie si ça a changé.
        Retourne True si la valeur a effectivement bougé."""
        if self._vals.get(nom) == valeur and nom in self._vals:
            return False
        self._vals[nom] = valeur
        self.notifier(nom, valeur)
        return True

    # ─── Méthodes génériques (1m1…1m7) ───────────────────────────────────────────────────────
    def _enregistrer_methodes(self):
        self._methodes.update({
            (1, 1): self._m_get,
            (1, 2): self._m_set,
            (1, 3): self._m_get_sequence_item,
            (1, 4): self._m_set_sequence_item,
            (1, 5): self._m_add_sequence_item,
            (1, 6): self._m_remove_sequence_item,
            (1, 7): self._m_get_sequence_length,
        })

    def _resoudre(self, args, cle="id"):
        """(descripteur, nom, None) ou (None, None, résultat d'erreur)."""
        pid = (args or {}).get(cle)
        if not isinstance(pid, dict) or "level" not in pid or "index" not in pid:
            return None, None, erreur(PARAMETER_ERROR, "identifiant de propriété absent ou mal formé")
        cle_pid = (pid["level"], pid["index"])
        desc = self._props.get(cle_pid)
        if desc is None:
            return None, None, erreur(PROPERTY_NOT_IMPLEMENTED,
                                      "propriété %dp%d inconnue de %s"
                                      % (cle_pid[0], cle_pid[1], self.__class__.__name__))
        return desc, desc["name"], None

    def _m_get(self, args):
        desc, nom, err = self._resoudre(args)
        if err:
            return err
        return ok(self._lire(nom), avec_valeur=True)

    def _m_set(self, args):
        desc, nom, err = self._resoudre(args)
        if err:
            return err
        if desc.get("isReadOnly"):
            return erreur(READONLY, "propriété %s en lecture seule" % nom)
        valeur = (args or {}).get("value")
        if valeur is None and not desc.get("isNullable"):
            return erreur(PARAMETER_ERROR, "propriété %s non nullable" % nom)
        return self._ecrire(nom, valeur)

    def _sequence(self, desc, nom):
        if not desc.get("isSequence"):
            return None, erreur(INVALID_REQUEST, "propriété %s n'est pas une séquence" % nom)
        val = self._lire(nom)
        if val is None:
            return None, erreur(INVALID_REQUEST, "séquence %s absente" % nom)
        return list(val), None

    def _m_get_sequence_item(self, args):
        desc, nom, err = self._resoudre(args)
        if err:
            return err
        seq, err = self._sequence(desc, nom)
        if err:
            return err
        i = (args or {}).get("index")
        if not isinstance(i, int) or i < 0 or i >= len(seq):
            return erreur(INDEX_OUT_OF_BOUNDS, "index hors bornes pour %s" % nom)
        return ok(seq[i], avec_valeur=True)

    def _m_get_sequence_length(self, args):
        desc, nom, err = self._resoudre(args)
        if err:
            return err
        seq, err = self._sequence(desc, nom)
        if err:
            return err
        return ok(len(seq), avec_valeur=True)

    def _m_set_sequence_item(self, args):
        desc, nom, err = self._resoudre(args)
        if err:
            return err
        if desc.get("isReadOnly"):
            return erreur(READONLY, "séquence %s en lecture seule" % nom)
        return erreur(METHOD_NOT_IMPLEMENTED, "séquence %s non modifiable" % nom)

    def _m_add_sequence_item(self, args):
        desc, nom, err = self._resoudre(args)
        if err:
            return err
        if desc.get("isReadOnly"):
            return erreur(READONLY, "séquence %s en lecture seule" % nom)
        return erreur(METHOD_NOT_IMPLEMENTED, "séquence %s non modifiable" % nom)

    def _m_remove_sequence_item(self, args):
        desc, nom, err = self._resoudre(args)
        if err:
            return err
        if desc.get("isReadOnly"):
            return erreur(READONLY, "séquence %s en lecture seule" % nom)
        return erreur(METHOD_NOT_IMPLEMENTED, "séquence %s non modifiable" % nom)

    # ─── Invocation ──────────────────────────────────────────────────────────────────────────
    def invoquer(self, method_id, args):
        f = self._methodes.get(method_id)
        if f is None:
            return erreur(METHOD_NOT_IMPLEMENTED,
                          "méthode %dm%d inconnue de %s"
                          % (method_id[0], method_id[1], self.__class__.__name__))
        try:
            return f(args or {})
        except Exception as e:                      # jamais de trace nue vers le contrôleur
            log.exception("IS-12 : méthode %dm%d sur oid %s", method_id[0], method_id[1], self.oid)
            return erreur(DEVICE_ERROR, "erreur interne : %s" % e)


class NcWorker(NcObject):
    """MS-05-02 §NcWorker — un objet fonctionnel activable."""

    class_id = [1, 2]

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._vals.setdefault("enabled", True)


class NcBlock(NcObject):
    """MS-05-02 §NcBlock — conteneur d'objets."""

    class_id = [1, 1]

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self._vals["enabled"] = True
        self.membres = []            # liste ordonnée de NcObject

    def _enregistrer_methodes(self):
        super()._enregistrer_methodes()
        self._methodes.update({
            (2, 1): self._m_get_member_descriptors,
            (2, 2): self._m_find_members_by_path,
            (2, 3): self._m_find_members_by_role,
            (2, 4): self._m_find_members_by_class_id,
        })

    def _lire(self, nom):
        if nom == "members":
            return [m.descripteur_membre() for m in self.membres]
        return super()._lire(nom)

    def ajouter(self, obj):
        self.membres.append(obj)
        self.notifier("members", self._lire("members"))
        return obj

    def retirer(self, obj):
        if obj in self.membres:
            self.membres.remove(obj)
            self.notifier("members", self._lire("members"))

    def _descendants(self):
        for m in self.membres:
            yield m
            if isinstance(m, NcBlock):
                for d in m._descendants():
                    yield d

    def _m_get_member_descriptors(self, args):
        recurse = bool(args.get("recurse"))
        src = self._descendants() if recurse else iter(self.membres)
        return ok([m.descripteur_membre() for m in src], avec_valeur=True)

    def _m_find_members_by_path(self, args):
        chemin = args.get("path")
        if not isinstance(chemin, list) or not chemin:
            return erreur(PARAMETER_ERROR, "chemin de rôles absent ou vide")
        courant = self
        for role in chemin:
            if not isinstance(courant, NcBlock):
                return erreur(INVALID_REQUEST, "le chemin traverse un objet qui n'est pas un bloc")
            suivant = next((m for m in courant.membres if m.role == role), None)
            if suivant is None:
                return ok([], avec_valeur=True)
            courant = suivant
        return ok([courant.descripteur_membre()], avec_valeur=True)

    def _m_find_members_by_role(self, args):
        role = args.get("role")
        if not isinstance(role, str):
            return erreur(PARAMETER_ERROR, "rôle absent")
        sensible = bool(args.get("caseSensitive"))
        entier = bool(args.get("matchWholeString"))
        src = self._descendants() if bool(args.get("recurse")) else iter(self.membres)
        cible = role if sensible else role.lower()

        def _match(r):
            r = r if sensible else r.lower()
            return r == cible if entier else cible in r

        return ok([m.descripteur_membre() for m in src if _match(m.role)], avec_valeur=True)

    def _m_find_members_by_class_id(self, args):
        cid = args.get("classId")
        if not isinstance(cid, list) or not cid:
            return erreur(PARAMETER_ERROR, "classId absent")
        cid = tuple(cid)
        derives = bool(args.get("includeDerived"))
        src = self._descendants() if bool(args.get("recurse")) else iter(self.membres)

        def _match(m):
            mc = tuple(m.class_id)
            return mc[:len(cid)] == cid if derives else mc == cid

        return ok([m.descripteur_membre() for m in src if _match(m)], avec_valeur=True)


class NcManager(NcObject):
    class_id = [1, 3]


class NcDeviceManager(NcManager):
    """MS-05-02 §NcDeviceManager — identité du produit."""

    class_id = [1, 3, 1]

    def __init__(self, appareil, oid, owner, produit, fabricant, serial, device_name=None):
        super().__init__(appareil, oid, "DeviceManager", owner,
                         user_label="Device manager", constant_oid=True,
                         description="Identité et état opérationnel de l'appareil")
        self._vals.update({
            "ncVersion": "v1.0.0",
            "manufacturer": fabricant,
            "product": produit,
            "serialNumber": serial,
            "userInventoryCode": None,
            "deviceName": device_name,
            "deviceRole": None,
            "operationalState": {"generic": 1, "deviceSpecificDetails": None},   # NormalOperation
            "resetCause": 1,                                                     # PowerOn
            "message": None,
        })


class NcClassManager(NcManager):
    """MS-05-02 §NcClassManager — publie le modèle chargé depuis `nc_models/`."""

    class_id = [1, 3, 2]

    def __init__(self, appareil, oid, owner):
        super().__init__(appareil, oid, "ClassManager", owner,
                         user_label="Class manager", constant_oid=True,
                         description="Classes de contrôle et types de données pris en charge")

    def _enregistrer_methodes(self):
        super()._enregistrer_methodes()
        self._methodes.update({(3, 1): self._m_get_control_class,
                               (3, 2): self._m_get_datatype})

    def _lire(self, nom):
        if nom == "controlClasses":
            return registre().liste_classes()
        if nom == "datatypes":
            return registre().liste_types()
        return super()._lire(nom)

    def _m_get_control_class(self, args):
        cid = args.get("classId")
        if not isinstance(cid, list) or not cid:
            return erreur(PARAMETER_ERROR, "classId absent")
        d = registre().descripteur_classe(cid, inclure_herite=bool(args.get("includeInherited")))
        if d is None:
            return erreur(PARAMETER_ERROR, "classe %s inconnue" % (cid,))
        return ok(d, avec_valeur=True)

    def _m_get_datatype(self, args):
        nom = args.get("name")
        if not isinstance(nom, str) or not nom:
            return erreur(PARAMETER_ERROR, "nom de type absent")
        d = registre().descripteur_type(nom, inclure_herite=bool(args.get("includeInherited")))
        if d is None:
            return erreur(PARAMETER_ERROR, "type %s inconnu" % nom)
        return ok(d, avec_valeur=True)


class NcBulkPropertiesManager(NcManager):
    """Jeu de fonctionnalités « device configuration » — sauvegarde et restauration du modèle.

    C'est le moteur derrière l'endpoint `bulkProperties` d'IS-14, et il est publié comme un objet
    ordinaire : un contrôleur IS-12 peut donc sauvegarder et restaurer par les MÊMES méthodes. La
    logique n'existe qu'ici, les deux protocoles n'en sont que des façades."""

    class_id = [1, 3, 3]

    def __init__(self, appareil, oid, owner):
        super().__init__(appareil, oid, "BulkPropertiesManager", owner,
                         user_label="Bulk properties manager", constant_oid=True,
                         description="Sauvegarde et restauration des propriétés du modèle")

    def _enregistrer_methodes(self):
        super()._enregistrer_methodes()
        self._methodes.update({(3, 1): self._m_get_by_path,
                               (3, 2): self._m_validate_set_by_path,
                               (3, 3): self._m_set_by_path})

    # ─── Sauvegarde ──────────────────────────────────────────────────────────────────────────
    def empreinte(self):
        """`validationFingerprint` : ce qui permet à un contrôleur de reconnaître qu'un jeu de
        sauvegarde vient d'un modèle compatible. On y met le produit et sa révision — pas un
        horodatage, qui rendrait deux sauvegardes du même appareil artificiellement différentes."""
        p = self.appareil.device_manager._vals.get("product") or {}
        return "{}|{}".format(p.get("name") or "?", p.get("revisionLevel") or "?")

    def _portee(self, chemin, recurse):
        """Objets visés par une opération : celui du chemin, plus ses descendants si `recurse`."""
        cible = self.appareil.par_chemin(chemin)
        if cible is None:
            return None
        objets = [cible]
        if recurse and isinstance(cible, NcBlock):
            objets.extend(cible._descendants())
        return objets

    def _proprietes_de(self, obj, avec_descripteurs):
        holders = []
        for cle in sorted(obj._props):
            desc = obj._props[cle]
            holders.append({
                "id": {"level": cle[0], "index": cle[1]},
                "descriptor": desc if avec_descripteurs else None,
                "value": obj._lire(desc["name"]),
            })
        return {
            "path": obj.chemin(),
            "dependencyPaths": [],
            # Nos blocs ne se reconstruisent pas depuis un jeu de sauvegarde : leurs membres sont
            # dérivés du modèle NMOS, pas déclarés. On l'annonce plutôt que de le laisser deviner.
            "allowedMembersClasses": [],
            "values": holders,
            "isRebuildable": False,
        }

    def _m_get_by_path(self, args):
        chemin = args.get("path")
        if not isinstance(chemin, list) or not chemin:
            return erreur(PARAMETER_ERROR, "chemin de rôles absent")
        recurse = args.get("recurse", True)
        avec_desc = args.get("includeDescriptors", True)
        objets = self._portee(chemin, bool(recurse))
        if objets is None:
            return erreur(BAD_OID, "chemin de rôles inconnu : %s" % ".".join(map(str, chemin)))
        # Sans les descripteurs, la spec impose d'OMETTRE le ClassManager : ses deux propriétés
        # sont le modèle entier, et un jeu de sauvegarde n'a pas à le trimballer.
        if not avec_desc:
            objets = [o for o in objets if not isinstance(o, NcClassManager)]
        return ok({"validationFingerprint": self.empreinte(),
                   "values": [self._proprietes_de(o, bool(avec_desc)) for o in objets]},
                  avec_valeur=True)

    # ─── Restauration ────────────────────────────────────────────────────────────────────────
    def _restaurer_objet(self, obj, holder, mode, appliquer):
        """(entrée NcObjectPropertiesSetValidation) pour un objet et son lot de propriétés."""
        notices = []
        for ph in (holder.get("values") or []) if isinstance(holder, dict) else []:
            pid = ph.get("id") or {}
            cle = (pid.get("level"), pid.get("index"))
            desc = obj._props.get(cle)
            if desc is None:
                # Jeu de sauvegarde venu d'un modèle plus riche : on ignore, on le DIT, et on ne
                # fait pas échouer l'objet pour autant.
                notices.append(self._notice(pid, "?", NOTICE_WARNING,
                                            "Property not present on this object — ignored"))
                continue
            if desc.get("isReadOnly"):
                notices.append(self._notice(pid, desc["name"], NOTICE_WARNING,
                                            "Property is readonly"))
                continue
            if not appliquer:
                continue
            res = obj._ecrire(desc["name"], ph.get("value"))
            if (res or {}).get("status") != OK:
                notices.append(self._notice(pid, desc["name"], NOTICE_ERROR,
                                            (res or {}).get("errorMessage") or "Set failed"))
        if mode == RESTORE_REBUILD:
            # La spec impose d'ACCEPTER un restore « Rebuild » même sans objet reconstructible,
            # en se limitant aux propriétés modifiables et en signalant le reste.
            notices.append(self._notice({"level": 1, "index": 1}, "classId", NOTICE_WARNING,
                                        "Rebuild not supported by this device — only writeable "
                                        "properties were considered"))
        echec = any(n["noticeType"] == NOTICE_ERROR for n in notices)
        return {"path": obj.chemin(),
                "status": RESTORE_FAILED if echec else RESTORE_OK,
                "notices": notices,
                "statusMessage": "Au moins une propriété n'a pas pu être appliquée" if echec else None}

    @staticmethod
    def _notice(pid, nom, type_, message):
        return {"id": {"level": pid.get("level"), "index": pid.get("index")},
                "name": nom, "noticeType": type_, "noticeMessage": message}

    def _restaurer(self, args, appliquer):
        chemin = args.get("path")
        if not isinstance(chemin, list) or not chemin:
            return erreur(PARAMETER_ERROR, "chemin de rôles absent")
        jeu = args.get("dataSet")
        if not isinstance(jeu, dict):
            return erreur(PARAMETER_ERROR, "dataSet absent ou mal formé")
        mode = args.get("restoreMode", RESTORE_MODIFY)
        if mode not in (RESTORE_MODIFY, RESTORE_REBUILD):
            return erreur(PARAMETER_ERROR, "restoreMode inconnu")
        objets = self._portee(chemin, bool(args.get("recurse", True)))
        if objets is None:
            return erreur(BAD_OID, "chemin de rôles inconnu : %s" % ".".join(map(str, chemin)))
        par_chemin = {tuple(h.get("path") or []): h for h in (jeu.get("values") or [])
                      if isinstance(h, dict)}
        # Une entrée PAR OBJET DE LA PORTÉE, même absente du jeu (exigence de la spec) : le
        # contrôleur doit pouvoir constater ce qui n'a PAS été restauré, pas seulement ce qui l'a été.
        sorties = []
        for o in objets:
            h = par_chemin.get(tuple(o.chemin()))
            if h is None:
                sorties.append({"path": o.chemin(), "status": RESTORE_NOT_FOUND, "notices": [],
                                "statusMessage": "Aucune donnée pour ce chemin dans la sauvegarde"})
            else:
                sorties.append(self._restaurer_objet(o, h, mode, appliquer))
        return ok(sorties, avec_valeur=True)

    def _m_validate_set_by_path(self, args):
        return self._restaurer(args, appliquer=False)

    def _m_set_by_path(self, args):
        return self._restaurer(args, appliquer=True)


# ═════════════════════════════════════════════════════════════════════════════════════════════
# Appareil = arbre + registre d'oids + diffusion des notifications
# ═════════════════════════════════════════════════════════════════════════════════════════════

class Appareil:
    """Modèle d'appareil complet. Les sessions IS-12 s'y abonnent via `abonner`/`desabonner`."""

    def __init__(self, produit, fabricant, serial, device_name=None):
        self._lock = threading.RLock()
        self._objets = {}                    # oid → NcObject
        self._oid_suivant = _OID_PREMIER_LIBRE
        self._abonnes = {}                   # session → set(oid)
        self.racine = NcBlock(self, OID_ROOT, "root", None, user_label="Root block",
                              constant_oid=True, description="Bloc racine de l'appareil")
        self._inscrire(self.racine)
        self.device_manager = NcDeviceManager(self, OID_DEVICE_MANAGER, OID_ROOT,
                                              produit, fabricant, serial, device_name)
        self.class_manager = NcClassManager(self, OID_CLASS_MANAGER, OID_ROOT)
        self.bulk_manager = NcBulkPropertiesManager(self, OID_BULK_MANAGER, OID_ROOT)
        for m in (self.device_manager, self.class_manager, self.bulk_manager):
            self._inscrire(m)
            self.racine.membres.append(m)

    # ─── Registre d'objets ───────────────────────────────────────────────────────────────────
    def _inscrire(self, obj):
        self._objets[obj.oid] = obj

    def oid_libre(self):
        with self._lock:
            oid = self._oid_suivant
            self._oid_suivant += 1
            return oid

    def ajouter(self, bloc, obj):
        """Ajoute `obj` au bloc `bloc` et l'inscrit au registre d'oids."""
        with self._lock:
            self._inscrire(obj)
        bloc.ajouter(obj)
        return obj

    def retirer(self, bloc, obj):
        bloc.retirer(obj)
        with self._lock:
            self._objets.pop(obj.oid, None)
            for oids in self._abonnes.values():
                oids.discard(obj.oid)

    def objet(self, oid):
        return self._objets.get(oid)

    # ─── Adressage par chemin de rôles (NcRolePath — c'est l'adresse d'IS-14) ─────────────────
    def par_chemin(self, chemin):
        """Objet désigné par un NcRolePath (liste de rôles depuis la racine), ou None."""
        if not isinstance(chemin, (list, tuple)) or not chemin:
            return None
        if chemin[0] != self.racine.role:
            return None
        courant = self.racine
        for role in chemin[1:]:
            if not isinstance(courant, NcBlock):
                return None
            courant = next((m for m in courant.membres if m.role == role), None)
            if courant is None:
                return None
        return courant

    def chemins(self):
        """Tous les chemins de rôles du modèle, racine comprise, dans l'ordre de l'arbre."""
        return [self.racine.chemin()] + [o.chemin() for o in self.racine._descendants()]

    def bloc(self, role, description=None):
        """Sous-bloc de la racine, créé au besoin (idempotent)."""
        with self._lock:
            existant = next((m for m in self.racine.membres
                             if m.role == role and isinstance(m, NcBlock)), None)
            if existant is not None:
                return existant
            b = NcBlock(self, self.oid_libre(), role, OID_ROOT,
                        user_label=role, description=description)
        return self.ajouter(self.racine, b)

    # ─── Commandes ───────────────────────────────────────────────────────────────────────────
    def commander(self, oid, method_id, args):
        obj = self._objets.get(oid)
        if obj is None:
            return erreur(BAD_OID, "objet %s inconnu" % oid)
        return obj.invoquer(method_id, args)

    # ─── Abonnements et notifications ────────────────────────────────────────────────────────
    def abonner(self, session, oids):
        """Retourne la liste des oids RÉELLEMENT abonnés (la spec impose de filtrer les invalides).
        Un message Subscription REMPLACE la liste d'abonnements de la session."""
        with self._lock:
            valides = {o for o in oids if isinstance(o, int) and o in self._objets}
            self._abonnes[session] = valides
            return sorted(valides)

    def desabonner(self, session):
        with self._lock:
            self._abonnes.pop(session, None)

    def emettre(self, oid, event_data):
        """Diffuse un PropertyChanged aux sessions abonnées à cet oid."""
        with self._lock:
            cibles = [s for s, oids in self._abonnes.items() if oid in oids]
        if not cibles:
            return
        msg = {"messageType": 2,
               "notifications": [{"oid": oid, "eventId": EVENT_PROPERTY_CHANGED,
                                  "eventData": event_data}]}
        for s in cibles:
            try:
                s.envoyer(msg)
            except Exception as e:                  # une session morte ne casse pas les autres
                log.debug("IS-12 : notification non délivrée (%s)", e)
