# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""NMOS IS-04 (Node API) + IS-05 (Connection API) — provider centralisé orchestrateur.

Phase 1 : ossature complète (modèle Node/Device/Receiver, endpoints REST,
client de registration RDS, activation → forward SDP à l'agent).
L'intégration ffmpeg réelle (côté receiver.py + agent multicast) sera Phase 2.

Modèle :
- 1 Node = l'orchestrateur (UUID stable persisté)
- 1 Device par container managé par NMOS
- N Receivers par container (lu depuis deploy_config.params.video_count/audio_count)
- SMPTE 2022-7 : flag smpte_2022_7 dans params → receivers exposés avec 2 transport_params legs
- Au PATCH IS-05 staged.activation.mode == "activate_immediate", on parse le SDP
  de transport_file et on POST le multicast info à l'agent du container.

Settings (table `settings`) :
- nmos_enabled (bool)
- nmos_registry_url (string) — ex http://10.x:8235
- nmos_node_label (string)
- nmos_node_description (string)
- nmos_host_address (string) — IP/host annoncée comme href du Node
"""
import json
from app.numerotation import slot_tx, slot_rx, numero, indice, cle_tx_shm
import logging
import re
import socket
import threading
import time
import uuid

import requests
from flask import Blueprint, jsonify, request, abort

log = logging.getLogger(__name__)

IS04_VERSION = "v1.3"
IS05_VERSION = "v1.1"
HEARTBEAT_S = 5.0
# Période du FILET de reconstruction (cf. _register_loop) : assez rare pour être gratuite,
# assez fréquente pour qu'une notification oubliée ne laisse pas le modèle périmé longtemps.
REBUILD_FILET_S = 60.0

bp = Blueprint("nmos", __name__)


# ═════════════════════════════════════════════════════════════════════
# CORS — exigé par IS-04 sur TOUTES les réponses des APIs NMOS
# ═════════════════════════════════════════════════════════════════════
# Un contrôleur NMOS est très souvent une page web servie depuis un AUTRE hôte : sans ces
# en-têtes le navigateur refuse la réponse, et l'API paraît muette alors qu'elle répond 200.
# Invisible en curl, invisible dans nos bancs, fatal pour un contrôleur tiers — c'est
# exactement ce que la suite AMWA a trouvé le 2026-08-31 (quatorze tests sur un seul défaut).
#
# ⚠ Posé au niveau de l'APPLICATION, pas du blueprint. Un `bp.after_request` ne voit que les
# vues du blueprint : un 404 sur un chemin `/x-nmos/` inconnu est rendu par Flask, hors
# blueprint, et repartait donc SANS en-tête et en text/html. Or c'est une réponse d'API comme
# une autre, et la suite la teste.

_CORS_METHODES = "GET, PUT, POST, HEAD, OPTIONS, DELETE, PATCH"
_CORS_ENTETES = "Content-Type, Accept, Authorization"


def _est_nmos(chemin):
    return (chemin or "").startswith("/x-nmos/")


def installer_cors(app):
    """Pose les en-têtes CORS et le rendu JSON des erreurs sur les seuls chemins `/x-nmos/`.

    Bornée aux chemins NMOS À DESSEIN : ouvrir le reste de l'orchestrateur en `Allow-Origin: *`
    exposerait des routes authentifiées par cookie aux requêtes d'un autre site."""
    from flask import request as _rq, jsonify as _js

    @app.after_request
    def _cors(rep):
        if _est_nmos(_rq.path):
            rep.headers.setdefault("Access-Control-Allow-Origin", "*")
            rep.headers.setdefault("Access-Control-Allow-Headers", _CORS_ENTETES)
            rep.headers.setdefault("Access-Control-Allow-Methods", _CORS_METHODES)
            rep.headers.setdefault("Access-Control-Max-Age", "3600")
        return rep

    @app.errorhandler(404)
    def _404(_e):
        # Hors NMOS on rend le 404 habituel : cette fonction est globale, elle ne doit pas
        # transformer les erreurs de l'interface web en JSON.
        if not _est_nmos(_rq.path):
            return "Not Found", 404
        return _js({"code": 404, "error": "resource not found", "debug": None}), 404

# ═════════════════════════════════════════════════════════════════════
# État global (singleton process-level)
# ═════════════════════════════════════════════════════════════════════

_lock = threading.RLock()
_running = False
_register_thread = None
_mdns_zc = None         # zeroconf instance
_mdns_services = []     # ServiceInfo enregistrés (_nmos-node, + register/query si le registre est ouvert)
_state = {
    "node_id": None,
    "node_started_unix": int(time.time()),
    "registry_url": None,
    "registered": False,
    "last_register_error": None,
    "last_heartbeat_unix": None,
    "mdns_active": False,
}

# In-memory model. Persisté en DB via les helpers ci-dessous.
_devices = {}        # device_id → dict (NMOS resource)
_receivers = {}      # receiver_id → dict
_recv_state = {}     # receiver_id → {"staged": {...}, "active": {...}}
_senders = {}        # sender_id → dict (NMOS resource, populé pour sender_2110)
_sources = {}        # source_id → dict
_flows = {}          # flow_id → dict
_send_state = {}     # sender_id → {"staged": {...}, "active": {...}, "vmid": ...}
_last_mcast_conflict_sig = ""   # B2-3 : anti-spam de l'alerte de collision multicast

# ═════════════════════════════════════════════════════════════════════
# Helpers : versionning TAI, IDs stables
# ═════════════════════════════════════════════════════════════════════

def _tai_version():
    """TAI version au format '<seconds>:<nanoseconds>' — bonne approximation via time.time_ns()."""
    ns = time.time_ns()
    return f"{ns // 1_000_000_000}:{ns % 1_000_000_000:09d}"

def _stable_uuid(seed):
    """Génère un UUID stable à partir d'une seed (vmid + index, etc.) via UUID5 dans un namespace fixe."""
    return str(uuid.uuid5(uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8"), str(seed)))

def _get_node_id():
    """Charge ou crée l'UUID stable du Node (persisté en settings)."""
    from app.database import db_get_setting, db_set_setting
    nid = db_get_setting("nmos_node_uuid", None)
    if not nid:
        nid = str(uuid.uuid4())
        db_set_setting("nmos_node_uuid", nid)
    return nid

def _get_host_address():
    from app.database import db_get_setting
    host = db_get_setting("nmos_host_address", None)
    if host:
        return host
    # Best-effort : IP locale via socket trick
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

# ═════════════════════════════════════════════════════════════════════
# Construction du modèle NMOS depuis l'état DB
# ═════════════════════════════════════════════════════════════════════

def _empty_transport_params(n=1):
    """transport_params NMOS IS-05 pour un receiver RTP single-leg (n=1) ou redundant (n=2)."""
    return [{
        "rtp_enabled": True,
        "source_ip": None,
        "interface_ip": "auto",
        "multicast_ip": None,
        "destination_port": None,
    } for _ in range(n)]

def _empty_staged(n_legs=1):
    return {
        "sender_id": None,
        "master_enable": False,
        "transport_params": _empty_transport_params(n_legs),
        "activation": {"mode": None, "requested_time": None, "activation_time": None},
        "transport_file": {"data": None, "type": None},
    }

def _empty_sender_staged(mcast_ip, port, leg1=None):
    """transport_params IS-05 d'un Sender RTP multicast. master_enable=True par défaut.
    leg1=(mcast_ip1, port1) active SMPTE 2022-7 (2 legs transport_params)."""
    legs = [{
        "rtp_enabled": True,
        "source_ip": None,
        "destination_ip": mcast_ip,
        "destination_port": int(port),
    }]
    if leg1:
        legs.append({
            "rtp_enabled": True,
            "source_ip": None,
            "destination_ip": leg1[0],
            "destination_port": int(leg1[1]),
        })
    return {
        "receiver_id": None,
        "master_enable": True,
        "transport_params": legs,
        "activation": {"mode": None, "requested_time": None, "activation_time": None},
    }


def _build_source_resource(sid, did, vmid, label, version):
    return {
        "id": sid,
        "version": version,
        "label": f"{label} source",
        "description": f"Source video sender container {vmid}",
        "tags": {"urn:x-mxl:vmid": [str(vmid)]},
        "device_id": did,
        "parents": [],   # IS-04 v1.3 : required array (sources mères, vide si racine)
        "format": "urn:x-nmos:format:video",
        "caps": {},
        "clock_name": "clk0",
        "grain_rate": {"numerator": 25, "denominator": 1},
    }


def _build_flow_resource(fid, did, src_id, vmid, label, width, height, version, chroma="422",
                         bit_depth=10, colorspace="BT709", transfer="SDR", scan="p", fo=""):
    from app.scripts import CHROMA_DIV, normalize_bit_depth, nmos_interlace_mode
    cw, ch = CHROMA_DIV.get(str(chroma), CHROMA_DIV["422"])
    half_w   = max(1, width // cw)
    chroma_h = max(1, height // ch)
    bd = normalize_bit_depth(bit_depth)
    # grain_rate = cadence TRAME : pour 1080i50 (entrelacé) c'est 25, pas 50 (= la cadence champ).
    return {
        "id": fid,
        "version": version,
        "label": f"{label} flow",
        "description": f"Flow video raw 2110-20 sender container {vmid}",
        "tags": {"urn:x-mxl:vmid": [str(vmid)]},
        "device_id": did,
        "source_id": src_id,
        "parents": [],   # IS-04 v1.3 : required array (flows mères, vide si racine)
        "format": "urn:x-nmos:format:video",
        "media_type": "video/raw",
        "grain_rate": {"numerator": 25, "denominator": 1},
        "frame_width": int(width),
        "frame_height": int(height),
        "interlace_mode": nmos_interlace_mode({"scan": scan, "field_order": fo, "height": height}),
        "colorspace": colorspace,
        "transfer_characteristic": transfer,
        "components": [
            {"name": "Y",  "width": int(width),  "height": int(height), "bit_depth": bd},
            {"name": "Cb", "width": half_w,      "height": chroma_h,    "bit_depth": bd},
            {"name": "Cr", "width": half_w,      "height": chroma_h,    "bit_depth": bd},
        ],
    }


def _build_sender_resource(snd_id, did, fid, vmid, label, version, legs=1):
    host = _get_host_address()
    return {
        "id": snd_id,
        "version": version,
        "label": label,
        "description": f"Sender 2110-20 container {vmid}",
        "tags": {"urn:x-mxl:vmid": [str(vmid)]},
        "device_id": did,
        "flow_id": fid,
        "transport": "urn:x-nmos:transport:rtp.mcast",
        "manifest_href": f"http://{host}:5000/x-nmos/connection/{IS05_VERSION}/single/senders/{snd_id}/transportfile",
        "interface_bindings": _iface_bindings(vmid, legs),
        "subscription": {"receiver_id": None, "active": False},
        "caps": {},
    }


# ─── BCP-004-01 « Receiver Capabilities » ────────────────────────────────────────────────────
# Release v1.0.0 (tag vérifié le 2026-08-15 — contrairement à BCP-007-03, celle-ci est publiée).
# Sans `constraint_sets`, nos receivers n'annoncent que `caps.media_types` : un contrôleur ne peut
# pas savoir si un Rx accepte du 1080p50 ou du 2160p59.94, il TENTE la connexion et découvre après
# coup. Avec, il le sait avant de câbler — et il peut le montrer dans sa grille de commutation.
#
# ★ D'où viennent les valeurs : du réglage `video_formats` (Réglages → Vidéo), source UNIQUE des
# formats du site, lue par `app.video_formats`. RIEN n'est codé en dur ici — un site qui déclare
# d'autres formats voit ses receivers les annoncer, et un format retiré des réglages disparaît des
# capacités. Annoncer une liste figée serait pire que ne rien annoncer : un contrôleur REFUSERAIT
# de câbler une source parfaitement valide.
_CAP = "urn:x-nmos:cap:"


_cs_video_cache = {}        # texte brut du réglage → liste de Constraint Sets déjà construite


def _video_constraint_sets():
    """Un Constraint Set par format vidéo déclaré au site. Liste vide si le réglage est vide —
    l'appelant n'annonce alors PAS `constraint_sets` du tout (cf. `_build_receiver_resource`).

    MÉMOÏSÉ sur le texte du réglage, pour deux raisons de fond et pas seulement de coût : un
    moteur 2110 publie ~16 receivers vidéo, et sans cache (a) on relisait et re-parsait le réglage
    seize fois par annonce, (b) surtout, l'avertissement sur un format incohérent sortait SEIZE
    FOIS par cycle d'annonce, indéfiniment. Un journal qui répète la même anomalie des milliers de
    fois ne la signale plus, il la noie — et c'est ce même mécanisme qui a déjà mangé la rétention
    des alertes sur ce projet. Le réglage change ⇒ la clé change ⇒ on re-parse et on re-signale."""
    from app import video_formats as _vf
    from app import settings as _s
    brut = _s.get("video_formats") or ""
    if brut in _cs_video_cache:
        return _cs_video_cache[brut]
    sets = []
    for f in _vf.formats(brut):
        souci = _vf.anomalie(f)
        if souci:
            # On n'annonce pas une capacité bâtie sur une ligne incohérente, et on le DIT : un
            # format simplement absent des capacités ferait refuser la source par le contrôleur,
            # sans que personne ne sache pourquoi (cf. le rejet silencieux qu'on veut éviter).
            log.warning("BCP-004-01 : format ignoré dans les capacités des receivers — %s", souci)
            continue
        num, den = _vf.frame_rate(f)
        cs = {
            f"{_CAP}meta:label":            f["label"],
            f"{_CAP}format:media_type":     {"enum": ["video/raw"]},
            f"{_CAP}format:frame_width":    {"enum": [f["w"]]},
            f"{_CAP}format:frame_height":   {"enum": [f["h"]]},
            f"{_CAP}format:grain_rate":     {"enum": [{"numerator": num, "denominator": den}]},
            f"{_CAP}format:interlace_mode": {"enum": [_vf.interlace_mode(f)]},
            f"{_CAP}format:color_sampling": {"enum": [_vf.color_sampling(f)]},
            f"{_CAP}format:component_depth": {"enum": [f["bit_depth"]]},
        }
        espace = _vf.colorspace(f)
        if espace:
            cs[f"{_CAP}format:colorspace"] = {"enum": [espace]}
        # `transfer_characteristic` : DÉLIBÉRÉMENT absent. Le réglage ne porte pas la courbe de
        # transfert, et une contrainte omise vaut « pas de contrainte » là où une contrainte
        # devinée (« SDR ») ferait rejeter une source HLG ou PQ par ailleurs acceptable.
        sets.append(cs)
    _cs_video_cache.clear()          # borne le cache : un seul réglage vit à la fois
    _cs_video_cache[brut] = sets
    return sets


def _audio_constraint_sets():
    """Constraint Set audio. Valeurs prises sur le moteur, pas sur le réglage : `A_CHANNELS = 8`,
    L24/48 kHz sont FIXES dans `plugins/2110_io/docker/controller.py` (le RX écrit du 8 canaux L24
    tel quel dans le shm). D'où un `enum` à 8 et non un `maximum` : le moteur ne sait pas recevoir
    autre chose, annoncer « au plus 8 » laisserait croire qu'un flux 2 canaux passerait.

    `packet_time` n'est PAS annoncé : `A_PTIME_DEF` est surchargeable par variable d'environnement
    (`AUDIO_PTIME`), donc le contrôleur ne peut pas en faire une vérité de site."""
    return [{
        f"{_CAP}meta:label":            "ST 2110-30 — 8 canaux L24 48 kHz",
        f"{_CAP}format:media_type":     {"enum": ["audio/L24"]},
        f"{_CAP}format:channel_count":  {"enum": [8]},
        f"{_CAP}format:sample_rate":    {"enum": [{"numerator": 48000, "denominator": 1}]},
    }]


def _receiver_caps(fmt, media_types):
    """`caps` d'un receiver — avec `constraint_sets` + `version` quand on a quelque chose de VRAI
    à déclarer (BCP-004-01 exige les deux ensemble, jamais l'un sans l'autre).

    Le format `data` (ANC, `video/smpte291`) n'a pas de contrainte exprimable dans le registre des
    capacités : on s'en tient à `media_types`, plutôt que d'inventer un Constraint Set vide qui
    n'apprendrait rien à personne."""
    caps = {"media_types": media_types}
    sets = {"video": _video_constraint_sets, "audio": _audio_constraint_sets}.get(fmt)
    sets = sets() if sets else []
    if sets:
        caps["constraint_sets"] = sets
        # `version` de caps : horodatage TAI du dernier changement des capacités. Il suit ici la
        # version de la ressource — nos capacités ne bougent que quand les réglages bougent, et un
        # changement de réglage passe par une ré-annonce complète.
        caps["version"] = _tai_version()
    return caps


def _build_receiver_resource(rid, did, vmid, recv_idx, label, version, fmt="video", legs=1):
    media_types = {"video": ["video/raw"], "audio": ["audio/L24"], "data": ["video/smpte291"]}[fmt]
    return {
        "id": rid,
        "version": version,
        "label": label,
        "description": f"Receiver {fmt} #{recv_idx + 1} sur container {vmid}",
        "tags": {"urn:x-mxl:vmid": [str(vmid)], "urn:x-mxl:receiver_index": [str(recv_idx)]},
        "device_id": did,
        "transport": "urn:x-nmos:transport:rtp.mcast",
        "format": f"urn:x-nmos:format:{fmt}",
        "subscription": {"sender_id": None, "active": False},
        "caps": _receiver_caps(fmt, media_types),
        "interface_bindings": _iface_bindings(vmid, legs),
    }


def _build_audio_source_resource(sid, did, vmid, label, version):
    return {
        "id": sid, "version": version,
        "label": f"{label} source",
        "description": f"Source audio sender container {vmid}",
        "tags": {"urn:x-mxl:vmid": [str(vmid)]},
        "device_id": did,
        "parents": [],   # IS-04 v1.3 : required array
        "format": "urn:x-nmos:format:audio",
        "caps": {},
        "clock_name": "clk0",
        "grain_rate": {"numerator": 48000, "denominator": 1},
        "channels": [{"label": f"ch{i}"} for i in range(8)],
    }


def _build_audio_flow_resource(fid, did, src_id, vmid, label, version):
    return {
        "id": fid, "version": version,
        "label": f"{label} flow",
        "description": f"Flow audio 2110-30 (L24/48k/8ch) container {vmid}",
        "tags": {"urn:x-mxl:vmid": [str(vmid)]},
        "device_id": did,
        "source_id": src_id,
        "parents": [],   # IS-04 v1.3 : required array
        "format": "urn:x-nmos:format:audio",
        "media_type": "audio/L24",
        "sample_rate": {"numerator": 48000, "denominator": 1},
        "bit_depth": 24,
    }

def _build_data_source_resource(sid, did, vmid, label, version):
    return {
        "id": sid, "version": version,
        "label": f"{label} source",
        "description": f"Source ANC (2110-40) sender container {vmid}",
        "tags": {"urn:x-mxl:vmid": [str(vmid)]},
        "device_id": did,
        "parents": [],   # IS-04 v1.3 : required array
        "format": "urn:x-nmos:format:data",
        "caps": {},
        "clock_name": "clk0",
    }


def _build_data_flow_resource(fid, did, src_id, vmid, label, version):
    return {
        "id": fid, "version": version,
        "label": f"{label} flow",
        "description": f"Flow ANC 2110-40 (SMPTE 291) container {vmid}",
        "tags": {"urn:x-mxl:vmid": [str(vmid)]},
        "device_id": did,
        "source_id": src_id,
        "parents": [],   # IS-04 v1.3 : required array
        "format": "urn:x-nmos:format:data",
        "media_type": "video/smpte291",
    }


def _registry_id(current_seed, instance_uuid, slot_key, essence, kind,
                 label="", transport=None, group_name="", role="", bind_override=None,
                 allow_autoseed=True):
    """C2a : id NMOS STABLE depuis le registre cluster (table nmos_resources), découplé du vmid.
    Lookup par (instance_uuid, slot_key, essence, kind) :
      - 1re fois (registre vide pour ce slot) → id = formule ACTUELLE (`_stable_uuid(current_seed)`,
        seedée vmid) → PRÉSERVE l'UUID existant (abonnements IS-05 intacts) ;
      - ensuite → id stable retrouvé via l'instance_uuid (survit recreate/projet, vmid changé).
    Rafraîchit aussi label/transport/binding (le slot peut changer de conteneur). Sans instance_uuid
    (ne devrait pas arriver post-C1) → fallback formule actuelle, hors registre.
    Renvoie `(id, label_effectif)` : si l'op a figé le libellé (label_locked), `label_effectif` est
    celui du registre → le caller construit la ressource avec, pas avec le hostname du conteneur.
    Mode STATIQUE (`allow_autoseed=False`, réglage cluster `nmos_mode=static`) : un slot SANS
    binding explicite ni ressource déjà au registre n'émet RIEN → renvoie `(None, label)` (le caller
    saute le slot). Les ressources déjà servies/bindées sont inchangées (pas de coupure)."""
    if not instance_uuid:
        return _stable_uuid(current_seed), label
    from app.database import (db_nmos_resource_get_by_bind, db_nmos_resource_upsert,
                              db_nmos_resource_get, db_nmos_resource_rebind)
    # C2b+ : rebinding EXPLICITE — ce slot sert une ressource précise du registre (override op). La
    # ressource fait AUTORITÉ : on ne touche pas son transport, on ré-écrit juste son binding sur ce
    # conteneur/slot (résolution « servi par ») et on renvoie SON libellé + son UUID. Override périmé
    # (ressource supprimée) → on retombe sur l'auto.
    if bind_override:
        ov = db_nmos_resource_get(bind_override)
        if ov and ov.get("kind") == kind and ov.get("essence") == essence:
            db_nmos_resource_rebind(bind_override, instance_uuid, slot_key)
            return bind_override, ov.get("label") or label
    ex = db_nmos_resource_get_by_bind(instance_uuid, slot_key, essence, kind)
    # Mode statique : pas d'auto-création. Un slot non bindé et absent du registre n'émet pas de
    # ressource (le caller fait `continue`). Une ressource déjà seedée (`ex`) reste servie.
    if ex is None and not allow_autoseed:
        return None, label
    rid = ex["id"] if ex else _stable_uuid(current_seed)
    # C2b : si le libellé a été figé par l'op (label_locked), on le PRÉSERVE (ne pas réécrire avec le
    # hostname du conteneur) ; sinon il suit le conteneur servant.
    eff_label = ex["label"] if (ex and ex.get("label_locked")) else label
    db_nmos_resource_upsert(rid, kind, essence, eff_label, group_name, role,
                            transport or {}, instance_uuid, slot_key)
    # Grouping IMMUABLE : la valeur du registre fait foi dès qu'elle existe (l'upsert ci-dessus ne
    # l'écrase plus). On la relaie à `_set_grouphint` par `_grouphint_fige` plutôt que par un
    # troisième retour, pour ne pas changer la signature sur la quinzaine d'appelants.
    _grouphint_fige[rid] = ((ex.get("group_name") or group_name) if ex else group_name,
                            (ex.get("role") or role) if ex else role)
    return rid, eff_label

def _build_cluster_device_resource(did, version):
    """C2a : Device de NIVEAU CLUSTER possédant TOUTES les ressources du registre, stable et
    indépendant des conteneurs. Le contrôleur voit une seule I/O 2110 quels que soient les conteneurs
    (ou nœuds) qui servent. Remplace les Devices par-conteneur (seedés sur le vmid)."""
    return {
        "id": did,
        "version": version,
        "label": _setting("nmos_cluster_label", "Bobi.Studio 2110 I/O"),
        "description": "Cluster 2110 I/O — ressources stables, conteneurs bindés via instance_uuid",
        "tags": _asset_tags(avec_fonction=True),      # BCP-002-02 (Device : + fonction)
        "type": "urn:x-nmos:device:generic",
        "node_id": _state["node_id"],
        "senders": [],
        "receivers": [],
        "controls": _controls(),
    }


# ─── BCP-002-02 : information distinctive d'asset ────────────────────────────────────────────
# Le problème que la BCP décrit mot pour mot : une requête IS-04 rend plusieurs Nodes au même
# libellé, et l'ingénieur d'exploitation n'a aucun moyen de savoir lequel est lequel. Trois tags
# sur le Node ET le Device (exactement une valeur chacun), plus la FONCTION sur le Device.
TAG_MANUFACTURER = "urn:x-nmos:tag:asset:manufacturer/v1.0"
TAG_PRODUCT      = "urn:x-nmos:tag:asset:product/v1.0"
TAG_INSTANCE_ID  = "urn:x-nmos:tag:asset:instance-id/v1.0"
TAG_FUNCTION     = "urn:x-nmos:tag:asset:function/v1.0"

ASSET_MANUFACTURER_DEFAUT = "BOBI SAS"
ASSET_PRODUCT_DEFAUT      = "Bobi.Studio"
ASSET_FUNCTION_DEFAUT     = "Gateway"


def asset_info():
    """Identité de l'asset — SOURCE UNIQUE, partagée par IS-04 (tags BCP-002-02) et IS-12
    (`NcDeviceManager`, propriétés `manufacturer`/`product`/`serialNumber`).

    Les deux protocoles publient la même information à des contrôleurs qui peuvent lire les deux :
    les servir depuis deux endroits, c'est se garantir qu'ils finiront par se contredire.

    `instance_id` = l'UUID de Node déjà persisté (`nmos_node_uuid`) : identifiant d'instance stable,
    unique par installation, et qui ne demande rien de plus à l'exploitant. La BCP exige que le
    triplet (fabricant, produit, instance) soit unique parmi les Nodes et parmi les Devices — il
    l'est par construction, et elle autorise explicitement le Device à partager celui de son Node."""
    return {
        "manufacturer": _setting("nmos_asset_manufacturer", ASSET_MANUFACTURER_DEFAUT)
                        or ASSET_MANUFACTURER_DEFAUT,
        "product":      _setting("nmos_asset_product", ASSET_PRODUCT_DEFAUT) or ASSET_PRODUCT_DEFAUT,
        "instance_id":  _get_node_id(),
        "function":     _setting("nmos_asset_function", ASSET_FUNCTION_DEFAUT)
                        or ASSET_FUNCTION_DEFAUT,
    }


def _asset_tags(avec_fonction=False, base=None):
    """Tags BCP-002-02 fusionnés dans `base` (tags déjà présents sur la ressource)."""
    a = asset_info()
    tags = dict(base or {})
    tags[TAG_MANUFACTURER] = [a["manufacturer"]]
    tags[TAG_PRODUCT]      = [a["product"]]
    tags[TAG_INSTANCE_ID]  = [a["instance_id"]]
    if avec_fonction:
        tags[TAG_FUNCTION] = [a["function"]]
    return tags


def _controls():
    """Tableau `controls` d'un Device IS-04 : la Connection API IS-05, plus le point d'accès
    IS-12 quand il tourne. C'est par là — et seulement par là — qu'un contrôleur découvre le
    protocole de contrôle (IS-12 §« IS-04 interactions »)."""
    ctrl = [{
        "href": f"http://{_get_host_address()}:5000/x-nmos/connection/{IS05_VERSION}/",
        "type": f"urn:x-nmos:control:sr-ctrl/{IS05_VERSION}",
    }]
    try:
        from . import is12
        if is12.actif():
            ctrl.append({"href": is12.href(), "type": is12.TYPE_CONTROL})
    except Exception as e:
        log.debug("nmos: control IS-12 non annoncé (%s)", e)
    try:
        from . import is14
        if is14.actif():
            ctrl.append({"href": is14.href(), "type": is14.TYPE_CONTROL})
    except Exception as e:
        log.debug("nmos: control IS-14 non annoncé (%s)", e)
    return ctrl


def _build_orphan_resources(row, version, did):
    """C2b : reconstruit une ressource NMOS INACTIVE depuis une ligne du registre dont aucun conteneur
    live ne la sert (orpheline). Transport figé depuis le registre, rattachée au Device cluster, vmid
    None. Renvoie un dict {senders/receivers/sources/flows: {id: resource}} à fusionner."""
    rid = row["id"]; essence = row.get("essence") or "video"; kind = row.get("kind") or "sender"
    label = row.get("label") or rid[:8]; tr = row.get("transport") or {}
    group = row.get("group_name") or label; role = row.get("role") or essence
    out = {"senders": {}, "receivers": {}, "sources": {}, "flows": {}}
    if kind == "receiver":
        fmt = {"video": "video", "audio": "audio", "data": "data"}.get(essence, "video")
        rec = _build_receiver_resource(rid, did, None, 0, label, version, fmt=fmt)
        _set_grouphint(rec, group, role)
        out["receivers"][rid] = rec
        return out
    # sender : source + flow + sender selon l'essence
    src_id = _stable_uuid(f"source:{rid}"); fid = _stable_uuid(f"flow:{rid}")
    if essence == "audio":
        out["sources"][src_id] = _build_audio_source_resource(src_id, did, None, label, version)
        out["flows"][fid]      = _build_audio_flow_resource(fid, did, src_id, None, label, version)
    elif essence == "data":
        out["sources"][src_id] = _build_data_source_resource(src_id, did, None, label, version)
        out["flows"][fid]      = _build_data_flow_resource(fid, did, src_id, None, label, version)
    else:  # video
        width  = int(tr.get("width") or 1280); height = int(tr.get("height") or 720)
        chroma = str(tr.get("chroma") or "422"); bd = tr.get("bit_depth") or 10
        cs     = tr.get("colorspace") or "BT709"; transfer = tr.get("transfer") or "SDR"
        scan   = str(tr.get("scan") or "p"); fo = str(tr.get("field_order") or "")
        out["sources"][src_id] = _build_source_resource(src_id, did, None, label, version)
        out["flows"][fid]      = _build_flow_resource(fid, did, src_id, None, label, width, height,
                                                      version, chroma, bd, cs, transfer, scan, fo)
    snd = _build_sender_resource(rid, did, fid, None, label, version)
    _set_grouphint(snd, group, role)
    out["senders"][rid] = snd
    return out


def _build_device_resource(did, vmid, hostname, version):
    return {
        "id": did,
        "version": version,
        "label": hostname or f"container_{vmid}",
        "description": f"Container VMID {vmid}",
        "tags": _asset_tags(avec_fonction=True, base={"urn:x-mxl:vmid": [str(vmid)]}),
        "type": "urn:x-nmos:device:generic",
        "node_id": _state["node_id"],
        "senders": [],     # Phase 2 : peuplé pour les workers 2110_sender
        "receivers": [],
        "controls": _controls(),
    }

def _build_node_resource(version):
    host = _get_host_address()
    return {
        "id": _state["node_id"],
        "version": version,
        "label": _setting("nmos_node_label", "MXL Orchestrator"),
        "description": _setting("nmos_node_description", "Bobi.Studio — provider NMOS centralisé"),
        "tags": _asset_tags(),                         # BCP-002-02 (Node : sans fonction)
        "href": f"http://{host}:5000/",
        "hostname": socket.gethostname(),
        "api": {
            "versions": [IS04_VERSION],
            "endpoints": [{"host": host, "port": 5000, "protocol": "http"}],
        },
        "services": [],
        "caps": {},
        "clocks": [{"name": "clk0", "ref_type": "internal"}],
        "interfaces": [{
            "name": _primary_iface(),
            # IS-04 v1.3 autorise null, mais certains clients (Buttons, Zod strict)
            # exigent une string. On remplit avec la MAC de la NIC principale
            # (format AMWA : 6 octets hex séparés par des tirets, minuscules).
            "chassis_id": _mac_address(_primary_iface()),
            "port_id":    _mac_address(_primary_iface()),
        }],
    }

def _setting(key, default):
    from app.database import db_get_setting
    v = db_get_setting(key, None)
    return v if v is not None else default


def _prefixe_libelle(node_id=None):
    """Préfixe des LIBELLÉS NMOS (réglage `nmos_label_prefix`, override par nœud).

    Les libellés étaient construits sur le hostname du conteneur (« mtlrx603 Rx 1 (video) »), ce qui
    exposait au réseau un identifiant interne dérivé du vmid — lequel est un handle local jetable.
    Les ressources sont désormais nommées « Rx 1 », « TX 1 2110-20 »… et le préfixe ne sert qu'à
    lever une ambiguïté quand elle existe.

    ⚠ ELLE EXISTE dès qu'un cluster a PLUSIEURS moteurs : toutes les ressources vivent sous un
    Device de niveau cluster unique (cf. `_build_cluster_device_resource`), donc deux nœuds
    numérotent chacun leur « Rx 1 » et un contrôleur tiers voit deux entrées identiques. D'où
    l'override PAR NŒUD : un site multi-nœuds pose un préfixe distinct par nœud.

    N'affecte QUE l'affichage : ni les UUID (registre `nmos_resources`), ni les noms de flux MXL,
    ni les SSRC — tous indépendants du libellé. Renommer est donc sans effet sur le câblage."""
    try:
        from app.settings import setting_for
        v = setting_for("nmos_label_prefix", node_id)
    except Exception:
        v = _setting("nmos_label_prefix", "")
    v = (v or "").strip()
    return f"{v} " if v else ""

_ifb_cache = {}   # vmid → (ts, [ifnames media2110 du nœud, red d'abord]) — évite N requêtes DB par rebuild

def _media_ifaces_of(vmid):
    """NIC média 2110 du NŒUD qui héberge `vmid` (node_interfaces role=media2110, red en tête,
    blue ensuite — ordre des legs 2022-7). [] si nœud/interfaces inconnus. Cache 10 s."""
    ent = _ifb_cache.get(vmid)
    if ent and time.time() - ent[0] < 10:
        return ent[1]
    out = []
    try:
        from app.database import db_get_container, db_get_node_interfaces
        c = db_get_container(int(vmid)) if vmid is not None else None
        if c and c.get("node_id"):
            rows = [r for r in db_get_node_interfaces(c["node_id"]) if r.get("role") == "media2110"]
            rows.sort(key=lambda r: ((r.get("pair_role") or "red") != "red", r.get("ifname") or ""))
            out = [r["ifname"] for r in rows if r.get("ifname")]
    except Exception:
        out = []
    _ifb_cache[vmid] = (time.time(), out)
    return out

def _iface_bindings(vmid, legs=1):
    """interface_bindings IS-04 : les NIC média du nœud du container — 1 entrée (red) ou 2
    (red+blue, 2022-7, cohérent avec le nombre de transport_params legs). Un labo NMOS vérifie
    cette cohérence. Repli historique (NIC locale du contrôleur) si rien de déclaré en base."""
    ifs = _media_ifaces_of(vmid)
    if not ifs:
        return [_primary_iface()]
    return ifs[:max(1, min(int(legs or 1), len(ifs)))]

def _primary_iface():
    """Trouve le nom de la NIC principale (première non-loopback up avec une MAC)."""
    try:
        import os
        for name in sorted(os.listdir("/sys/class/net/")):
            if name == "lo":
                continue
            mac_path = f"/sys/class/net/{name}/address"
            if os.path.exists(mac_path):
                with open(mac_path) as f:
                    mac = f.read().strip()
                if mac and mac != "00:00:00:00:00:00":
                    return name
    except Exception:
        pass
    return "eth0"

def _mac_address(iface):
    """Lit la MAC d'une interface au format AMWA (xx-xx-xx-xx-xx-xx).
    Fallback non vide (Buttons et Zod strict exigent une string)."""
    try:
        with open(f"/sys/class/net/{iface}/address") as f:
            mac = f.read().strip()
        if mac and len(mac) == 17:
            return mac.replace(":", "-").lower()
    except Exception:
        pass
    return "00-00-00-00-00-00"

GROUPHINT_TAG = "urn:x-nmos:tag:grouphint/v1.0"

def _set_grouphint(resource, group_name, role):
    """Pose le tag de natural grouping BCP-002-01 sur une ressource sender/receiver.
    Format : '<group-name>:<role-in-group>' (scope device par défaut). Le ':' est
    réservé comme séparateur → on le bannit des noms/rôles. Un bundle = même
    group_name sur le même device ; les contrôleurs NMOS regroupent dessus.

    ★ La valeur FIGÉE au registre l'emporte sur celle qu'on vient de recalculer (cf.
    `db_nmos_resource_upsert`) : le grouphint est immuable, et `_registry_id` a déposé la valeur
    d'origine dans `_grouphint_fige` juste avant. Les valeurs passées en argument ne servent que
    pour une ressource hors registre (orpheline reconstruite, où elles VIENNENT du registre)."""
    gn, rl = _grouphint_fige.get(resource.get("id"), (group_name, role))
    gn = str(gn).replace(":", " ")
    rl = str(rl).replace(":", " ")
    resource.setdefault("tags", {})[GROUPHINT_TAG] = [f"{gn}:{rl}"]


# Valeurs de grouping figées, relevées au registre par `_registry_id` pendant le rebuild courant.
# {id de ressource → (group_name, role)}. Vidé à chaque rebuild : c'est un relais interne à la
# passe, pas un cache (le registre reste la seule source de vérité).
_grouphint_fige = {}

_RE_DERNIER_NOMBRE = re.compile(r"(\d+)(?!.*\d)", re.S)


def pad_index(texte):
    """Complète le DERNIER nombre d'un libellé de groupe/rôle sur deux chiffres.

    Le registre NMOS RECOMMANDE des noms et des rôles triables alphanumériquement (son exemple :
    « SDI 09 », « SDI 10 », « SDI 11 »). Sans ça un contrôleur affiche « 2110 1, 2110 10, 2110 11,
    2110 2 » — l'ordre du dictionnaire, pas celui de la régie.

    Seul le DERNIER nombre est touché, et jamais un nombre déjà à deux chiffres ou plus : l'index
    que nous générons est toujours en fin de chaîne (« 2110 Tx 1 », « audio 3 »), tandis que le
    préfixe de l'exploitant, lui, ne doit pas être réécrit — « STUDIO 2 » reste « STUDIO 2 »."""
    s = str(texte)
    m = _RE_DERNIER_NOMBRE.search(s)
    if not m or len(m.group(1)) >= 2:
        return s
    return s[:m.start(1)] + m.group(1).zfill(2) + s[m.end(1):]

def normaliser_grouping_registre():
    """Complète sur deux chiffres les index des `group_name`/`role` déjà au registre.

    RUPTURE ASSUMÉE, EN UNE FOIS. Les grouphints sont désormais immuables (cf.
    `db_nmos_resource_upsert`) : ils ne bougeront plus jamais tout seuls. Mais ceux déjà écrits
    sont non triables (« 2110 1, 2110 10, 2110 11, 2110 2 »), et les figer tels quels aurait gravé
    ce défaut pour de bon. On les normalise donc une dernière fois, ici, explicitement.

    Idempotent : compléter un index déjà à deux chiffres ne fait rien. Tourne au démarrage du
    service, avant le premier rebuild — jamais dans le chemin d'un rebuild.

    ⚠ CONSERVATRICE, et il le faut. Compléter le dernier nombre d'une chaîne à l'aveugle réécrirait
    aussi le préfixe de l'exploitant : « REGIE 1 » (préfixe seul, cas d'un conteneur à une vidéo)
    deviendrait « REGIE 01 » — on aurait renommé SON texte, définitivement, puisque la valeur est
    figée juste après. On ne complète donc QUE les familles : un radical (le texte avant le nombre
    final) qui porte AU MOINS DEUX index distincts dans le registre. C'est exactement le cas où le
    tri a un sens ; un groupe seul n'a rien à trier, et son nombre appartient à l'exploitant."""
    from app.database import db_nmos_resources, db_nmos_resource_set_group
    lignes = db_nmos_resources() or []

    def _radical(v):
        """(radical, index) si la chaîne finit par un nombre, sinon (None, None)."""
        m = _RE_DERNIER_NOMBRE.search(str(v or ""))
        return (str(v)[:m.start(1)], m.group(1)) if m else (None, None)

    # La prudence ne vaut que pour `group_name` : c'est LUI qui contient le préfixe de l'exploitant.
    # Un `role` est à nous de bout en bout (« video », « audio N », « anc N ») — c'est même
    # l'exemple que donne le registre pour le tri (« audio 09 », « audio 10 ») — et sa famille est
    # souvent de taille 1 (un seul flux audio par groupe), ce qui ferait rater la règle ci-dessous.
    familles = {}
    for r in lignes:
        rad, idx = _radical(r.get("group_name"))
        if rad is not None:
            familles.setdefault(rad, set()).add(idx)

    def _completer_groupe(v):
        rad, _ = _radical(v)
        if rad is None or len(familles.get(rad, ())) < 2:
            return v
        return pad_index(v)

    n = 0
    for r in lignes:
        gn, rl = r.get("group_name") or "", r.get("role") or ""
        gn2, rl2 = _completer_groupe(gn), pad_index(rl)
        if (gn2, rl2) != (gn, rl):
            db_nmos_resource_set_group(r["id"], gn2, rl2)
            n += 1
    if n:
        log.info("nmos: %d grouphints normalisés (index sur deux chiffres, BCP-002-01) — "
                 "rupture unique, ils sont immuables à partir de maintenant", n)
    return n


def rebuild_model():
    """Reconstruit la liste des devices / receivers / senders / sources / flows
    depuis l'état DB. Préserve les staged/active states existants (matché par id).

    Natural grouping (BCP-002-01) : chaque ensemble « 1 vidéo + ses audios » porte
    un même group_name → le contrôleur l'affiche comme un bundle distinct. Côté
    receiver, les audios sont répartis sur les groupes vidéo (audio j → vidéo j %
    n_video) ; côté sender (≤1 vidéo) tout le container forme un seul bundle."""
    from app.database import db_get_containers
    version = _tai_version()
    _grouphint_fige.clear()          # relais interne à CETTE passe (cf. _set_grouphint)
    new_devices = {}
    new_receivers = {}
    new_senders = {}
    new_sources = {}
    new_flows = {}
    # C2a : un seul Device de NIVEAU CLUSTER (stable) possède toutes les ressources. Les senders/
    # receivers y sont rattachés quel que soit le conteneur servant. (Remplace le device par-vmid.)
    cluster_did = _stable_uuid("device:cluster")
    new_devices[cluster_did] = _build_cluster_device_resource(cluster_did, version)
    for c in db_get_containers():
        vmid = c["vmid"]
        instance_uuid = c.get("instance_uuid")
        dc = c.get("deploy_config")
        try:
            dc = json.loads(dc) if isinstance(dc, str) else dc
        except Exception:
            dc = None
        dc_type = dc.get("type") if dc else None
        from app import plugins as _plg
        _manifest   = _plg.REGISTRY.get(dc_type) or {}
        _role = _manifest.get("nmos_role")           # "receiver" | "sender" | "both" (moteur bi-rôle)
        is_receiver = _role in ("receiver", "both")
        is_sender   = _role in ("sender", "both")
        dc_params = (dc.get("params") or {}) if dc else {}
        # C2b+ : overrides de rebinding explicite {slot_key: resource_id} (vide = tout en auto).
        nmos_bind = dc_params.get("nmos_bind") or {}
        # Mode NMOS : cluster `auto` (défaut, auto-création par slot) | `static` (pool fixe : un slot
        # non bindé n'émet rien). Override par conteneur `nmos_no_autoseed` force le statique localement.
        _static_mode = (_setting("nmos_mode", "auto") == "static") or bool(dc_params.get("nmos_no_autoseed"))
        allow_autoseed = not _static_mode
        # Flux RX composables (« Option A ») : ``rx_flows`` fait foi — le groupement audio/ANC→vidéo
        # suit ``attached_to`` (et non plus un ratio ``idx % n_video``). Repli DÉRIVÉ du legacy
        # (compteurs + ratio, fenêtre ``active_rx_count``) si la liste est absente (container pas
        # encore migré). Chaque flux = un receiver à SON idx → le keying NMOS reste vmid+idx (stable).
        from app import io2110_flows as _iof
        _rx_flows  = _iof.active_flows(dc_params, "rx") if is_receiver else []
        _rx_videos = [f for f in _rx_flows if f["essence"] == "video"]
        _rx_audios = [f for f in _rx_flows if f["essence"] == "audio"]
        _rx_ancs   = [f for f in _rx_flows if f["essence"] == "anc"]
        _rx_vid_of, _rx_sub_of = _iof.grouping_maps(_rx_flows)
        n_video, n_audio, n_data = len(_rx_videos), len(_rx_audios), len(_rx_ancs)
        has_video_send = is_sender and bool(dc_params.get("video"))
        n_audio_send = len(dc_params.get("audios") or []) if is_sender else 0
        # Moteur bi-rôle : N senders vidéo (slots TX), limités à active_tx_count.
        tx_slots_full = (dc_params.get("tx_slots") or []) if is_sender else []
        _atc = dc_params.get("active_tx_count")
        n_tx_active = min(int(_atc if _atc is not None else len(tx_slots_full)), len(tx_slots_full)) if is_sender else 0
        tx_slots = tx_slots_full[:n_tx_active]
        smpte_2022_7 = bool(dc_params.get("smpte_2022_7")) if is_receiver else False
        n_legs = 2 if smpte_2022_7 else 1
        # Container exposé en NMOS s'il a au moins un receiver ou sender
        if (n_video + n_audio + n_data) <= 0 and not has_video_send and n_audio_send <= 0 and not tx_slots:
            continue
        # C2a : toutes les ressources sous le Device cluster stable (plus de device par-vmid).
        did = cluster_did
        # Libellés NMOS : préfixe réglable (vide par défaut) au lieu du hostname du conteneur,
        # qui exposait « mtlrx<vmid> » — un handle local jetable — aux contrôleurs tiers.
        _pfx = _prefixe_libelle(c.get("node_id"))
        base = _pfx.strip() or "2110"

        # Nom de bundle d'un ensemble : si plusieurs vidéos sur le container, on
        # suffixe l'index pour distinguer les bundles ; sinon le container = 1 bundle.
        def _grp_name(group_idx):
            return f"{base} {group_idx + 1:02d}" if n_video > 1 else base

        # Groupe d'un flux audio/ANC : celui de SA vidéo attachée (câblage groupé) ; un flux
        # INDÉPENDANT (attached_to=None → video_idx None) forme son propre bundle.
        def _child_grp(video_idx, essence, idx):
            if video_idx is not None:
                return _grp_name(video_idx)
            return f"{base} · {'audio' if essence == 'audio' else 'anc'} {idx + 1:02d}"

        # Receivers vidéo — un bundle (group_name) par flux vidéo
        for vf in _rx_videos:
            idx = vf["idx"]
            label = f"{_pfx}Rx {idx + 1} (video)"
            rid, label = _registry_id(f"receiver:v:{vmid}:{idx}", instance_uuid, slot_rx("v", idx), "video",
                                      "receiver", label, {}, _grp_name(idx), "video",
                                      bind_override=nmos_bind.get(slot_rx("v", idx)), allow_autoseed=allow_autoseed)
            if rid is None:
                continue
            new_receivers[rid] = _build_receiver_resource(rid, did, vmid, idx, label, version, fmt="video", legs=n_legs)
            _set_grouphint(new_receivers[rid], _grp_name(idx), "video")
            new_devices[did]["receivers"].append(rid)
            cur = _recv_state.get(rid)
            if not cur or len(cur["staged"]["transport_params"]) != n_legs:
                _recv_state[rid] = {
                    "staged": _empty_staged(n_legs), "active": _empty_staged(n_legs),
                    "vmid": vmid, "recv_idx": idx, "essence": "video",
                }

        # Receivers audio — rattachés au bundle de leur vidéo (attached_to) ou indépendants
        for af in _rx_audios:
            idx = af["idx"]
            vi  = _rx_vid_of.get(("audio", idx))
            n   = (_rx_sub_of.get(("audio", idx), 0) or 0) + 1
            grp = _child_grp(vi, "audio", idx)
            label = f"{_pfx}Rx {idx + 1} (audio)"
            rid, label = _registry_id(f"receiver:a:{vmid}:{idx}", instance_uuid, slot_rx("a", idx), "audio",
                                      "receiver", label, {}, grp, f"audio {n:02d}",
                                      bind_override=nmos_bind.get(slot_rx("a", idx)), allow_autoseed=allow_autoseed)
            if rid is None:
                continue
            new_receivers[rid] = _build_receiver_resource(rid, did, vmid, idx, label, version, fmt="audio", legs=n_legs)
            _set_grouphint(new_receivers[rid], grp, f"audio {n:02d}")
            new_devices[did]["receivers"].append(rid)
            cur = _recv_state.get(rid)
            if not cur or len(cur["staged"]["transport_params"]) != n_legs:
                _recv_state[rid] = {
                    "staged": _empty_staged(n_legs), "active": _empty_staged(n_legs),
                    "vmid": vmid, "recv_idx": idx, "essence": "audio",
                }

        # Receivers ANC (2110-40 / data) — rattachés au bundle de leur vidéo ou indépendants
        for df in _rx_ancs:
            idx = df["idx"]
            vi  = _rx_vid_of.get(("anc", idx))
            n   = (_rx_sub_of.get(("anc", idx), 0) or 0) + 1
            grp = _child_grp(vi, "anc", idx)
            label = f"{_pfx}Rx {idx + 1} (anc)"
            rid, label = _registry_id(f"receiver:d:{vmid}:{idx}", instance_uuid, slot_rx("d", idx), "data",
                                      "receiver", label, {}, grp, f"anc {n:02d}",
                                      bind_override=nmos_bind.get(slot_rx("d", idx)), allow_autoseed=allow_autoseed)
            if rid is None:
                continue
            new_receivers[rid] = _build_receiver_resource(rid, did, vmid, idx, label, version, fmt="data", legs=n_legs)
            _set_grouphint(new_receivers[rid], grp, f"anc {n:02d}")
            new_devices[did]["receivers"].append(rid)
            cur = _recv_state.get(rid)
            if not cur or len(cur["staged"]["transport_params"]) != n_legs:
                _recv_state[rid] = {
                    "staged": _empty_staged(n_legs), "active": _empty_staged(n_legs),
                    "vmid": vmid, "recv_idx": idx, "essence": "anc",
                }

        # Sender vidéo (0 ou 1)
        if has_video_send:
            v = (dc.get("params") or {}).get("video") or {}
            # Défaut UNIQUE par container (dérivé du vmid, base dédiée) : un défaut partagé
            # (ex-239.10.10.1) écrase un flux existant dès qu'on active un slot resté aux défauts.
            mcast = v.get("multicast_ip") or "239.10.30.{}".format((vmid % 250) + 1)
            port  = int(v.get("dest_port") or 5000)
            width = int(v.get("width") or 1280)
            height= int(v.get("height") or 720)
            # chroma/bit_depth/colorimétrie du flow 2110-20 (top-level params, fallback video.*)
            _pp = dc.get("params") or {}
            chroma = str(_pp.get("chroma") or v.get("chroma") or "422")
            bit_depth = _pp.get("bit_depth") or v.get("bit_depth") or 10
            from app.scripts import COLORIMETRY, nmos_colorimetry, DEFAULT_COLORIMETRY
            _colo = str(_pp.get("colorimetry") or v.get("colorimetry") or "").strip().lower()
            if _colo in COLORIMETRY:
                cs, transfer = COLORIMETRY[_colo]["nmos_colorspace"], COLORIMETRY[_colo]["nmos_transfer"]
            else:   # fallback : déduire des color_* ffmpeg si présents, sinon BT709/SDR
                cs, transfer = nmos_colorimetry(v.get("color_primaries"), v.get("color_trc"))
            label  = f"{_pfx}Tx (video)"
            _scan = str(_pp.get("scan") or v.get("scan") or "p")
            _fo   = str(_pp.get("field_order") or v.get("field_order") or "")
            _tr = {"multicast_ip": mcast, "port": port, "width": width, "height": height,
                   "chroma": chroma, "bit_depth": bit_depth, "colorspace": cs, "transfer": transfer,
                   "scan": _scan, "field_order": _fo, "fps": v.get("fps")}
            snd_id, label = _registry_id(f"sender:v:{vmid}", instance_uuid, "v", "video",
                                         "sender", label, _tr, base, "video",
                                         bind_override=nmos_bind.get("v"), allow_autoseed=allow_autoseed)
            if snd_id is not None:   # None = mode statique, slot non bindé → aucun sender émis
                src_id = _stable_uuid(f"source:{snd_id}")
                fid    = _stable_uuid(f"flow:{snd_id}")
                new_sources[src_id] = _build_source_resource(src_id, did, vmid, label, version)
                new_flows[fid]      = _build_flow_resource(fid, did, src_id, vmid, label, width, height,
                                                           version, chroma, bit_depth, cs, transfer, _scan, _fo)
                new_senders[snd_id] = _build_sender_resource(snd_id, did, fid, vmid, label, version)
                _set_grouphint(new_senders[snd_id], base, "video")
                new_devices[did]["senders"].append(snd_id)
                smpte_2022_7_v = bool(v.get("smpte_2022_7"))
                mcast1_v = v.get("multicast_ip_leg1")
                port1_v  = v.get("dest_port_leg1")
                leg1_v   = (mcast1_v, port1_v) if (smpte_2022_7_v and mcast1_v and port1_v) else None
                if leg1_v:
                    new_senders[snd_id]["interface_bindings"] = _iface_bindings(vmid, 2)
                if snd_id not in _send_state:
                    empty = _empty_sender_staged(mcast, port, leg1=leg1_v)
                    _send_state[snd_id] = {
                        "staged": empty, "active": json.loads(json.dumps(empty)),
                        "vmid": vmid, "multicast_ip": mcast, "destination_port": port,
                        "essence": "video",
                    }
                else:
                    _send_state[snd_id]["multicast_ip"] = mcast
                    _send_state[snd_id]["destination_port"] = port
                    # Resync transport_params if 2022-7 config changed
                    cur_legs = len(_send_state[snd_id]["staged"].get("transport_params") or [])
                    want_legs = 2 if leg1_v else 1
                    if cur_legs != want_legs:
                        empty = _empty_sender_staged(mcast, port, leg1=leg1_v)
                        _send_state[snd_id]["staged"] = empty
                        _send_state[snd_id]["active"] = json.loads(json.dumps(empty))

        # Senders vidéo MOTEUR (slots TX) — un sender NMOS par entrée tx_slots, keyé sur tx_idx.
        # (Additif : sans tx_slots, aucun sender — les receivers/senders existants sont inchangés.)
        for tx_idx, tslot in enumerate(tx_slots):
            # Slot audio-seul / ANC-seul (marqueur `video_off` posé par io2110_layouts) : PAS de sender
            # vidéo. Sans ce saut, un slot sans vidéo enregistrait un sender 2110-20 fantôme (défauts
            # 1280×720 + mcast dérivé) → flux vidéo inexistant annoncé en IS-04. Les senders audio/ANC
            # de ce slot restent créés par leurs boucles dédiées (gardées sur leur propre mcast).
            if tslot.get("video_off"):
                continue
            # Défaut UNIQUE par slot (dérivé de tx_idx ; TX0 = .1 rétro-compat) — jamais la même
            # adresse pour deux slots : activer un slot aux défauts n'écrase plus le flux d'un autre.
            mcast = tslot.get("multicast_ip") or "239.10.10.{}".format((tx_idx % 250) + 1)
            port  = int(tslot.get("dest_port") or 5000)
            width = int(tslot.get("width") or 1280)
            height = int(tslot.get("height") or 720)
            chroma = str(tslot.get("chroma") or "422")
            bit_depth = tslot.get("bit_depth") or 10
            from app.scripts import COLORIMETRY, nmos_colorimetry
            _colo = str(tslot.get("colorimetry") or "").strip().lower()
            if _colo in COLORIMETRY:
                cs, transfer = COLORIMETRY[_colo]["nmos_colorspace"], COLORIMETRY[_colo]["nmos_transfer"]
            else:
                cs, transfer = nmos_colorimetry(tslot.get("color_primaries"), tslot.get("color_trc"))
            # Passthrough du balayage : la vérité est le format du PRODUCTEUR du shm câblé sur ce
            # slot ; à défaut, le scan reçu par le moteur lui-même. Ne pas mettre progressif en dur.
            _tx_shm = dc_params.get(cle_tx_shm(tx_idx)) or ""
            _src_fmt = {}
            if _tx_shm:
                try:
                    from app.monitor import _shm_fmt as _mtl_shm_fmt
                    _src_fmt = _mtl_shm_fmt(_tx_shm) or {}
                except Exception:
                    _src_fmt = {}
            _scan = str(_src_fmt.get("scan") or dc_params.get("scan") or "p")
            _fo   = str(_src_fmt.get("field_order") or dc_params.get("field_order") or "")
            label  = f"{_pfx}Tx {tx_idx + 1} (video)"
            _tr = {"multicast_ip": mcast, "port": port, "width": width, "height": height,
                   "chroma": chroma, "bit_depth": bit_depth, "colorspace": cs, "transfer": transfer,
                   "scan": _scan, "field_order": _fo, "fps": tslot.get("fps")}
            snd_id, label = _registry_id(f"sender:v:{vmid}:tx{tx_idx}", instance_uuid, slot_tx(tx_idx, "v"),
                                         "video", "sender", label, _tr, f"{base} Tx {tx_idx + 1:02d}", "video",
                                         bind_override=nmos_bind.get(slot_tx(tx_idx, "v")), allow_autoseed=allow_autoseed)
            if snd_id is None:
                continue
            src_id = _stable_uuid(f"source:{snd_id}")
            fid    = _stable_uuid(f"flow:{snd_id}")
            new_sources[src_id] = _build_source_resource(src_id, did, vmid, label, version)
            new_flows[fid]      = _build_flow_resource(fid, did, src_id, vmid, label, width, height,
                                                       version, chroma, bit_depth, cs, transfer, _scan, _fo)
            new_senders[snd_id] = _build_sender_resource(snd_id, did, fid, vmid, label, version)
            # Groupé PAR SLOT TX (comme les receivers le sont par canal) : vidéo + audio(s)
            # + ANC d'un même slot partagent le group_name, le rôle = essence.
            _set_grouphint(new_senders[snd_id], f"{base} Tx {tx_idx + 1:02d}", "video")
            new_devices[did]["senders"].append(snd_id)
            smpte_v = bool(tslot.get("smpte_2022_7") or dc_params.get("smpte_2022_7"))
            mcast1_v = tslot.get("multicast_ip_leg1")
            port1_v  = tslot.get("dest_port_leg1")
            leg1_v   = (mcast1_v, port1_v) if (smpte_v and mcast1_v and port1_v) else None
            if leg1_v:
                new_senders[snd_id]["interface_bindings"] = _iface_bindings(vmid, 2)
            if snd_id not in _send_state:
                empty = _empty_sender_staged(mcast, port, leg1=leg1_v)
                _send_state[snd_id] = {
                    "staged": empty, "active": json.loads(json.dumps(empty)),
                    "vmid": vmid, "multicast_ip": mcast, "destination_port": port,
                    "essence": "video", "tx_idx": tx_idx,
                }
            else:
                _send_state[snd_id]["multicast_ip"] = mcast
                _send_state[snd_id]["destination_port"] = port
                # AUTORITÉ du conteneur : rafraîchir l'identité (tx_idx/essence) même sur une entrée
                # préexistante — sinon une entrée semée SANS tx_idx par la passe ORPHELINE (registre
                # persisté, quand le sender était transitoirement absent) reste collée à tx_idx=None
                # → transportfile prend la mauvaise branche → SDP vide (404). Auto-réparation.
                _send_state[snd_id]["tx_idx"] = tx_idx
                _send_state[snd_id]["essence"] = "video"
                cur_legs = len(_send_state[snd_id]["staged"].get("transport_params") or [])
                want_legs = 2 if leg1_v else 1
                if cur_legs != want_legs:
                    empty = _empty_sender_staged(mcast, port, leg1=leg1_v)
                    _send_state[snd_id]["staged"] = empty
                    _send_state[snd_id]["active"] = json.loads(json.dumps(empty))

        # Senders audio (0, 1 ou 2)
        for a_idx, a in enumerate((dc.get("params") or {}).get("audios") or []):
            # Défaut UNIQUE par flux audio (a_idx 0 = .1 rétro-compat).
            mcast = a.get("multicast_ip") or "239.10.20.{}".format((a_idx % 250) + 1)
            port  = int(a.get("dest_port") or (5004 + 2 * a_idx))
            label  = f"{_pfx}Tx (audio {a_idx + 1})"
            snd_id, label = _registry_id(f"sender:a:{vmid}:{a_idx}", instance_uuid, slot_rx("a", a_idx), "audio",
                                         "sender", label, {"multicast_ip": mcast, "port": port}, base, f"audio {a_idx + 1:02d}",
                                         bind_override=nmos_bind.get(slot_rx("a", a_idx)), allow_autoseed=allow_autoseed)
            if snd_id is None:
                continue
            src_id = _stable_uuid(f"source:{snd_id}")
            fid    = _stable_uuid(f"flow:{snd_id}")
            new_sources[src_id] = _build_audio_source_resource(src_id, did, vmid, label, version)
            new_flows[fid]      = _build_audio_flow_resource(fid, did, src_id, vmid, label, version)
            new_senders[snd_id] = _build_sender_resource(snd_id, did, fid, vmid, label, version)
            _set_grouphint(new_senders[snd_id], base, f"audio {a_idx + 1:02d}")
            new_devices[did]["senders"].append(snd_id)
            smpte_2022_7_a = bool(a.get("smpte_2022_7"))
            mcast1_a = a.get("multicast_ip_leg1")
            port1_a  = a.get("dest_port_leg1")
            leg1_a   = (mcast1_a, port1_a) if (smpte_2022_7_a and mcast1_a and port1_a) else None
            if leg1_a:
                new_senders[snd_id]["interface_bindings"] = _iface_bindings(vmid, 2)
            if snd_id not in _send_state:
                empty = _empty_sender_staged(mcast, port, leg1=leg1_a)
                _send_state[snd_id] = {
                    "staged": empty, "active": json.loads(json.dumps(empty)),
                    "vmid": vmid, "multicast_ip": mcast, "destination_port": port,
                    "essence": "audio", "audio_idx": a_idx,
                }
            else:
                _send_state[snd_id]["multicast_ip"] = mcast
                _send_state[snd_id]["destination_port"] = port

        # Senders audio par slot TX (moteur bi-rôle, ex. 2110_io) — distinct des senders
        # audio globaux (sender_2110). « Option A » : N flux 2110-30 par slot (plus de cap à 2).
        for tx_idx, tslot in enumerate(tx_slots):
            for ai, acfg in enumerate(tslot.get("audios") or []):
                mcast = acfg.get("multicast_ip")
                if not mcast:
                    continue
                port   = int(acfg.get("dest_port") or 0)
                label  = f"{_pfx}Tx {tx_idx + 1} (audio {ai + 1})"
                snd_id, label = _registry_id(f"sender:a:{vmid}:tx{tx_idx}:{ai}", instance_uuid,
                                             slot_tx(tx_idx, "a%d" % ai), "audio", "sender", label,
                                             {"multicast_ip": mcast, "port": port},
                                             f"{base} Tx {tx_idx + 1:02d}", f"audio {ai + 1:02d}",
                                             bind_override=nmos_bind.get(slot_tx(tx_idx, "a%d" % ai)), allow_autoseed=allow_autoseed)
                if snd_id is None:
                    continue
                src_id = _stable_uuid(f"source:{snd_id}")
                fid    = _stable_uuid(f"flow:{snd_id}")
                new_sources[src_id] = _build_audio_source_resource(src_id, did, vmid, label, version)
                new_flows[fid]      = _build_audio_flow_resource(fid, did, src_id, vmid, label, version)
                new_senders[snd_id] = _build_sender_resource(snd_id, did, fid, vmid, label, version)
                _set_grouphint(new_senders[snd_id], f"{base} Tx {tx_idx + 1:02d}", f"audio {ai + 1:02d}")
                new_devices[did]["senders"].append(snd_id)
                smpte_a = bool(tslot.get("smpte_2022_7") or dc_params.get("smpte_2022_7"))
                mcast1_a = acfg.get("multicast_ip_leg1")
                port1_a  = acfg.get("dest_port_leg1")
                leg1_a   = (mcast1_a, port1_a) if (smpte_a and mcast1_a and port1_a) else None
                if leg1_a:
                    new_senders[snd_id]["interface_bindings"] = _iface_bindings(vmid, 2)
                if snd_id not in _send_state:
                    empty_a = _empty_sender_staged(mcast, port, leg1=leg1_a)
                    _send_state[snd_id] = {
                        "staged": empty_a, "active": json.loads(json.dumps(empty_a)),
                        "vmid": vmid, "multicast_ip": mcast, "destination_port": port,
                        "essence": "audio", "tx_idx": tx_idx, "audio_idx": ai,
                    }
                else:
                    _send_state[snd_id]["multicast_ip"] = mcast
                    _send_state[snd_id]["destination_port"] = port
                    # Auto-réparation identité (cf. sender vidéo) : une entrée semée par la passe
                    # orpheline sans tx_idx/audio_idx serait collée → SDP audio par-slot introuvable.
                    _send_state[snd_id]["tx_idx"] = tx_idx
                    _send_state[snd_id]["essence"] = "audio"
                    _send_state[snd_id]["audio_idx"] = ai

        # Senders ANC MOTEUR (slots TX) — un sender data (2110-40) par slot TX porteur d'une dest ANC.
        # Le TX ANC suit la vidéo câblée (shm dérivé) → même tx_idx que le sender vidéo, essence "anc".
        for tx_idx, tslot in enumerate(tx_slots):
            mcast = tslot.get("anc_multicast_ip")
            if not mcast:
                continue
            port = int(tslot.get("anc_dest_port") or 0)
            label  = f"{_pfx}Tx {tx_idx + 1} (anc)"
            snd_id, label = _registry_id(f"sender:d:{vmid}:tx{tx_idx}", instance_uuid, slot_tx(tx_idx, "d"),
                                         "data", "sender", label, {"multicast_ip": mcast, "port": port},
                                         f"{base} Tx {tx_idx + 1:02d}", "anc",
                                         bind_override=nmos_bind.get(slot_tx(tx_idx, "d")), allow_autoseed=allow_autoseed)
            if snd_id is None:
                continue
            src_id = _stable_uuid(f"source:{snd_id}")
            fid    = _stable_uuid(f"flow:{snd_id}")
            new_sources[src_id] = _build_data_source_resource(src_id, did, vmid, label, version)
            new_flows[fid]      = _build_data_flow_resource(fid, did, src_id, vmid, label, version)
            new_senders[snd_id] = _build_sender_resource(snd_id, did, fid, vmid, label, version)
            _set_grouphint(new_senders[snd_id], f"{base} Tx {tx_idx + 1:02d}", "anc")
            new_devices[did]["senders"].append(snd_id)
            smpte_d = bool(tslot.get("smpte_2022_7") or dc_params.get("smpte_2022_7"))
            mcast1_d = tslot.get("anc_multicast_ip_leg1")
            port1_d  = tslot.get("anc_dest_port_leg1")
            leg1_d   = (mcast1_d, port1_d) if (smpte_d and mcast1_d and port1_d) else None
            if leg1_d:
                new_senders[snd_id]["interface_bindings"] = _iface_bindings(vmid, 2)
            if snd_id not in _send_state:
                empty = _empty_sender_staged(mcast, port, leg1=leg1_d)
                _send_state[snd_id] = {
                    "staged": empty, "active": json.loads(json.dumps(empty)),
                    "vmid": vmid, "multicast_ip": mcast, "destination_port": port,
                    "essence": "anc", "tx_idx": tx_idx,
                }
            else:
                _send_state[snd_id]["multicast_ip"] = mcast
                _send_state[snd_id]["destination_port"] = port
                # Auto-réparation identité (cf. sender vidéo/audio).
                _send_state[snd_id]["tx_idx"] = tx_idx
                _send_state[snd_id]["essence"] = "anc"

    # C2b : passe REGISTRE — ressources orphelines (servies par aucun conteneur live ce rebuild).
    # Restent exposées sous le Device cluster, INACTIVES, transport figé depuis le registre → le
    # routage et les abonnements RX d'un contrôleur survivent à la disparition du conteneur. Dédup
    # par `built` (ne pas réécrire celles déjà construites par la passe conteneurs).
    from app.database import db_nmos_resources as _db_nmos_resources
    built = set(new_senders) | set(new_receivers)
    for row in _db_nmos_resources():
        if row["id"] in built:
            continue
        parts = _build_orphan_resources(row, version, cluster_did)
        new_senders.update(parts["senders"]);   new_receivers.update(parts["receivers"])
        new_sources.update(parts["sources"]);    new_flows.update(parts["flows"])
        tr = row.get("transport") or {}
        for sid in parts["senders"]:
            new_devices[cluster_did]["senders"].append(sid)
            if sid not in _send_state:
                empty = _empty_sender_staged(tr.get("multicast_ip") or "239.0.0.1",
                                             tr.get("port") or 0)
                _send_state[sid] = {
                    "staged": empty, "active": json.loads(json.dumps(empty)),
                    "vmid": None, "multicast_ip": tr.get("multicast_ip"),
                    "destination_port": tr.get("port"), "essence": row.get("essence"),
                }
        for rcv in parts["receivers"]:
            new_devices[cluster_did]["receivers"].append(rcv)
            if rcv not in _recv_state:   # un abonnement préservé (live → orphelin) n'est PAS réinitialisé
                _recv_state[rcv] = {
                    "staged": _empty_staged(1), "active": _empty_staged(1),
                    "vmid": None, "recv_idx": 0, "essence": row.get("essence"),
                }

    # C2b/C2b+ : « servi par » AUTORITATIF à chaque rebuild. Servi (construit ce rebuild = dans `built`)
    # → vmid du conteneur servant (résolu via le binding registre instance_uuid → conteneur live) ;
    # orphelin → vmid None. Corrige le résidu d'un slot rebindé/libéré (l'ancien _send_state gardait un
    # vmid périmé → sender_sid_for/serving le résolvaient encore). Les staged/active RX sont préservés.
    from app.database import db_get_containers as _dbc2
    _iu2vmid = {c["instance_uuid"]: c["vmid"] for c in _dbc2() if c.get("instance_uuid")}
    _bind_by_id = {r["id"]: r.get("bind_instance_uuid") for r in _db_nmos_resources()}
    for sid in new_senders:
        sv = _iu2vmid.get(_bind_by_id.get(sid)) if sid in built else None
        if sid in _send_state:
            _send_state[sid]["vmid"] = sv
    for rid in new_receivers:
        sv = _iu2vmid.get(_bind_by_id.get(rid)) if rid in built else None
        if rid in _recv_state:
            _recv_state[rid]["vmid"] = sv

    # ── Tally IS-07 ──────────────────────────────────────────────────────────────────────────
    # Sources et Flows seulement : pas de Sender tant que le transport WebSocket n'est pas servi
    # (annoncer un Sender sans son transport promettrait un abonnement qui n'arriverait jamais).
    try:
        from . import is07 as _i7
        _r7 = _i7.ressources(cluster_did, version)
        for _s in _r7["sources"]:
            new_sources[_s["id"]] = _s
        for _f in _r7["flows"]:
            new_flows[_f["id"]] = _f
        for _sd in _r7.get("senders") or []:
            new_senders[_sd["id"]] = _sd
            new_devices[cluster_did]["senders"].append(_sd["id"])
            _c7 = (_r7.get("connection") or {}).get(_sd["id"])
            if _c7:
                _send_state[_sd["id"]] = dict(_c7, vmid=None, essence="data", is07=True)
    except Exception as e:
        log.warning("nmos: ressources IS-07 non construites (%s)", e)

    # ── Surface MXL (BCP-007-03) ──────────────────────────────────────────────────────────────
    # Ajoutée APRÈS les passes conteneurs et registre : elle ne partage rien avec elles (identité
    # dérivée, hors `nmos_resources`) et ne doit surtout pas perturber leurs invariants. Les états
    # atterrissent dans les MÊMES `_recv_state`/`_send_state` — la purge des orphelins en fin de
    # rebuild s'y applique donc telle quelle, sans code dédié.
    #
    # ⚠ Best-effort ASSUMÉ : le provider 2110 est du chemin de production, la surface MXL est un
    # ajout. Si elle explose, elle ne doit pas emporter le 2110 avec elle.
    try:
        from . import mxl as _mxl
        _mxl.build(new_devices, new_sources, new_flows, new_senders, new_receivers,
                   _recv_state, _send_state, cluster_did, version)
        _mxl.reindex(_send_state)
        _mxl.resync_subscriptions(new_receivers, _recv_state, new_senders, _send_state)
    except Exception as e:
        log.warning("nmos: surface MXL non construite (%s) — le provider 2110 continue", e)

    with _lock:
        _devices.clear();   _devices.update(new_devices)
        _receivers.clear(); _receivers.update(new_receivers)
        _senders.clear();   _senders.update(new_senders)
        _sources.clear();   _sources.update(new_sources)
        _flows.clear();     _flows.update(new_flows)
        # _build_receiver_resource met subscription.active=False par défaut → re-synchroniser depuis
        # l'état IS-05 DURABLE (_recv_state.active) : un récepteur abonné (master_enable ou SDP actif)
        # doit le rester à travers les rebuilds, sinon le badge passe « idle » alors que le flux
        # arrive (cas vu sur le moteur MTL : les ops TX déclenchent des rebuilds qui resettaient le RX).
        for rid, rc in _receivers.items():
            _st = (_recv_state.get(rid) or {}).get("active") or {}
            _on = bool(_st.get("master_enable")) or bool((_st.get("transport_file") or {}).get("data"))
            rc["subscription"] = {"sender_id": _st.get("sender_id") if _on else None, "active": _on}
        for orphan in list(_recv_state):
            if orphan not in _receivers:
                del _recv_state[orphan]
        for orphan in list(_send_state):
            if orphan not in _senders:
                del _send_state[orphan]

    # B2-3 : détection de collision multicast cluster (groupe partagé par >1 ressource du registre).
    # Non bloquant — alerte de visibilité (anti-spam : seulement si l'ensemble des collisions change).
    try:
        from app.allocations import multicast_conflicts as _mcast_conflicts
        conflicts = _mcast_conflicts()
        if conflicts:
            sig = ";".join(sorted(conflicts))
            global _last_mcast_conflict_sig
            if sig != _last_mcast_conflict_sig:
                from app.database import db_add_alert
                db_add_alert(f"{len(conflicts)} groupe(s) multicast en collision (registre NMOS) : "
                             f"{', '.join(sorted(conflicts)[:5])}", "warning", kind="net")
                _last_mcast_conflict_sig = sig
        else:
            _last_mcast_conflict_sig = ""
    except Exception:
        pass

    # Le modèle IS-12 suit le modèle IS-04 : un receiver qui apparaît/disparaît fait
    # apparaître/disparaître son monitor BCP-008. Best-effort — le provider IS-04/05 n'a pas à
    # tomber parce que la supervision a hoqueté.
    try:
        from . import is12
        if is12.actif():
            is12.sync_model()
    except Exception as e:
        log.warning("nmos: synchronisation du modèle IS-12 échouée : %s", e)


def _get_recv_count_for_vmid(vmid):
    from app.database import db_get_container
    c = db_get_container(vmid)
    if not c:
        return 0
    dc = c.get("deploy_config")
    try:
        dc = json.loads(dc) if isinstance(dc, str) else dc
    except Exception:
        dc = None
    return int(((dc or {}).get("params") or {}).get("video_count", 0) or 0)

# ═════════════════════════════════════════════════════════════════════
# IS-04 Node API (GET seulement, on est le Node lui-même)
# ═════════════════════════════════════════════════════════════════════

@bp.route(f"/x-nmos/node/{IS04_VERSION}/self", methods=["GET"])
def is04_self():
    return jsonify(_build_node_resource(_tai_version()))

@bp.route(f"/x-nmos/node/{IS04_VERSION}/devices", methods=["GET"])
def is04_devices():
    with _lock:
        return jsonify(list(_devices.values()))

@bp.route(f"/x-nmos/node/{IS04_VERSION}/devices/<did>", methods=["GET"])
def is04_device(did):
    with _lock:
        d = _devices.get(did)
    if not d: abort(404)
    return jsonify(d)

@bp.route(f"/x-nmos/node/{IS04_VERSION}/receivers", methods=["GET"])
def is04_receivers():
    with _lock:
        return jsonify(list(_receivers.values()))

@bp.route(f"/x-nmos/node/{IS04_VERSION}/receivers/<rid>", methods=["GET"])
def is04_receiver(rid):
    with _lock:
        r = _receivers.get(rid)
    if not r: abort(404)
    return jsonify(r)

@bp.route(f"/x-nmos/node/{IS04_VERSION}/senders", methods=["GET"])
def is04_senders():
    with _lock:
        return jsonify(list(_senders.values()))

@bp.route(f"/x-nmos/node/{IS04_VERSION}/senders/<sid>", methods=["GET"])
def is04_sender(sid):
    with _lock:
        s = _senders.get(sid)
    if not s: abort(404)
    return jsonify(s)

@bp.route(f"/x-nmos/node/{IS04_VERSION}/sources", methods=["GET"])
def is04_sources():
    with _lock:
        return jsonify(list(_sources.values()))

@bp.route(f"/x-nmos/node/{IS04_VERSION}/sources/<sid>", methods=["GET"])
def is04_source(sid):
    with _lock:
        s = _sources.get(sid)
    if not s: abort(404)
    return jsonify(s)

@bp.route(f"/x-nmos/node/{IS04_VERSION}/flows", methods=["GET"])
def is04_flows():
    with _lock:
        return jsonify(list(_flows.values()))

@bp.route(f"/x-nmos/node/{IS04_VERSION}/flows/<fid>", methods=["GET"])
def is04_flow(fid):
    with _lock:
        f = _flows.get(fid)
    if not f: abort(404)
    return jsonify(f)

# Index endpoints (NMOS exige une discoverability hiérarchique)
@bp.route("/x-nmos/", methods=["GET"])
def is_root():
    return jsonify(["node/", "connection/"])

@bp.route("/x-nmos/node/", methods=["GET"])
def is04_root():
    return jsonify([f"{IS04_VERSION}/"])

@bp.route(f"/x-nmos/node/{IS04_VERSION}/", methods=["GET"])
def is04_v_root():
    return jsonify(["self/", "devices/", "sources/", "flows/", "senders/", "receivers/", "subscriptions/"])

@bp.route(f"/x-nmos/node/{IS04_VERSION}/subscriptions", methods=["GET"])
def is04_subscriptions():
    return jsonify([])

@bp.route("/x-nmos/connection/", methods=["GET"])
def is05_root():
    return jsonify([f"{IS05_VERSION}/"])

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/", methods=["GET"])
def is05_v_root():
    return jsonify(["bulk/", "single/"])

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/", methods=["GET"])
def is05_single():
    return jsonify(["receivers/", "senders/"])

# ═════════════════════════════════════════════════════════════════════
# IS-05 Connection API — single/receivers
# ═════════════════════════════════════════════════════════════════════

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/receivers", methods=["GET"])
def is05_recv_list():
    with _lock:
        return jsonify([f"{rid}/" for rid in _receivers])

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/receivers/<rid>", methods=["GET"])
def is05_recv_endpoints(rid):
    if rid not in _receivers: abort(404)
    return jsonify(["constraints/", "staged/", "active/", "transportfile/"])

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/receivers/<rid>/constraints", methods=["GET"])
def is05_recv_constraints(rid):
    if rid not in _receivers: abort(404)
    from . import mxl as _mxl
    _c = _mxl.constraints(rid, _send_state, _recv_state)
    if _c is not None:
        return jsonify(_c)
    return jsonify([{}])  # single-leg, sans contrainte particulière

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/receivers/<rid>/staged", methods=["GET"])
def is05_recv_staged_get(rid):
    if rid not in _receivers: abort(404)
    with _lock:
        return jsonify(_recv_state[rid]["staged"])

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/receivers/<rid>/active", methods=["GET"])
def is05_recv_active_get(rid):
    if rid not in _receivers: abort(404)
    with _lock:
        return jsonify(_recv_state[rid]["active"])

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/receivers/<rid>/transportfile", methods=["GET"])
def is05_recv_transportfile(rid):
    if rid not in _receivers: abort(404)
    # BCP-007-03 : MXL n'a PAS de fichier de transport. L'endpoint doit exister (IS-05 l'exige)
    # et répondre 404 — c'est le pendant de `manifest_href: null` côté IS-04.
    from . import mxl as _mxl
    if _mxl.est_mxl(rid, _send_state, _recv_state):
        return ("", 404)
    with _lock:
        tf = _recv_state[rid]["active"].get("transport_file") or {}
    if not tf.get("data"):
        return ("", 404)
    return (tf["data"], 200, {"Content-Type": tf.get("type", "application/sdp")})

# ─── Abonnement manuel (UI I/O → Receiver, SDP collé) ────────────────────────

def receiver_rid_for(vmid, recv_idx, essence="video"):
    """(vmid, idx, essence) → receiver_id NMOS, ou None."""
    for rid, s in _recv_state.items():
        if (s.get("vmid") == int(vmid) and s.get("recv_idx") == int(recv_idx)
                and s.get("essence", "video") == essence):
            return rid
    return None


def sender_sid_for(vmid, tx_idx=None, essence="video", audio_idx=None):
    """(vmid, tx_idx, essence[, audio_idx]) → sender_id NMOS courant, ou None.
    C2a : l'id sender vient du registre cluster (plus seedé sur le vmid) ; ce résolveur
    découple les appelants (routes.py page I/O) du vmid → après un recreate « déplacement »
    (même instance_uuid, vmid changé) l'id reste celui du registre."""
    for sid, s in _send_state.items():
        if s.get("vmid") != int(vmid):
            continue
        if s.get("tx_idx") != tx_idx:
            continue
        if s.get("essence", "video") != essence:
            continue
        if essence == "audio" and audio_idx is not None and s.get("audio_idx") != audio_idx:
            continue
        return sid
    return None


def _slot_key_for_state(s, kind):
    """Reconstruit (slot_key, essence_registre) depuis une entrée _send_state/_recv_state.
    essence_registre : video|audio|data (l'ANC interne « anc » → « data » côté registre)."""
    ess = s.get("essence", "video")
    if kind == "sender":
        tx = s.get("tx_idx")
        if ess == "video":
            return (slot_tx(tx, "v") if tx is not None else "v"), "video"
        if ess == "audio":
            ai = s.get("audio_idx", 0)
            return (slot_tx(tx, "a%d" % ai) if tx is not None else slot_rx("a", ai)), "audio"
        if ess == "anc":
            return ((slot_tx(tx, "d") if tx is not None else None), "data")
    else:
        idx = s.get("recv_idx", 0)
        if ess == "video": return slot_rx("v", idx), "video"
        if ess == "audio": return slot_rx("a", idx), "audio"
        if ess == "anc":   return slot_rx("d", idx), "data"
    return None, None


def describe_slots():
    """C2b+ : tous les slots 2110 bindables des conteneurs live (cible des rebinds explicites).
    Renvoie [{vmid, hostname, slot_key, essence, kind, current_id}]. Sert le sélecteur « Servie par »
    de l'éditeur Réglages→NMOS."""
    from app.database import db_get_container
    hn = {}
    def _hn(vmid):
        if vmid not in hn:
            c = db_get_container(vmid) or {}
            hn[vmid] = c.get("hostname") or str(vmid)
        return hn[vmid]
    out = []
    with _lock:
        for uid, s in list(_send_state.items()):
            if s.get("vmid") is None: continue
            sk, ess = _slot_key_for_state(s, "sender")
            if sk: out.append({"vmid": s["vmid"], "hostname": _hn(s["vmid"]), "slot_key": sk,
                               "essence": ess, "kind": "sender", "current_id": uid})
        for uid, s in list(_recv_state.items()):
            if s.get("vmid") is None: continue
            sk, ess = _slot_key_for_state(s, "receiver")
            if sk: out.append({"vmid": s["vmid"], "hostname": _hn(s["vmid"]), "slot_key": sk,
                               "essence": ess, "kind": "receiver", "current_id": uid})
    return out


def _container_slot_keys(params, kind):
    """slot_keys POTENTIELS d'un conteneur (fenêtre active_*), INDÉPENDAMMENT du binding/mode — pour
    l'auto-map et les cartes (en mode statique describe_slots est vide tant qu'on n'a pas câblé).
    Miroir EXACT des conditions d'émission de rebuild_model. Renvoie {video:[], audio:[], data:[]}."""
    out = {"video": [], "audio": [], "data": []}
    from app import io2110_flows as _iof
    if kind == "receiver":
        # Slot_keys = idx réels des flux ACTIFS (rx_flows fait foi ; repli legacy via active_flows).
        rxf = _iof.active_flows(params, "rx")
        out["video"] = [slot_rx("v", f["idx"]) for f in rxf if f["essence"] == "video"]
        out["audio"] = [slot_rx("a", f["idx"]) for f in rxf if f["essence"] == "audio"]
        out["data"]  = [slot_rx("d", f["idx"]) for f in rxf if f["essence"] == "anc"]
    else:                                              # sender
        if params.get("video"):
            out["video"].append("v")
        for ai in range(len(params.get("audios") or [])):
            out["audio"].append(slot_rx("a", ai))
        tx_full = params.get("tx_slots") or []
        atc = params.get("active_tx_count")
        n_tx = min(int(atc if atc is not None else len(tx_full)), len(tx_full))
        for i in range(n_tx):
            ts = tx_full[i] or {}
            if not ts.get("video_off"):           # slot audio-seul / ANC-seul : pas de clé vidéo
                out["video"].append(slot_tx(i, "v"))
            # Plus de cap à 2 : autant de clés audio que de flux audio attachés au slot.
            for ai, acfg in enumerate(ts.get("audios") or []):
                if (acfg or {}).get("multicast_ip"):
                    out["audio"].append(slot_tx(i, "a%d" % ai))
            if ts.get("anc_multicast_ip"):
                out["data"].append(slot_tx(i, "d"))
    return out


def _slot_order(slot_key):
    """Index de tri d'un slot_key (v:3→3, tx2:v→2010, tx1:a0→1100, …)."""
    import re as _re
    m = _re.match(r"^tx(\d+):(.+)$", slot_key)
    if m:
        base, sub = int(m.group(1)), m.group(2)
        sa = _re.match(r"a(\d+)", sub)
        off = (100 + int(sa.group(1))) if sa else (50 if sub == "d" else 0)
        return base * 1000 + off
    m = _re.match(r"^[vad]:(\d+)$", slot_key)
    return int(m.group(1)) if m else 0


def _bundle_mappings(params, kind, pool, include):
    """Construit les mappings d'affectation PAR CANAL (bundle) : pour chaque vidéo, on câble vidéo +
    N audio (`audio_count`) + ANC ensemble, en consommant le pool séquentiellement par essence.
    `include` = {video:bool, audio_count:0|1|2, anc:bool}. Renvoie (mappings, unmatched).
    Grouping audio↔vidéo : RX = `idx % n_video` (cf. rebuild_model) → audio du groupe g = a:{g+j*n_video} ;
    TX = par slot tx{i} (déjà bundlé)."""
    slots = _container_slot_keys(params, kind)
    audio_set, data_set = set(slots["audio"]), set(slots["data"])
    video_keys = sorted(slots["video"], key=_slot_order)
    n_video = len([k for k in slots["video"] if k.startswith("v:")]) or len(video_keys) or 1
    inc_v = bool(include.get("video", True))
    ac    = max(0, min(2, int(include.get("audio_count", 0) or 0)))
    inc_d = bool(include.get("anc", False))
    pv = list(pool.get("video") or []); pa = list(pool.get("audio") or []); pd = list(pool.get("data") or [])
    vi = ai = di = 0
    mappings, unmatched = [], []
    for vk in video_keys:
        if vk.startswith("v:"):                       # RX
            g = _slot_order(vk)
            a_members = [slot_rx("a", g + j * n_video) for j in range(ac)]
            d_member = slot_rx("d", g)
        elif vk.startswith("tx") and vk.endswith(":v"):  # TX moteur
            i = int(vk[2:vk.index(":")])
            a_members = [slot_tx(i, "a%d" % j) for j in range(ac)]
            d_member = slot_tx(i, "d")
        else:                                          # sender vidéo global ("v")
            a_members = [slot_rx("a", j) for j in range(ac)]
            d_member = None
        if inc_v:
            if vi < len(pv): mappings.append({"slot_key": vk, "resource_id": pv[vi]})
            else: unmatched.append(vk)
            vi += 1
        for ak in a_members:
            if ak not in audio_set:
                continue                               # le conteneur n'a pas cet audio sur ce canal
            if ai < len(pa): mappings.append({"slot_key": ak, "resource_id": pa[ai]})
            else: unmatched.append(ak)
            ai += 1
        if inc_d and d_member and d_member in data_set:
            if di < len(pd): mappings.append({"slot_key": d_member, "resource_id": pd[di]})
            else: unmatched.append(d_member)
            di += 1
    return mappings, unmatched


def bindable_containers(kind):
    """Conteneurs 2110 (rôle NMOS) avec leurs slots POTENTIELS pour `kind` + binding courant.
    Sert les cartes de l'UI d'affectation en masse. [{vmid, hostname, node_id, slots:{ess:[keys]},
    bound:{slot_key: resource_id}}]."""
    import json as _json
    from app.database import db_get_containers
    from app import plugins as _plg
    out = []
    for c in db_get_containers():
        try:
            dc = _json.loads(c.get("deploy_config") or "{}")
        except Exception:
            dc = {}
        role = (_plg.REGISTRY.get(dc.get("type")) or {}).get("nmos_role")
        if kind == "receiver" and role not in ("receiver", "both"):
            continue
        if kind == "sender" and role not in ("sender", "both"):
            continue
        params = dc.get("params") or {}
        slots = _container_slot_keys(params, kind)
        if not any(slots.values()):
            continue
        out.append({"vmid": c["vmid"], "hostname": c.get("hostname") or str(c["vmid"]),
                    "node_id": c.get("node_id"), "slots": slots,
                    "bound": params.get("nmos_bind") or {}})
    return out


def active_sdp_for(vmid, recv_idx, essence="video"):
    """SDP actuellement actif sur ce flux (active.transport_file.data) ou None."""
    rid = receiver_rid_for(vmid, recv_idx, essence)
    if not rid:
        return None
    return (_recv_state[rid]["active"].get("transport_file") or {}).get("data")


def manual_subscribe(vmid, recv_idx, essence, sdp, enable=True):
    """Abonnement manuel d'un flux receiver à partir d'un SDP collé. Réutilise la
    chaîne IS-05 standard (_apply_receiver_staged → _activate_receiver → agent).
    Retourne (code, dict)."""
    rid = receiver_rid_for(vmid, recv_idx, essence)
    if not rid:
        return 404, {"error": "receiver introuvable (NMOS pas encore enregistré ?)"}
    body = {
        "master_enable": bool(enable),
        "transport_file": ({"data": sdp, "type": "application/sdp"} if enable
                           else {"data": None, "type": None}),
        "activation": {"mode": "activate_immediate"},
    }
    return _apply_receiver_staged(rid, body)


def _mxl_verrouille(res_id):
    """La ressource `res_id` est-elle une ressource MXL en lecture seule ? (code, payload) ou None.

    ★ CETTE GARDE VIT AU POINT DE PASSAGE, PAS SUR LA ROUTE. Elle a d'abord été posée sur les deux
    routes PATCH unitaires — et les endpoints BULK (`/bulk/receivers`, `/bulk/senders`), qui
    appellent `_apply_*_staged` en direct, la contournaient intégralement. Un garde conditionné à
    QUI APPELLE ne protège que celui-là : il faut le mettre là où tout le monde passe."""
    # IS-07 : un Sender d'événements n'a rien de patchable — sa connexion est décrite par ses
    # transport_params statiques, et sa valeur vient du tally. Le refuser explicitement vaut mieux
    # que de le laisser tomber dans la logique RTP, qui rendrait une erreur incompréhensible.
    if (_send_state.get(res_id) or {}).get("is07"):
        return 405, {"code": 405,
                     "error": "un Sender IS-07 n'est pas patchable : sa connexion est statique et "
                              "son état vient du tally",
                     "debug": res_id}
    from . import mxl as _mxl
    if not _mxl.est_mxl(res_id, _send_state, _recv_state) or _mxl.ecriture_ouverte():
        return None
    return 405, {
        "code": 405,
        "error": "surface MXL en lecture seule : le câblage passe par l'orchestrateur "
                 "(page Câbles). Réglage `nmos_mxl_ecriture` pour lever ce verrou.",
        "debug": res_id,
    }


def _apply_receiver_staged(rid, body):
    """Logique de merge staged + activation pour un receiver. Renvoie (code, dict).
    Utilisé par le PATCH single ET le bulk POST."""
    _refus = _mxl_verrouille(rid)
    if _refus:
        return _refus
    if rid not in _receivers:
        return 404, {"error": "receiver not found", "id": rid}
    with _lock:
        staged = _recv_state[rid]["staged"]
        for k in ("sender_id", "master_enable"):
            if k in body:
                staged[k] = body[k]
        if "transport_params" in body and isinstance(body["transport_params"], list):
            for i, tp in enumerate(body["transport_params"]):
                if i >= len(staged["transport_params"]):
                    staged["transport_params"].append({})
                staged["transport_params"][i].update(tp or {})
        if "transport_file" in body:
            staged["transport_file"] = body["transport_file"] or {"data": None, "type": None}
        if "activation" in body:
            staged["activation"] = body["activation"] or {"mode": None}
        mode = (staged.get("activation") or {}).get("mode")
        if mode == "activate_immediate":
            _activate_receiver(rid)
            staged["activation"]["activation_time"] = _tai_version()
        # activate_scheduled_relative / activate_scheduled_absolute : non géré (phase 1)
        return 200, dict(staged)


def _apply_sender_staged(sid, body):
    """Merge staged + activation pour un sender. Pas de forward agent (le worker
    2110_sender émet en continu selon ses params de deploy)."""
    _refus = _mxl_verrouille(sid)
    if _refus:
        return _refus
    if sid not in _senders:
        return 404, {"error": "sender not found", "id": sid}
    with _lock:
        staged = _send_state[sid]["staged"]
        for k in ("receiver_id", "master_enable"):
            if k in body:
                staged[k] = body[k]
        if "transport_params" in body and isinstance(body["transport_params"], list):
            for i, tp in enumerate(body["transport_params"]):
                if i >= len(staged["transport_params"]):
                    staged["transport_params"].append({})
                staged["transport_params"][i].update(tp or {})
        if "activation" in body:
            staged["activation"] = body["activation"] or {"mode": None}
        mode = (staged.get("activation") or {}).get("mode")
        if mode == "activate_immediate":
            _send_state[sid]["active"] = json.loads(json.dumps(staged))
            staged["activation"]["activation_time"] = _tai_version()
            # MAJ de la subscription côté resource IS-04
            rcv_id = _send_state[sid]["active"].get("receiver_id")
            mast = bool(_send_state[sid]["active"].get("master_enable"))
            if sid in _senders:
                _senders[sid]["subscription"] = {
                    "receiver_id": rcv_id if mast else None,
                    "active": mast,
                }
                _senders[sid]["version"] = _tai_version()
        return 200, dict(staged)


@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/receivers/<rid>/staged", methods=["PATCH"])
def is05_recv_staged_patch(rid):
    body = request.get_json(force=True, silent=True) or {}
    code, payload = _apply_receiver_staged(rid, body)
    return jsonify(payload), code

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/senders/<sid>/staged", methods=["PATCH"])
def is05_send_staged_patch(sid):
    body = request.get_json(force=True, silent=True) or {}
    code, payload = _apply_sender_staged(sid, body)
    return jsonify(payload), code



# ─── IS-05 bulk endpoints ─────────────────────────────────────────

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/bulk/", methods=["GET"])
def is05_bulk_root():
    return jsonify(["senders/", "receivers/"])

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/bulk/receivers", methods=["POST"])
def is05_bulk_recv():
    items = request.get_json(force=True, silent=True)
    if not isinstance(items, list):
        return jsonify({"error": "expected JSON array of {id, params}"}), 400
    results = []
    for it in items:
        rid = (it or {}).get("id")
        params = (it or {}).get("params") or {}
        code, _ = _apply_receiver_staged(rid, params)
        results.append({"id": rid, "code": code})
    return jsonify(results), 200

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/bulk/senders", methods=["POST"])
def is05_bulk_send():
    items = request.get_json(force=True, silent=True)
    if not isinstance(items, list):
        return jsonify({"error": "expected JSON array of {id, params}"}), 400
    results = []
    for it in items:
        sid = (it or {}).get("id")
        params = (it or {}).get("params") or {}
        code, _ = _apply_sender_staged(sid, params)
        results.append({"id": sid, "code": code})
    return jsonify(results), 200

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/senders", methods=["GET"])
def is05_send_list():
    with _lock:
        return jsonify([f"{sid}/" for sid in _senders])

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/senders/<sid>", methods=["GET"])
def is05_send_endpoints(sid):
    if sid not in _senders: abort(404)
    return jsonify(["constraints/", "staged/", "active/", "transportfile/"])

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/senders/<sid>/constraints", methods=["GET"])
def is05_send_constraints(sid):
    if sid not in _senders: abort(404)
    from . import mxl as _mxl
    _c = _mxl.constraints(sid, _send_state, _recv_state)
    if _c is not None:
        return jsonify(_c)
    return jsonify([{}])

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/senders/<sid>/staged", methods=["GET"])
def is05_send_staged_get(sid):
    if sid not in _senders: abort(404)
    with _lock:
        return jsonify(_send_state[sid]["staged"])

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/senders/<sid>/active", methods=["GET"])
def is05_send_active_get(sid):
    if sid not in _senders: abort(404)
    with _lock:
        return jsonify(_send_state[sid]["active"])

@bp.route(f"/x-nmos/connection/{IS05_VERSION}/single/senders/<sid>/transportfile", methods=["GET"])
def is05_send_transportfile(sid):
    """Fetch en live le SDP depuis le container sender (champ `sdp` du /metrics).
    Si PTP est sync côté host, injecte les lignes ts-refclk + mediaclk (conformité 2110)."""
    if sid not in _senders: abort(404)
    from . import mxl as _mxl
    if _mxl.est_mxl(sid, _send_state, _recv_state):
        return ("", 404)          # BCP-007-03 : pas de fichier de transport en MXL
    with _lock:
        vmid = _send_state[sid]["vmid"]
        tx_idx = _send_state[sid].get("tx_idx")   # présent ⇒ sender d'un moteur MTL bi-rôle
        snd_essence = _send_state[sid].get("essence", "video")
        snd_aidx = _send_state[sid].get("audio_idx")   # désambiguïse les 2 flux audio d'un même slot
    if vmid is None:   # C2b : ressource orpheline (servie par aucun conteneur live) → pas de SDP
        return ("sender orphelin (servi par aucun conteneur)", 503)
    from app.addressing import get_container_ip
    ip = get_container_ip(vmid)
    if not ip:
        return ("container IP introuvable", 503)
    try:
        r = requests.get(f"http://{ip}:8080", timeout=2)
        if r.status_code != 200:
            return (f"metrics HTTP {r.status_code}", 503)
        data = r.json() or {}
    except Exception as e:
        return (f"metrics fetch failed: {e}", 503)
    if tx_idx is not None:
        # Moteur MTL : SDP par slot TX + essence (vidéo 2110-20 / audio 2110-30 / ANC 2110-40),
        # exposé dans metrics.senders[].sdp (généré par le contrôleur). On désambiguïse par tx_idx +
        # essence, et par audio_idx pour les 2 flux audio d'un même slot.
        sdp = next((s.get("sdp") for s in (data.get("senders") or [])
                    if s.get("tx_idx") == tx_idx and s.get("essence", "video") == snd_essence
                    and (snd_essence != "audio" or s.get("audio_idx") == snd_aidx)), "") or ""
    else:
        sdp = data.get("sdp") or ""
    if not sdp:
        return ("sdp non encore disponible (slot TX pas câblé / pas d'émission ?)", 404)

    # Upgrade PTP : le conteneur émet a=ts-refclk:localmac par section média (repli d'horloge).
    # On le REMPLACE par la forme traçable a=ts-refclk:ptp=IEEE1588-2008:<gm>:<domain> lue du ptp4l
    # qui discipline RÉELLEMENT le nœud du sender — pas le conteneur (qui ne gère pas le PTP). Pour
    # un moteur MTL (tx_idx), ce ptp4l tourne sur l'HÔTE du nœud (= `ip`, --network host) → on
    # l'interroge par SSH pmc (caché, app/ptp.refclk_for_host), DÉCOUPLÉ de ptp_enabled (le nœud MTL
    # a son ptp4l quoi qu'il arrive). Sinon (LXC legacy) : modèle host-ptp global gated ptp_enabled.
    # Par section, dual-leg compris ; repli insertion avant a=mediaclk si pas de localmac.
    try:
        from app import settings as st, ptp as _ptp
        from app.addressing import primary_host as _primary_host
        if tx_idx is not None:
            refclk = _ptp.refclk_for_host(ip)              # ptp4l du nœud du sender (domaine = réglage)
            if not refclk:
                # Socle full-PF DPDK : plus de ptp4l kernel (port en vfio, PTP dans libmtl) → pmc
                # ne voit rien. Repli : le grandmaster verrouillé par le PTP interne, relayé par le
                # contrôleur sur :8080 (champ ptp). Garantit un SDP TX conforme (a=ts-refclk:ptp).
                refclk = _ptp.refclk_from_engine(ip)
        elif st.get("ptp_enabled"):
            refclk = _ptp.sdp_refclk_lines(_primary_host())   # B1a : hôte du nœud (B1b : par-nœud)
        else:
            refclk = ""
        ptp_line = next((ln for ln in refclk.splitlines() if "ts-refclk" in ln), "")
        if ptp_line:
            ptp_line += "\r\n"
            if "ts-refclk:localmac=" in sdp:
                sdp = re.sub(r"a=ts-refclk:localmac=\S+\r?\n", ptp_line, sdp)
            elif "ts-refclk" not in sdp:
                if "a=mediaclk" in sdp:
                    sdp = re.sub(r"(a=mediaclk)", ptp_line + r"\1", sdp)
                else:
                    if not sdp.endswith("\n"):
                        sdp += "\r\n"
                    sdp += ptp_line
    except Exception as e:
        log.warning(f"sdp ptp injection skipped: {e}")
    return (sdp, 200, {"Content-Type": "application/sdp"})


_AUTO_ACTIVATE_HTTP_TIMEOUT = 2  # timeout par requête HTTP (curl) vers le device distant
# Ports d'API Node NMOS connus pour certains devices (ex. convertisseurs SDI→IP Blackmagic-like)
# — essayés en direct sur l'IP source du SDP (pas de découverte mDNS : le plan média 2110 est une
# NIC dédiée, non routée depuis le contrôleur — cf. _node_host_for_vmid). Le convertisseur
# "IP-Converter-8x12G-SFP" expose UNE API NMOS PAR CANAL SDI (pas 1 API listant 8 senders) : 1
# port par canal (8090=SDI1, 8092=SDI2, … pas+2), le tout joignable depuis N'IMPORTE LAQUELLE de
# ses IP (boîtier unique multi-homé — cf. [[io2110-blackmagic-is05-sender-activation]]).
_KNOWN_NMOS_API_PORTS = (8090, 8092, 8094, 8096, 8098, 8100, 8102, 8104)


def _sdp_endpoint(sdp):
    """Lit (ip_source, ip_multicast, port) d'un SDP RX vidéo 2110-20 — `a=source-filter` prime sur
    `o=` (plus fiable, cf. devices dont le `o=`/self-report est faux, [[io2110-blackmagic-is05-sender-activation]])."""
    src_ip = None
    m = re.search(r"^o=\S+ \S+ \S+ IN IP4 (\S+)", sdp, re.M)
    if m: src_ip = m.group(1)
    sf = re.search(r"a=source-filter:incl IN IP4 \S+ (\S+)", sdp)
    if sf: src_ip = sf.group(1)
    mcast_ip = None
    c = re.search(r"^c=IN IP4 (\S+?)(?:/\d+)?\s*$", sdp, re.M)
    if c: mcast_ip = c.group(1)
    port = None
    ml = re.search(r"^m=\S+ (\d+)", sdp, re.M)
    if ml: port = int(ml.group(1))
    return src_ip, mcast_ip, port


def _node_host_for_vmid(vmid):
    """Hôte SSH/agent du nœud hébergeant `vmid`, ou None. Le plan média 2110 (NIC dédiée) n'est
    routable QUE depuis le nœud lui-même — un `curl`/`requests` exécuté depuis le process
    contrôleur vers le device distant timeout systématiquement (confirmé en diagnostic, séance
    2026-07-01) même quand une route L3 existe. Toute requête vers un sender NMOS distant doit
    donc être exécutée via SSH/agent SUR LE NŒUD, jamais directement depuis le contrôleur."""
    from app.database import db_get_container, db_get_node
    c = db_get_container(vmid)
    if not c or not c.get("node_id"):
        return None
    node = db_get_node(c["node_id"])
    return node.get("host") if node else None


def _curl_json(host, url, timeout=_AUTO_ACTIVATE_HTTP_TIMEOUT):
    """GET JSON via curl exécuté SUR `host` (SSH/agent, cf. `_node_host_for_vmid`). None si
    rc != 0 ou réponse non-JSON."""
    from app.host_ops import ssh_run
    rc, out, _ = ssh_run(host, f"curl -s -m {timeout} '{url}'", timeout=timeout + 5)
    if rc != 0 or not out:
        return None
    try:
        return json.loads(out)
    except Exception:
        return None


def _curl_text(host, url, timeout=_AUTO_ACTIVATE_HTTP_TIMEOUT):
    from app.host_ops import ssh_run
    rc, out, _ = ssh_run(host, f"curl -s -m {timeout} '{url}'", timeout=timeout + 5)
    return out if rc == 0 and out else None


def _curl_patch(host, url, body, timeout=_AUTO_ACTIVATE_HTTP_TIMEOUT):
    from app.host_ops import ssh_run
    payload = json.dumps(body).replace("'", "'\\''")
    cmd = (f"curl -s -o /dev/null -w '%{{http_code}}' -m {timeout} -X PATCH "
           f"-H 'Content-Type: application/json' -d '{payload}' '{url}'")
    rc, out, err = ssh_run(host, cmd, timeout=timeout + 5)
    return rc == 0 and (out or "").strip().startswith("2")


def _try_activate_sender_on_ports(host, vmid, recv_idx, src_ip, mcast_ip, port, node_ports):
    """Essaie chaque port d'API Node candidat sur `src_ip` (via curl SUR `host`, cf.
    `_node_host_for_vmid`), cherche le sender dont le SDP matche (mcast, port) et l'active
    (PATCH master_enable) si besoin. Retourne un dict détaillé (utilisé par le best-effort auto
    ET par l'activation manuelle) :
    {matched: bool, activated: bool, already_active: bool, sender_id, base, tried_ports}."""
    tried = []
    for node_port in node_ports:
        base = f"http://{src_ip}:{node_port}"
        tried.append(node_port)
        senders = _curl_json(host, f"{base}/x-nmos/node/v1.3/senders/")
        if not senders:
            continue
        for snd in senders:
            sid, href = snd.get("id"), snd.get("manifest_href")
            if not sid or not href:
                continue
            sdp_text = _curl_text(host, href)
            if not sdp_text:
                continue
            _, r_mcast, r_port = _sdp_endpoint(sdp_text)
            if r_mcast != mcast_ip or r_port != port:
                continue
            st = _curl_json(host, f"{base}/x-nmos/connection/v1.0/single/senders/{sid}/staged") or {}
            if st.get("master_enable"):
                log.info(f"nmos: auto-activation vmid={vmid} idx={recv_idx} — sender {sid}@{base} déjà actif")
                return {"matched": True, "activated": False, "already_active": True,
                        "sender_id": sid, "base": base, "tried_ports": tried}
            ok = _curl_patch(host, f"{base}/x-nmos/connection/v1.0/single/senders/{sid}/staged",
                              {"master_enable": True, "activation": {"mode": "activate_immediate"}})
            if ok:
                log.info(f"nmos: auto-activation vmid={vmid} idx={recv_idx} — sender {sid}@{base} activé (IS-05)")
            else:
                log.warning(f"nmos: auto-activation vmid={vmid} idx={recv_idx} — PATCH {sid}@{base} échoué")
            return {"matched": True, "activated": ok, "already_active": False,
                    "sender_id": sid, "base": base, "tried_ports": tried}
    return {"matched": False, "activated": False, "already_active": False,
            "sender_id": None, "base": None, "tried_ports": tried}


def _activate_remote_sender_if_needed(vmid, recv_idx, sdp):
    """Best-effort : certains devices 2110 (ex. convertisseurs SDI→IP) exposent une vraie API
    NMOS IS-04/05 mais n'émettent RIEN tant que leur propre sender n'a pas reçu le PATCH IS-05
    d'activation (master_enable) — comportement NMOS standard, pas propriétaire. Un abonnement RX
    fait ici à la main (SDP collé, pas de registry commun) ne déclenche jamais ce PATCH. Essaie les
    ports d'API Node CONNUS (_KNOWN_NMOS_API_PORTS) directement sur l'IP source du SDP — TOUJOURS
    via SSH/agent sur le nœud hébergeant le receiver (le plan média n'est pas routable depuis le
    contrôleur, cf. [[io2110-blackmagic-is05-sender-activation]]). Gated par le réglage
    `nmos_auto_activate_senders`. N'écrit RIEN côté Bobi ; toute erreur est avalée (ne doit jamais
    perturber l'abonnement RX lui-même). Voir `manual_activate_remote_sender` pour le déclenchement
    manuel (bouton), qui remonte le résultat au lieu de l'avaler."""
    if not _setting("nmos_auto_activate_senders", False):
        return
    try:
        src_ip, mcast_ip, port = _sdp_endpoint(sdp)
        if not src_ip or not mcast_ip or not port:
            return
        host = _node_host_for_vmid(vmid)
        if not host:
            log.info(f"nmos: auto-activation vmid={vmid} idx={recv_idx} — nœud introuvable pour ce vmid")
            return
        res = _try_activate_sender_on_ports(host, vmid, recv_idx, src_ip, mcast_ip, port, _KNOWN_NMOS_API_PORTS)
        if not res["matched"]:
            log.info(f"nmos: auto-activation vmid={vmid} idx={recv_idx} — aucun sender distant (via {host}) "
                     f"ne correspond à {mcast_ip}:{port}")
    except Exception as e:
        log.warning(f"nmos: auto-activation vmid={vmid} idx={recv_idx} échouée : {e}")


def manual_activate_remote_sender(vmid, recv_idx, essence="video"):
    """Déclenchement MANUEL (bouton « Activer IS-05 ») : reprend la même logique que le
    best-effort auto (_activate_remote_sender_if_needed) mais NE L'AVALE PAS — retourne un dict
    exploitable par l'UI pour expliquer précisément ce qui s'est passé (pas de nœud NMOS trouvé,
    sender introuvable, PATCH échoué, etc.), utile vu la fiabilité variable du matériel concerné
    (cf. [[io2110-blackmagic-is05-sender-activation]]). Ignore le réglage `nmos_auto_activate_senders`
    (un clic explicite doit toujours tenter, réglage ou pas)."""
    sdp = active_sdp_for(vmid, recv_idx, essence)
    if not sdp:
        return {"ok": False, "error": "aucun SDP actif sur ce flux"}
    src_ip, mcast_ip, port = _sdp_endpoint(sdp)
    if not src_ip or not mcast_ip or not port:
        return {"ok": False, "error": "SDP illisible (source/multicast/port introuvables)"}
    host = _node_host_for_vmid(vmid)
    if not host:
        return {"ok": False, "error": "nœud introuvable pour ce conteneur"}
    try:
        res = _try_activate_sender_on_ports(host, vmid, recv_idx, src_ip, mcast_ip, port, _KNOWN_NMOS_API_PORTS)
    except Exception as e:
        return {"ok": False, "error": f"erreur : {e}"}
    if not res["matched"]:
        return {"ok": False, "error": f"aucun sender distant (via {host}, ports {res['tried_ports']}) "
                                       f"ne correspond à {mcast_ip}:{port}"}
    return {"ok": True, "already_active": res["already_active"], "activated": res["activated"],
            "sender_id": res["sender_id"], "base": res["base"]}


def _activate_receiver(rid):
    """Promote staged → active, parse le SDP éventuel, notifie l'agent."""
    state = _recv_state[rid]
    # Copie profonde du staged dans active
    state["active"] = json.loads(json.dumps(state["staged"]))
    # Mettre à jour la subscription côté resource IS-04
    sender_id = state["active"].get("sender_id")
    master_enable = bool(state["active"].get("master_enable"))
    with _lock:
        if rid in _receivers:
            _receivers[rid]["subscription"] = {
                "sender_id": sender_id if master_enable else None,
                "active": master_enable,
            }
            _receivers[rid]["version"] = _tai_version()

    # Forward vers l'agent
    vmid = state["vmid"]
    recv_idx = state["recv_idx"]
    essence  = state.get("essence", "video")
    sdp = (state["active"].get("transport_file") or {}).get("data")
    dual = len(state["active"].get("transport_params") or []) >= 2
    mcast_info = _extract_mcast_info(state["active"], smpte_2022_7=dual)
    # Conformité IS-05 : les paramètres issus du transport_file PRIMENT et doivent être VISIBLES
    # dans /active.transport_params (multicast_ip/destination_port/source_ip). Sans ce back-fill,
    # un contrôleur externe voit multicast_ip=null et croit le receiver non abonné alors que le
    # flux est reçu (constaté au diagnostic Horace 2026-07).
    infos = mcast_info if isinstance(mcast_info, list) else [mcast_info]
    tps = state["active"].get("transport_params") or []
    for i, inf in enumerate(infos):
        if i < len(tps) and isinstance(inf, dict):
            tps[i].update({k: v for k, v in inf.items() if v is not None})
    with _lock:
        if rid in _receivers:
            _receivers[rid]["version"] = _tai_version()
    threading.Thread(
        target=_notify_agent,
        args=(vmid, recv_idx, essence, master_enable, sdp, mcast_info),
        daemon=True
    ).start()
    # Propage le format RÉEL (lu du SDP) dans le deploy_config → topologie/câblage/consommateurs
    # (mixer, multiview…) voient le vrai WxH/fps reçu, pas seulement l'affichage I/O. Stream
    # auto-détecte déjà depuis le shm ; ceci aligne les autres. En thread (hors _lock NMOS).
    if essence == "video" and master_enable and sdp:
        threading.Thread(target=_propagate_sdp_format, args=(vmid, sdp, recv_idx), daemon=True).start()
    # Active le sender NMOS distant si besoin (device armé mais jamais activé, réglage opt-in).
    if master_enable and sdp:
        threading.Thread(target=_activate_remote_sender_if_needed, args=(vmid, recv_idx, sdp), daemon=True).start()
    # Persiste l'état d'abonnement (survie au redémarrage de l'orchestrateur).
    _persist_subscriptions()

def _propagate_sdp_format(vmid, sdp, recv_idx=None, slot_only=False):
    """Écrit width/height/fps/scan lus du SDP dans le deploy_config du receiver (si différent),
    puis notify_state_change → la topologie et les consommateurs voient le vrai format reçu.
    Modèle PAR-FLUX : `params['rx_fmt'][str(recv_idx)]` porte le format RÉEL de CHAQUE entrée RX
    (un moteur 2110_io multi-entrées peut mélanger des sources progressives ET entrelacées → le
    scan global « dernier flux activé fait foi » écrasait les autres ; cf. hooks.topology_ports qui
    lit ce format par-port). Le format GLOBAL (width/height/scan) reste mis à jour pour compat."""
    from app.database import db_get_container, db_update_deploy_config
    w  = re.search(r"width=(\d+)", sdp)
    h  = re.search(r"height=(\d+)", sdp)
    fr = re.search(r"exactframerate=(\d+)(?:/(\d+))?", sdp)
    if not (w and h):
        return
    c = db_get_container(vmid)
    if not c:
        return
    dc = c.get("deploy_config")
    try:
        dc = json.loads(dc) if isinstance(dc, str) else dc
    except Exception:
        dc = None
    if not dc:
        return
    params = dict(dc.get("params") or {})
    new_w, new_h = int(w.group(1)), int(h.group(1))
    scan = "i" if re.search(r"\binterlace\b", sdp) else "p"
    # Ordre de champ : le SDP 2110-20 ne le porte pas explicitement → défaut par résolution
    # (helper central : 1080i = TFF, 576i = BFF). Progressif → pas de field_order.
    from app.scripts import field_order as _field_order
    new_fo = _field_order({"scan": scan, "height": new_h}) if scan == "i" else ""
    new_fps = params.get("fps")
    if fr:
        num = int(fr.group(1)); den = int(fr.group(2)) if fr.group(2) else 1
        new_fps = round(num / den, 2)
    # Format PAR-FLUX (rx_fmt[idx]) : keyé par recv_idx (= idx vidéo = shm {hostname}_{idx}).
    slot_changed = False
    if recv_idx is not None:
        rx_fmt = dict(params.get("rx_fmt") or {})
        slot = {"width": new_w, "height": new_h, "scan": scan, "field_order": new_fo}
        if new_fps:
            slot["fps"] = new_fps
        if rx_fmt.get(str(recv_idx)) != slot:
            rx_fmt[str(recv_idx)] = slot
            params["rx_fmt"] = rx_fmt
            slot_changed = True
    global_changed = (int(params.get("width") or 0) != new_w
               or int(params.get("height") or 0) != new_h
               or str(params.get("scan") or "") != scan
               or str(params.get("field_order") or "") != new_fo)
    # `slot_only` (repropagation au boot) : ne TOUCHE PAS le format global (width/height/scan) du
    # container — il pourrait, en bouclant sur N flux, basculer le scan global (utilisé p.ex. côté
    # TX). On ne fait qu'enrichir rx_fmt par-flux.
    # ★ Sur un moteur à PLUSIEURS entrées vidéo, le format « global » ne décrit AUCUN flux en
    # particulier — et le laisser suivre le dernier récepteur activé le rend non déterministe : un
    # resync qui réabonne six entrées dont la dernière est 1080i50 fait basculer tout le container à
    # 25 fps « i ». Le 2026-07-27, `docker_driver._auto_lcores` s'y fiait et a dimensionné le moteur
    # pour la moitié du débit réel : sessions refusées, six RX mortes. Le garde `slot_only` existait
    # déjà pour la repropagation au boot, avec exactement ce motif — il manquait au chemin normal.
    # Mono-flux : le global EST ce flux, aucune ambiguïté, on garde le comportement historique.
    _multi = False
    try:
        from app import io2110_flows as _iof
        _multi = len([f for f in _iof.active_flows(params, "rx")
                      if f.get("essence") == "video"]) > 1
    except Exception as _e:
        log.debug("nmos: comptage des flux RX vidéo (vmid=%s) : %s", vmid, _e)
    write_global = global_changed and not slot_only and not _multi
    if not (write_global or slot_changed):
        return
    if write_global:
        params["width"] = new_w; params["height"] = new_h
        params["scan"] = scan
        params["field_order"] = new_fo
        if new_fps:
            params["fps"] = new_fps
    db_update_deploy_config(vmid, dc.get("type"), params)
    try:
        notify_state_change()
    except Exception as e:
        log.warning("nmos: notify après propagation format vmid=%s: %s", vmid, e)
    # Format source changé EN DIRECT (re-souscription / nouveau SDP) → rafraîchir à chaud les
    # consommateurs (multiview : in_w/in_h + /reconfigure, + reconcile pyramide). Différé pour
    # laisser le shm du RX se recréer à la nouvelle taille. Le câble/deploy_config porte le format.
    try:
        from app.deploy import _schedule_consumer_refresh
        _schedule_consumer_refresh(vmid)
    except Exception as e:
        log.warning("nmos: refresh consommateurs après format vmid=%s: %s", vmid, e)


def _build_dual_sdp(sdp_leg0, mcast1, port1):
    """Greffe une deuxième section m= (leg1) sur sdp_leg0 pour SMPTE 2022-7.
    Retourne un unique SDP avec deux blocs media (leg0 + leg1), groupés RFC 7104
    (a=group:DUP au niveau session + a=mid: par section — ce que produisent aussi les
    SDP TX du moteur et qu'attend un analyseur de labo)."""
    if not sdp_leg0 or not mcast1 or not port1:
        return sdp_leg0 or ""
    m = re.search(r"(m=(?:video|audio|application) )", sdp_leg0)
    if not m:
        return sdp_leg0
    nl = "\r\n" if "\r\n" in sdp_leg0 else "\n"
    head, media_block = sdp_leg0[:m.start()], sdp_leg0[m.start():]
    # Idempotent : un transport_file déjà dual-section (a=group:DUP) est renvoyé tel quel.
    if re.search(r"^a=group:DUP", sdp_leg0, re.M) or len(re.findall(r"^m=", sdp_leg0, re.M)) > 1:
        return sdp_leg0
    leg1 = re.sub(r"(m=(?:video|audio|application) )\d+", rf"\g<1>{int(port1)}", media_block)
    leg1 = re.sub(r"(c=IN IP4 )[\d.]+", rf"\g<1>{mcast1}", leg1)
    leg1 = re.sub(r"(a=source-filter: incl IN IP4 )[\d.]+", rf"\g<1>{mcast1}", leg1)
    if not head.endswith(nl):
        head += nl
    head += "a=group:DUP PRIMARY SECONDARY" + nl
    if not media_block.endswith(nl):
        media_block += nl
    if not leg1.endswith(nl):
        leg1 += nl
    return head + media_block + "a=mid:PRIMARY" + nl + leg1 + "a=mid:SECONDARY" + nl

def _extract_mcast_info(active, smpte_2022_7=False):
    """Résume les transport_params + SDP.
    Single-path → dict {multicast_ip, destination_port, source_ip}.
    Dual-path (smpte_2022_7=True) → liste de 2 dicts."""
    tps = active.get("transport_params") or [{}]
    tp0 = tps[0]
    info0 = {
        "multicast_ip": tp0.get("multicast_ip"),
        "destination_port": tp0.get("destination_port"),
        "source_ip": tp0.get("source_ip"),
    }
    sdp = (active.get("transport_file") or {}).get("data")
    if sdp:
        m_c = re.search(r"c=IN IP4 ([\d.]+)", sdp)
        m_m = re.search(r"m=video (\d+)", sdp)
        # SSM (source-specific) : `a=source-filter: incl IN IP4 <mcast> <source>` → l'IGMPv3 join
        # a besoin de l'IP source. Sans elle, source_ip reste null et le join SSM est bancal.
        m_sf = re.search(r"a=source-filter:\s*incl\s+IN\s+IP4\s+(\S+)\s+([\d.]+)", sdp)
        if m_c and not info0["multicast_ip"]:
            info0["multicast_ip"] = m_c.group(1)
        if m_m and not info0["destination_port"]:
            info0["destination_port"] = int(m_m.group(1))
        if m_sf and not info0["source_ip"]:
            info0["source_ip"] = m_sf.group(2)
    if not smpte_2022_7 or len(tps) < 2:
        return info0
    tp1 = tps[1]
    info1 = {
        "multicast_ip": tp1.get("multicast_ip"),
        "destination_port": tp1.get("destination_port"),
        "source_ip": tp1.get("source_ip"),
    }
    return [info0, info1]

def _notify_agent(vmid, recv_idx, essence, enable, sdp, mcast_info):
    """POST l'info de subscription à l'agent du container (port 8081).

    Renvoie True SEULEMENT si l'agent a répondu 200. Cette valeur de retour EXISTE parce que
    l'appelant en a besoin : sans elle, `repush_subscriptions` comptait ses appels et non ses
    livraisons, et annonçait « 16 abonnements re-poussés » alors que ZÉRO n'était arrivé. Cf.
    l'incident du 2026-08-15, où le moteur est resté sans aucune entrée vidéo pendant que la
    restauration se déclarait réussie."""
    from app.addressing import get_container_ip
    from app.database import db_add_alert
    ip = get_container_ip(vmid)
    if not ip:
        log.warning(f"nmos: pas d'IP pour container {vmid}, subscription ignorée")
        db_add_alert(f"NMOS subscription receiver #{recv_idx + 1}/{essence} container {vmid}: IP introuvable", "warning", vmid=vmid, kind="rx_stall")
        return False
    # ⚠ CONTRAT DE L'AGENT : `/nmos/subscribe` n'écrit le fichier SDP que si `enabled ET sdp`
    # (cf. plugins/2110_io/docker/controller.py) — et répond `{"ok": true}` dans TOUS les cas.
    # Une activation sans SDP est donc un NO-OP qui se présente comme un succès, et elle DÉTRUIT
    # la session existante au passage. On refuse ici plutôt que de laisser passer.
    if enable and not sdp:
        log.warning(f"nmos: activation receiver #{recv_idx + 1}/{essence} container {vmid} SANS SDP — refusée")
        db_add_alert(f"NMOS receiver #{recv_idx + 1}/{essence} container {vmid} : activation sans SDP "
                     f"refusée (l'agent l'aurait acceptée sans rien faire, en supprimant la session)",
                     "warning", vmid=vmid, kind="rx_stall")
        return False
    if isinstance(mcast_info, list):
        # SMPTE 2022-7 : un unique SDP avec deux sections m= (leg0 + leg1)
        info0, info1 = mcast_info[0], mcast_info[1]
        dual_sdp = _build_dual_sdp(sdp, info1.get("multicast_ip"), info1.get("destination_port")) if sdp else sdp
        payload = {
            "receiver_index": recv_idx,
            "essence": essence,
            "enabled": enable,
            "sdp": dual_sdp,
            "multicast_ip": info0.get("multicast_ip"),
            "destination_port": info0.get("destination_port"),
            "source_ip": info0.get("source_ip"),
        }
    else:
        payload = {
            "receiver_index": recv_idx,
            "essence": essence,
            "enabled": enable,
            "sdp": sdp,
            "multicast_ip": mcast_info.get("multicast_ip"),
            "destination_port": mcast_info.get("destination_port"),
            "source_ip": mcast_info.get("source_ip"),
        }
    try:
        from app import deploy
        # Port du contrat agent : :8081 par défaut, offsetté (:base+1) pour une sonde probe_2110
        # coexistant avec un moteur sur le même nœud (--network host, cf. controller_port_base).
        _aport = deploy.agent_port(vmid)
        # En-tête d'auth OBLIGATOIRE : depuis que MXL_AGENT_TOKEN est réellement injecté au
        # `docker run`, un agent sous token répond 401 sans lui. Inoffensif aujourd'hui — cette
        # route vise le contrôleur du moteur 2110, qui ne lit pas encore le token — mais l'oublier
        # ferait casser TOUS les abonnements NMOS le jour où l'image l'implémentera.
        r = deploy.agent_session().post(deploy.agent_url(ip, "/nmos/subscribe", port=_aport),
                          json=payload, timeout=5, headers=deploy.agent_headers(vmid))
        if r.status_code == 200:
            # Le fil `alerts` est AUSSI le journal d'exploitation (décision 2026-07-27) : une
            # (dés)inscription doit y rester visible, en `info`. Quand elle vient d'une action
            # utilisateur, `db_add_alert` y attache l'acteur tout seul (contexte de requête) ;
            # les rafales de la réconciliation automatique du moteur restent anonymes = machine.
            # C'est la rétention qui a été portée à 10 000 lignes pour que ce journal serve à
            # quelque chose, pas le volume qui a été coupé.
            db_add_alert(
                f"NMOS receiver #{recv_idx + 1}/{essence} container {vmid} → "
                f"{'subscribe' if enable else 'unsubscribe'}", "info", vmid=vmid, kind="rx_stall")
            return True
        else:
            log.warning(f"nmos: agent {vmid} a renvoyé {r.status_code}")
            db_add_alert(
                f"NMOS subscription container {vmid} : agent retour {r.status_code}",
                "warning", vmid=vmid, kind="agent")
            return False
    except Exception as e:
        log.warning(f"nmos: notification agent {vmid} échouée : {e}")
        db_add_alert(f"NMOS subscription container {vmid} : agent injoignable ({e})", "warning", vmid=vmid, kind="agent")
        return False


def subscriptions_actives(vmid):
    """Abonnements RX ACTIFS (master_enable) que l'ORCHESTRATEUR croit posés sur `vmid` :
    [(receiver_id, state), …]. Source unique de vérité de « ce que ce moteur DEVRAIT recevoir » —
    consommée par `repush_subscriptions` (quoi re-pousser) ET par la vérification post-resync /
    le détecteur permanent de `metrics` (moteur revenu vide après un redéploiement).

    Source : `_recv_state` (process du SERVICE), avec REPLI sur l'état persisté (setting
    nmos_subscriptions). Sans le repli, un deployer_script lancé HORS du process du service
    (script d'ops, CLI) recréait le conteneur mais ne repoussait RIEN (_recv_state vide dans ce
    process) → moteur muet jusqu'au prochain restart de l'orchestrateur (vu 2026-07-13 au
    redéploiement du 141). Le persisté est écrit à chaque (dés)activation → à jour."""
    entries = [(rid, st) for rid, st in _recv_state.items() if st.get("vmid") == vmid]
    if not any(bool((st.get("active") or {}).get("master_enable")) for _, st in entries):
        import json as _json
        try:
            subs = _json.loads(_setting("nmos_subscriptions", "") or "{}") or {}
        except Exception:
            subs = {}
        entries = [(rid, st) for rid, st in subs.items() if st.get("vmid") == vmid]
    return [(rid, st) for rid, st in entries
            if bool((st.get("active") or {}).get("master_enable"))]


def nb_sessions_rx_attendues(vmid, essence="video"):
    """Nombre de sessions RX d'une essence que le moteur `vmid` DEVRAIT servir (abonnements IS-05
    actifs côté orchestrateur). 0 = rien d'attendu (un moteur vide est alors NORMAL)."""
    return sum(1 for _, st in subscriptions_actives(vmid)
               if (st.get("essence") or "video") == essence)


def repush_subscriptions(vmid):
    """Re-pousse vers le contrôleur (:8081/nmos/subscribe) TOUS les abonnements RX ACTIFS du
    container `vmid`. Appelé après une (re)création du conteneur : ses fichiers SDP repartent de
    zéro, mais `_recv_state` survit en mémoire (l'orchestrateur, lui, ne redémarre pas) → on
    restaure les sessions RX SANS intervention du contrôleur NMOS externe (corrige la perte des
    flux à chaque redéploiement). Attend d'abord que le contrôleur :8081 réponde (readiness
    BORNÉE PAR UNE ÉCHÉANCE — cf. deploy.attendre_controleur_pret : l'ancienne boucle
    `for _ in range(30)` + sleep(1) épuisait son budget en ~30 s sur un port fermé, alors qu'un
    moteur 2110 recréé met 30-60 s à servir :8081 → les subscribes partaient dans le vide et le
    moteur revenait VIDE, silencieusement).

    Renvoie le nombre d'abonnements RÉELLEMENT LIVRÉS (agent répondu 200).

    ⚠ Il comptait auparavant les APPELS, pas les livraisons — `_notify_agent` ne rendant rien et
    avalant ses exceptions. Résultat vécu le 2026-08-15 : « 16 abonnements re-poussés » dans le
    journal, ZÉRO fichier SDP écrit côté moteur, et six entrées vidéo mortes pendant que la
    restauration se déclarait réussie. Un compteur qui ne peut pas décroître ne mesure rien.

    ⚠ CE MÉCANISME N'A JAMAIS ÉTÉ EN CAUSE dans les redéploiements laborieux du 2026-08-05..15,
    contrairement à ce qu'affirme le message du commit qui a corrigé le compteur. La cause réelle
    était le MODE DE DÉPLOIEMENT : des recréations lancées par script d'ops, hors du process Flask,
    qui ne font pas tourner le thread `resync_moteur` — donc personne n'appelait ceci. Vérifié
    depuis, par l'API : « 16/16 abonnement(s) RX livré(s) », 6/6 sessions RX actives sur un moteur
    FRAÎCHEMENT RECRÉÉ (le seul vrai test, alors annoncé comme non fait). Diagnostiquer un
    redéploiement muet en regardant d'abord PAR OÙ il a été lancé."""
    from app.addressing import get_container_ip
    from app import deploy
    ip = get_container_ip(vmid)
    if not ip:
        return 0
    if not deploy.attendre_controleur_pret(ip, vmid=vmid):
        from app.database import db_add_alert
        db_add_alert(f"NMOS {vmid} : contrôleur :8081 injoignable — abonnements RX NON restaurés "
                     f"(moteur potentiellement VIDE). Redéployer le moteur.", "error", vmid=vmid, kind="agent")
        return 0
    n = 0
    echecs = []
    entries = subscriptions_actives(vmid)
    for rid, state in entries:
        active = state.get("active") or {}
        sdp = (active.get("transport_file") or {}).get("data")
        dual = len(active.get("transport_params") or []) >= 2
        mcast_info = _extract_mcast_info(active, smpte_2022_7=dual)
        _ess = state.get("essence", "video")
        if _notify_agent(state["vmid"], state["recv_idx"], _ess, True, sdp, mcast_info):
            n += 1
        else:
            echecs.append("#%s/%s%s" % (state["recv_idx"] + 1, _ess, "" if sdp else " (sans SDP)"))
    if echecs:
        # Une restauration partielle est une PANNE, pas un détail : le moteur redémarre sans les
        # entrées qu'on croyait rétablies, et plus rien ne le signale ensuite.
        from app.database import db_add_alert
        log.error("nmos: %d/%d abonnement(s) NON restauré(s) sur container %s : %s",
                  len(echecs), len(entries), vmid, ", ".join(echecs))
        db_add_alert(f"NMOS {vmid} : {len(echecs)}/{len(entries)} abonnement(s) RX NON restauré(s) "
                     f"({', '.join(echecs[:6])}) — le moteur tourne SANS ces entrées",
                     "error", vmid=vmid, kind="rx_stall")
    if n:
        log.info(f"nmos: {n}/{len(entries)} abonnement(s) RX livré(s) sur container {vmid} après (re)déploiement")
    return n


def _persist_subscriptions():
    """Sauvegarde en DB (settings) les abonnements RX ACTIFS (master_enable) → survivent à un
    redémarrage de l'orchestrateur (l'état IS-05 vient du contrôleur externe, absent du deploy_config).
    Appelé à chaque (dés)activation. _lock est ré-entrant (RLock) : sûr depuis _activate_receiver."""
    import json as _json
    from app.database import db_set_setting
    out = {}
    with _lock:
        for rid, state in _recv_state.items():
            active = state.get("active") or {}
            if bool(active.get("master_enable")):
                out[rid] = {"vmid": state.get("vmid"), "recv_idx": state.get("recv_idx"),
                            "essence": state.get("essence"), "active": active}
    try:
        db_set_setting("nmos_subscriptions", _json.dumps(out))
    except Exception as e:
        log.warning(f"nmos: persistance abonnements échouée : {e}")


def _restore_persisted_subscriptions():
    """Au boot (après rebuild_model) : recharge les abonnements RX persistés dans _recv_state, met à
    jour la subscription IS-04, puis re-pousse vers chaque contrôleur (en fond, attend la readiness).
    Restaure les flux reçus sans intervention du contrôleur NMOS externe après un redémarrage."""
    import json as _json
    raw = _setting("nmos_subscriptions", "")
    try:
        subs = _json.loads(raw) if raw else {}
    except Exception:
        subs = {}
    vmids = set()
    vid_fmts = []   # (vmid, recv_idx, sdp) des flux VIDÉO actifs → repropager le format PAR-FLUX
    act_calls = []  # (vmid, recv_idx, sdp) tous essences → réactivation IS-05 best-effort au boot
    with _lock:
        for rid, info in subs.items():
            if rid not in _recv_state:
                continue   # le modèle ne contient pas (encore) ce receiver
            active = info.get("active") or {}
            if not bool(active.get("master_enable")):
                continue
            _recv_state[rid]["active"] = active
            _recv_state[rid]["staged"] = _json.loads(_json.dumps(active))
            if rid in _receivers:
                _receivers[rid]["subscription"] = {
                    "sender_id": active.get("sender_id"), "active": True}
                _receivers[rid]["version"] = _tai_version()
            vmids.add(_recv_state[rid]["vmid"])
            _sdp = (active.get("transport_file") or {}).get("data")
            if _sdp:
                act_calls.append((_recv_state[rid]["vmid"], _recv_state[rid].get("recv_idx"), _sdp))
                if (info.get("essence") or _recv_state[rid].get("essence")) == "video":
                    vid_fmts.append((_recv_state[rid]["vmid"], _recv_state[rid].get("recv_idx"), _sdp))
    for vmid in vmids:
        threading.Thread(target=repush_subscriptions, args=(vmid,), daemon=True).start()
    # Repropage le format PAR-FLUX (rx_fmt[idx]) depuis les SDP restaurés → la page Câbles et les
    # consommateurs affichent le scan i/p RÉEL de chaque entrée (et pas le scan global). Idempotent.
    for _vmid, _ridx, _sdp in vid_fmts:
        try:
            _propagate_sdp_format(_vmid, _sdp, _ridx, slot_only=True)
        except Exception as e:
            log.warning(f"nmos: repropagation format vmid={_vmid} idx={_ridx}: {e}")
    # Réactive les senders NMOS distants (redémarrage device = retombe à master_enable:false).
    for _vmid, _ridx, _sdp in act_calls:
        threading.Thread(target=_activate_remote_sender_if_needed, args=(_vmid, _ridx, _sdp), daemon=True).start()
    if vmids:
        log.info(f"nmos: abonnements RX restaurés depuis la DB pour {len(vmids)} container(s)")

# ═════════════════════════════════════════════════════════════════════
# Client de registration (RDS)
# ═════════════════════════════════════════════════════════════════════

def _register_one(reg_base, type_str, data):
    url = f"{reg_base}/x-nmos/registration/{IS04_VERSION}/resource"
    r = requests.post(url, json={"type": type_str, "data": data}, timeout=5)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"register {type_str} {data.get('id')} → HTTP {r.status_code}: {r.text[:200]}")

# Ce qu'on a POSTÉ au registre lors du dernier cycle, par type → {ids}. Sert à retirer ce qui a
# disparu de notre modèle : un POST est un upsert, il n'efface rien. Sans cette comparaison, un
# conteneur détruit laisse ses Sender/Receiver au registre jusqu'à l'expiration du Node entier —
# et un contrôleur continue de proposer un routage vers un flux qui n'existe plus.
_registre_pousse = {}


def _register_all(reg_base):
    """Enregistre node + devices + sources + flows + senders + receivers en cascade, puis RETIRE
    du registre ce qui a disparu de notre modèle depuis le cycle précédent.

    L'ordre compte côté RDS : source avant flow, flow avant sender. À la suppression, l'ordre est
    INVERSE (enfants d'abord) — un registre conforme retire la descendance d'office, mais tous ne
    le font pas, et supprimer un parent en premier laisserait des orphelins chez ceux-là."""
    version = _tai_version()
    node = _build_node_resource(version)
    _register_one(reg_base, "node", node)
    with _lock:
        devs    = list(_devices.values())
        srcs    = list(_sources.values())
        flows   = list(_flows.values())
        sends   = list(_senders.values())
        recvs   = list(_receivers.values())
    # ★ BATTRE PENDANT L'ENREGISTREMENT — sinon on se fait expirer avant d'avoir commencé.
    # Mesuré le 2026-08-31 : pousser nos ~1030 ressources prend ~9 s en séquentiel. Le client ne
    # battait qu'APRÈS la boucle complète, donc premier battement à T+14 s (9 s de POST + 5 s de
    # sommeil) — alors que le ramasse-miettes d'IS-04 expire un Node à T+12 s. Résultat observé
    # dans le journal : « enregistré » / « Node expiré » / « heartbeat 404 » en boucle sans fin,
    # et un registre dont le contenu oscille. Le défaut est CÔTÉ CLIENT et vaut pour n'importe
    # quel registre tiers : à notre échelle, on ferait clignoter le registre d'un client.
    _depuis = [time.monotonic()]

    def _pousser(type_str, items):
        for x in items:
            _register_one(reg_base, type_str, x)
            if time.monotonic() - _depuis[0] >= HEARTBEAT_S:
                try:
                    _heartbeat(reg_base)
                except Exception:
                    pass          # un battement raté pendant l'enregistrement n'est pas fatal :
                                  # la boucle principale le retentera et ré-enregistrera au besoin
                _depuis[0] = time.monotonic()

    _pousser("device",   devs)
    _pousser("source",   srcs)
    _pousser("flow",     flows)
    _pousser("sender",   sends)
    _pousser("receiver", recvs)

    courant = {"device": {d["id"] for d in devs}, "source": {s["id"] for s in srcs},
               "flow": {f["id"] for f in flows}, "sender": {s["id"] for s in sends},
               "receiver": {r["id"] for r in recvs}}
    ancien = _registre_pousse.get(reg_base) or {}
    for type_str in ("receiver", "sender", "flow", "source", "device"):
        for rid in (ancien.get(type_str, set()) - courant[type_str]):
            try:
                requests.delete("%s/x-nmos/registration/%s/resource/%ss/%s"
                                % (reg_base, IS04_VERSION, type_str, rid), timeout=5)
            except Exception as e:
                # Best-effort : un retrait raté sera retenté au cycle suivant (l'id reste dans
                # `ancien` puisqu'on ne remplace la carte qu'à la fin).
                log.warning("nmos: retrait de %s %s au registre échoué : %s", type_str, rid, e)
                courant[type_str] = courant[type_str] | {rid}
    _registre_pousse[reg_base] = courant

def _heartbeat(reg_base):
    url = f"{reg_base}/x-nmos/registration/{IS04_VERSION}/health/nodes/{_state['node_id']}"
    r = requests.post(url, timeout=5)
    if r.status_code not in (200,):
        raise RuntimeError(f"heartbeat HTTP {r.status_code}")
    _state["last_heartbeat_unix"] = int(time.time())

def _unregister_node(reg_base):
    try:
        url = f"{reg_base}/x-nmos/registration/{IS04_VERSION}/resource/nodes/{_state['node_id']}"
        requests.delete(url, timeout=3)
    except Exception:
        pass

def _register_loop():
    """Boucle de registration + heartbeat. Si erreur, retry après backoff."""
    backoff = 5
    while _running:
        reg = _state.get("registry_url")
        if not reg:
            time.sleep(2); continue
        try:
            rebuild_model()
            _register_all(reg)
            _state["registered"] = True
            _state["last_register_error"] = None
            log.info(f"nmos: enregistré auprès de {reg}")
            backoff = 5
            # Heartbeat loop
            _dernier_controle = time.monotonic()
            while _running and _state["registry_url"] == reg:
                time.sleep(HEARTBEAT_S)
                try:
                    _heartbeat(reg)
                except Exception as e:
                    log.warning(f"nmos: heartbeat échoué : {e}")
                    _state["registered"] = False
                    _state["last_register_error"] = str(e)
                    break  # ré-enregistrer
                # ── FILET : reconstruire périodiquement et re-pousser SI le modèle a bougé ──
                # `rebuild_model()` n'est appelé que sur notification (`notify_state_change`), et
                # cette couverture s'est révélée INCOMPLÈTE — aucun chemin de destruction compute
                # ne notifiait, découvert au banc le 2026-08-31. Un site d'appel oublié rendait
                # alors le modèle périmé POUR TOUJOURS. Auparavant un bug masquait le problème :
                # l'enregistrement expirait toutes les ~14 s et reconstruisait en boucle. En le
                # corrigeant, on perdait ce rafraîchissement accidentel — d'où ce filet explicite.
                # Coût mesuré : un rebuild = 0,71 s, soit ~1 % d'un cœur à cette période.
                if time.monotonic() - _dernier_controle >= REBUILD_FILET_S:
                    _dernier_controle = time.monotonic()
                    try:
                        rebuild_model()
                        with _lock:
                            actuel = {"device": {d["id"] for d in _devices.values()},
                                      "source": {x["id"] for x in _sources.values()},
                                      "flow": {x["id"] for x in _flows.values()},
                                      "sender": {x["id"] for x in _senders.values()},
                                      "receiver": {x["id"] for x in _receivers.values()}}
                        if actuel != (_registre_pousse.get(reg) or {}):
                            log.info("nmos: le modèle a changé sans notification — re-poussé")
                            _register_all(reg)
                    except Exception as e:
                        log.warning("nmos: filet de reconstruction échoué : %s", e)
        except Exception as e:
            log.warning(f"nmos: registration échouée : {e}")
            _state["registered"] = False
            _state["last_register_error"] = str(e)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

# ═════════════════════════════════════════════════════════════════════
# API publique (start/stop, status, refresh model)
# ═════════════════════════════════════════════════════════════════════

# ═════════════════════════════════════════════════════════════════════
# mDNS : annonce _nmos-node._tcp.local. (RFC 6762 / IS-04 bootstrapping)
# ═════════════════════════════════════════════════════════════════════

def _mdns_pri():
    """Priorité annoncée pour NOTRE registre. IS-04 : « Values 0 to 99 correspond to an active
    NMOS Registration API (zero being the highest priority). Values 100+ are reserved for
    development work to avoid colliding with a live system. »

    ★ DÉFAUT 100, c'est-à-dire « développement », et c'est VOULU. Un registre qui s'annonce en
    priorité de production détourne vers lui les Nodes d'un registre déjà en place sur le même
    LAN — un incident d'exploitation majeur causé par un simple réglage laissé à zéro. C'est à
    l'exploitant d'abaisser cette valeur quand il DÉCIDE que notre registre fait référence."""
    try:
        return max(0, min(255, int(_setting("nmos_registre_pri", 100))))
    except (TypeError, ValueError):
        return 100


def _mdns_services_a_publier(addr_bytes):
    """[(type, ServiceInfo)] à annoncer. Toujours `_nmos-node` ; plus `_nmos-register` et
    `_nmos-query` quand le registre embarqué est ouvert — sans ces deux-là, un tiers ne peut pas
    TROUVER notre registre et doit se voir donner l'URL à la main."""
    from zeroconf import ServiceInfo
    label = _setting("nmos_node_label", "MXL Orchestrator")
    suffixe = (_state.get("node_id") or "")[:8]
    txt_commun = {"api_proto": "http", "api_ver": IS04_VERSION, "api_auth": "false"}
    out = []

    def _ajouter(type_court, txt):
        type_ = f"_{type_court}._tcp.local."
        out.append((type_court, ServiceInfo(
            type_=type_,
            # Le nom DOIT être unique sur le LAN ; on inclut le node_id pour éviter les collisions.
            name=f"{label} {suffixe}.{type_}",
            addresses=[addr_bytes], port=5000, weight=0, priority=0,
            properties=txt, server=f"{socket.gethostname()}.local.")))

    _ajouter("nmos-node", dict(txt_commun))
    try:
        from . import registre as _reg
        if _reg.actif():
            # `pri` n'est requis que sur Registration et Query — pas sur Node.
            avec_pri = dict(txt_commun, pri=str(_mdns_pri()))
            _ajouter("nmos-register", avec_pri)
            _ajouter("nmos-query", dict(avec_pri))
    except Exception as e:
        log.warning("nmos: registre non interrogeable pour l'annonce mDNS (%s)", e)
    try:
        from . import is09 as _i9
        if _i9.actif():
            # Même prudence que pour le registre : `pri` par défaut à 100 (« développement »).
            # Une System API qui s'annonce en priorité de production détournerait les équipements
            # d'une installation déjà en place vers NOTRE domaine PTP.
            _ajouter("nmos-system", dict(txt_commun, pri=str(_i9._pri())))
    except Exception as e:
        log.warning("nmos: IS-09 non interrogeable pour l'annonce mDNS (%s)", e)
    return out


def _mdns_start():
    """Publie les services NMOS en mDNS (RFC 6762 / bootstrapping IS-04)."""
    global _mdns_zc, _mdns_services
    try:
        from zeroconf import IPVersion, Zeroconf
    except Exception as e:
        log.warning(f"nmos: zeroconf indisponible ({e})")
        return False, str(e)
    _mdns_stop()
    host = _get_host_address()
    try:
        addr_bytes = socket.inet_aton(host)
    except Exception:
        log.warning(f"nmos: mDNS — IP {host!r} invalide")
        return False, "IP invalide"
    services = _mdns_services_a_publier(addr_bytes)
    try:
        zc = Zeroconf(ip_version=IPVersion.V4Only)
        for _court, info in services:
            zc.register_service(info)
    except Exception as e:
        log.warning(f"nmos: échec register mDNS: {e}")
        try: zc.close()
        except Exception: pass
        return False, str(e)
    _mdns_zc = zc
    _mdns_services = [i for _c, i in services]
    _state["mdns_active"] = True
    _state["mdns_types"] = [c for c, _i in services]
    log.info("nmos: mDNS annoncé (%s) @ %s:5000", ", ".join(c for c, _i in services), host)
    return True, "ok"


def _mdns_stop():
    global _mdns_zc, _mdns_services
    if _mdns_zc:
        for info in _mdns_services:
            try: _mdns_zc.unregister_service(info)
            except Exception: pass
        try: _mdns_zc.close()
        except Exception: pass
    _mdns_zc = None
    _mdns_services = []
    _state["mdns_active"] = False
    _state["mdns_types"] = []


def start(registry_url):
    """Démarre/redémarre le client de registration vers `registry_url` (peut être '' ou None)."""
    global _register_thread, _running
    stop()
    _state["node_id"] = _get_node_id()
    _state["registry_url"] = (registry_url or "").rstrip("/") or None
    # Normalisation unique des index de grouping, AVANT le premier rebuild : après quoi les
    # grouphints sont immuables (BCP-002-01). Best-effort — un registre illisible ne doit pas
    # empêcher le provider de démarrer.
    try:
        normaliser_grouping_registre()
    except Exception as e:
        log.warning("nmos: normalisation des grouphints échouée : %s", e)
    # IS-12 AVANT le premier rebuild : c'est lui qui pose le control `ncp` dans les Devices, et
    # une ressource enregistrée sans ce control n'annoncerait le protocole à personne jusqu'au
    # prochain changement d'état.
    from app.database import db_get_setting as _dgs
    if _dgs("nmos_is12_enabled", False):
        try:
            from . import is12
            is12.start()
        except Exception as e:
            log.error("nmos: démarrage IS-12 échoué : %s", e)
    if _dgs("nmos_is14_enabled", False):
        try:
            from . import is14
            is14.start()
        except Exception as e:
            log.error("nmos: démarrage IS-14 échoué : %s", e)
    rebuild_model()
    # Restaure les abonnements RX persistés (survie au redémarrage de l'orchestrateur) + re-push.
    try:
        _restore_persisted_subscriptions()
    except Exception as e:
        log.warning(f"nmos: restauration abonnements échouée : {e}")
    if _state["registry_url"]:
        _running = True
        _register_thread = threading.Thread(target=_register_loop, daemon=True)
        _register_thread.start()
    else:
        # Mode purement mDNS-less / API-only : pas de thread de registration
        # mais les endpoints IS-04/IS-05 restent servis par Flask
        log.info("nmos: actif sans registry (endpoints IS-04/IS-05 disponibles, pas de RDS)")
    # mDNS optionnel (indépendant du RDS — peut coexister)
    from app.database import db_get_setting
    if db_get_setting("nmos_mdns_enabled", False):
        _mdns_start()

def stop():
    global _running, _register_thread
    from . import is12, is14
    for nom, mod in (("IS-12", is12), ("IS-14", is14)):
        try:
            mod.stop()
        except Exception as e:
            log.warning("nmos: arrêt %s : %s", nom, e)
    _mdns_stop()
    if not _running:
        return
    _running = False
    reg = _state.get("registry_url")
    if reg:
        _unregister_node(reg)
    if _register_thread:
        _register_thread.join(timeout=2)
    _register_thread = None
    _state["registered"] = False

def status_dict():
    with _lock:
        return {
            # ★ EN MARCHE ≠ ENREGISTRÉ. Le service tourne dès que le modèle est
            # bâti (`node_id` posé) : il sert alors IS-04, IS-05 et IS-12. Être
            # ENREGISTRÉ auprès d'un registre NMOS est autre chose, et c'est
            # facultatif — en mode pair-à-pair il n'y a aucun registre à joindre.
            # Faute de publier `running`, l'agrégateur retombait sur `registered`
            # et affichait « arrêté » un service parfaitement opérationnel.
            "running": _state.get("node_id") is not None,
            "enabled": bool(_state.get("registry_url")) or _state.get("node_id") is not None,
            "registry_url": _state.get("registry_url"),
            "registered": _state.get("registered"),
            "node_id": _state.get("node_id"),
            "host_address": _get_host_address(),
            "last_error": _state.get("last_register_error"),
            "last_heartbeat_unix": _state.get("last_heartbeat_unix"),
            "device_count": len(_devices),
            "receiver_count": len(_receivers),
            "sender_count": len(_senders),
            "mdns_active": bool(_state.get("mdns_active")),
            "mdns_types": list(_state.get("mdns_types") or []),
            "is12": _is12_status(),
        }


def _is12_status():
    try:
        from . import is12
        return is12.status_dict()
    except Exception as e:
        return {"actif": False, "erreur": str(e)}

def notify_state_change():
    """Appelé quand un container ou son nmos_receivers_count change. Rebuild + re-register si actif."""
    if _state.get("node_id") is None:
        return
    rebuild_model()
    reg = _state.get("registry_url")
    if reg and _state.get("registered"):
        try:
            _register_all(reg)
        except Exception as e:
            log.warning(f"nmos: re-register après changement état : {e}")


def purge_orphan_resources(dry_run=True):
    """Purge les ressources NMOS auto-seedées devenues orphelines (conteneur supprimé/recréé).
    GARDE une ressource si : `label_locked` (pool fixe / manuelle) OU explicitement bindée (id dans
    un params.nmos_bind live) OU servie (bind_instance_uuid d'un conteneur live) OU abonnée par un
    contrôleur (master_enable / transport_file actif). SUPPRIME le reste (auto-seed sur instance_uuid
    mort). `dry_run` → renvoie la liste candidate sans rien supprimer.
    Sert à régler les receivers fantômes (le contrôleur ne voit plus les ressources d'un conteneur
    disparu). Appelée à la suppression de conteneur (app/containers.py) et via l'endpoint UI."""
    import json as _json
    from app.database import (db_nmos_resources, db_get_containers, db_nmos_resource_delete)
    conts = db_get_containers()
    live_iu = {c.get("instance_uuid") for c in conts if c.get("instance_uuid")}
    explicit = set()
    for c in conts:
        try:
            _p = (_json.loads(c.get("deploy_config") or "{}").get("params") or {})
        except Exception:
            _p = {}
        explicit.update((_p.get("nmos_bind") or {}).values())
    candidates = []
    for row in db_nmos_resources():
        rid = row["id"]
        if row.get("label_locked"):
            continue                                   # pool fixe / manuelle → toujours gardée
        if rid in explicit:
            continue                                   # explicitement câblée par un conteneur live
        if row.get("bind_instance_uuid") in live_iu:
            continue                                   # servie par un conteneur live
        # NB : un orphelin ABONNÉ (bind_instance_uuid d'un conteneur DISPARU) est un fantôme —
        # le contrôleur croit le router mais aucun conteneur ne reçoit. On le purge (sinon les
        # receivers fantômes d'un conteneur supprimé restent éternellement exposés en IS-04).
        candidates.append({"id": rid, "kind": row.get("kind"), "essence": row.get("essence"),
                           "label": row.get("label")})
    if dry_run:
        return {"dry_run": True, "candidates": candidates, "count": len(candidates)}
    for cand in candidates:
        db_nmos_resource_delete(cand["id"])
    if candidates:
        notify_state_change()
    return {"dry_run": False, "purged": candidates, "count": len(candidates)}


def reset_container_resources(vmid):
    """Remet à zéro le NMOS d'UN conteneur : vide ses `nmos_bind` ET supprime ses ressources
    AUTO-SEEDÉES (bind sur son instance_uuid, non label_locked). Sert à « dé-assigner » un conteneur
    dont les ressources viennent de l'auto-création (mode auto historique) et non d'un câblage de pool.
    En mode STATIQUE → ardoise propre (le conteneur n'expose plus rien tant qu'on ne câble pas un pool).
    En mode AUTO les uuids se régénèrent au rebuild (déterministes) → n'efface durablement que les binds.
    Le pool fixe (label_locked) n'est jamais supprimé (juste délié). Retourne {unbound, deleted}."""
    import json as _json
    from app.database import (db_get_container, db_update_deploy_config, db_nmos_resources,
                              db_nmos_resource_delete)
    c = db_get_container(int(vmid))
    if not c:
        return {"ok": False, "error": "conteneur introuvable"}
    iu = c.get("instance_uuid")
    try:
        dc = _json.loads(c.get("deploy_config") or "{}")
    except Exception:
        dc = {}
    params = dc.get("params") or {}
    nb = params.get("nmos_bind") or {}
    n_unbound = len(nb)
    if nb:
        params["nmos_bind"] = {}
        db_update_deploy_config(int(vmid), dc.get("type"), params)
    deleted = 0
    if iu:
        for row in db_nmos_resources():
            if row.get("bind_instance_uuid") == iu and not row.get("label_locked"):
                db_nmos_resource_delete(row["id"]); deleted += 1
    notify_state_change()
    return {"ok": True, "unbound": n_unbound, "deleted": deleted}


def nmos_config_snapshot():
    """Capture la config NMOS (pool + bindings + réglages), UUID inclus. Sert export ET snapshot nommé."""
    import json as _json, datetime as _dt
    from app.database import db_nmos_resources, db_get_containers, db_get_setting
    resources = [{k: r.get(k) for k in ("id", "kind", "essence", "label", "group_name",
                                        "role", "transport", "label_locked")}
                 for r in db_nmos_resources()]
    bindings = []
    for c in db_get_containers():
        try:
            dc = _json.loads(c.get("deploy_config") or "{}")
        except Exception:
            dc = {}
        nb = (dc.get("params") or {}).get("nmos_bind") or {}
        if nb:
            bindings.append({"hostname": c.get("hostname"), "instance_uuid": c.get("instance_uuid"),
                             "type": dc.get("type"), "nmos_bind": nb})
    return {"version": 1, "exported_at": _dt.datetime.now().isoformat(timespec="seconds"),
            "nmos_mode": db_get_setting("nmos_mode", "auto"),
            "cluster_label": db_get_setting("nmos_cluster_label", ""),
            "resources": resources, "bindings": bindings}


def nmos_config_apply(cfg, mode="merge", restore_bindings=True, apply_mode=False):
    """Applique une config NMOS en CONSERVANT les UUID (import / rappel de snapshot). Retourne
    (ok, payload). `replace` supprime d'abord le pool actuel absent de la config."""
    import json as _json
    from app.database import (db_nmos_resource_import, db_nmos_resources, db_nmos_resource_delete,
                              db_get_containers, db_update_deploy_config, db_set_setting)
    res_in = (cfg or {}).get("resources") or []
    if not isinstance(res_in, list) or not res_in:
        return False, {"error": "config invalide (aucune ressource)"}
    import_ids = {r.get("id") for r in res_in if r.get("id")}
    if mode == "replace":
        for r in db_nmos_resources():
            if r.get("label_locked") and r["id"] not in import_ids:
                db_nmos_resource_delete(r["id"])
    imported = 0
    for r in res_in:
        if not r.get("id"):
            continue
        db_nmos_resource_import(r["id"], r.get("kind"), r.get("essence"), r.get("label") or "",
                                r.get("group_name") or "", r.get("role") or "",
                                r.get("transport") or {}, r.get("label_locked"))
        imported += 1
    bound = 0
    if restore_bindings:
        conts = db_get_containers()
        by_iu = {c.get("instance_uuid"): c for c in conts if c.get("instance_uuid")}
        by_hn = {c.get("hostname"): c for c in conts}
        for b in (cfg.get("bindings") or []):
            c = by_iu.get(b.get("instance_uuid")) or by_hn.get(b.get("hostname"))
            if not c:
                continue
            try:
                dc = _json.loads(c.get("deploy_config") or "{}")
            except Exception:
                dc = {}
            params = dc.get("params") or {}
            nb = {k: v for k, v in (b.get("nmos_bind") or {}).items() if v in import_ids}
            if nb:
                params["nmos_bind"] = nb
                db_update_deploy_config(c["vmid"], dc.get("type"), params)
                bound += 1
    if apply_mode and cfg.get("nmos_mode") in ("auto", "static"):
        db_set_setting("nmos_mode", cfg["nmos_mode"])
    notify_state_change()
    return True, {"imported": imported, "containers_bound": bound}


# ─── Core plugin manifest ─────────────────────────────────────────────

__manifest__ = {
    "id":            "nmos",
    "label":         "NMOS",
    "nav_tab":       "protocoles",
    "tab_group":     "protocoles",
    "tab_order":     0,
    "tab_template":  "settings_tabs/nmos_sub.html",
    "settings_keys": {
        "nmos_enabled":          {"type": "bool", "default": False},
        "nmos_registry_url":     {"type": "str",  "default": ""},
        "nmos_node_label":       {"type": "str",  "default": "Bobi.Studio"},
        "nmos_node_description": {"type": "str",  "default": ""},
        "nmos_cluster_label":    {"type": "str",  "default": "Bobi.Studio 2110 I/O"},
        # Mode de gestion des ressources NMOS : "auto" (chaque slot de conteneur auto-crée sa
        # ressource) | "static" (pool fixe : un slot n'émet que s'il est explicitement câblé).
        "nmos_mode":             {"type": "str",  "default": "auto"},
        "nmos_mdns_enabled":     {"type": "bool", "default": False},
        "nmos_auto_activate_senders": {"type": "bool", "default": False},
        # a=source-filter (SSM) dans les SDP TX du moteur 2110. True = bonne pratique sur fabric
        # SSM-capable (PIM-SSM/NBM) ; False pour un switch L2 en IGMP snooping pur (le join (S,G)
        # y est enregistré mais jamais forwardé → 0 Mbps silencieux). Plombé par docker_driver
        # (env SDP_SOURCE_FILTER), appliqué au redéploiement du moteur.
        "nmos_sdp_source_filter": {"type": "bool", "default": True},
        "nmos_node_uuid":        {"type": "str",  "default": ""},
        "nmos_host_address":     {"type": "str",  "default": ""},
        "nmos_2110_enabled":     {"type": "bool", "default": False},
        "nmos_2110_pf":          {"type": "str",  "default": ""},
        "nmos_2110_vf_count":    {"type": "int",  "default": 8},
    },
}


def register_routes(bp):
    from flask import request, jsonify
    from app.auth import require_login, require_perm
    from app.database import db_get_setting, db_set_setting

    @bp.route("/api/nmos/status", methods=["GET"])
    @require_login
    def nmos_status():
        st = status_dict()
        st["enabled_setting"]        = bool(db_get_setting("nmos_enabled", False))
        st["registry_url_setting"]   = db_get_setting("nmos_registry_url", "") or ""
        st["node_label_setting"]     = db_get_setting("nmos_node_label", "Bobi.Studio")
        st["label_prefix_setting"]   = db_get_setting("nmos_label_prefix", "")
        st["node_description_setting"] = db_get_setting("nmos_node_description", "")
        st["mdns_enabled_setting"]   = bool(db_get_setting("nmos_mdns_enabled", False))
        st["auto_activate_senders_setting"] = bool(db_get_setting("nmos_auto_activate_senders", False))
        st["sdp_source_filter_setting"] = bool(db_get_setting("nmos_sdp_source_filter", True))
        st["mode_setting"]           = db_get_setting("nmos_mode", "auto") or "auto"
        st["is12_enabled_setting"]   = bool(db_get_setting("nmos_is12_enabled", False))
        st["is12_port_setting"]      = int(db_get_setting("nmos_is12_port", 5010) or 5010)
        st["asset"]                  = asset_info()      # BCP-002-02
        # ★ CONTRÔLE DE COHÉRENCE BCP-002-01, EXPOSÉ À L'EXPLOITATION (2026-08-22).
        # Le MUST est l'unicité du couple `groupe:rôle` dans un même scope de Device. Un banc le
        # vérifiait déjà (`bench_bcp002.py`), mais un banc qu'on lance à la main ne surveille rien :
        # l'exploitation ne voyait pas la collision, et depuis que les grouphints sont figés elle
        # serait DÉFINITIVE. On la publie donc ici, avec les couples fautifs nommés — un compteur
        # seul dirait « 2 » sans dire lesquels, ce qui n'aide personne à réparer.
        #
        # `mesure` sépare l'ABSENCE DE COLLISION de l'ÉCHEC DE LA MESURE : sans ça, un `n: null`
        # se lit « rien à signaler » côté UI, et le contrôle ment exactement quand il tombe.
        try:
            from app.database import db_nmos_grouping_collisions
            _couples, _total = db_nmos_grouping_collisions()
            st["grouping_collisions"] = {"mesure": True, "n": _total, "couples": _couples}
        except Exception as _e:
            log.warning("BCP-002-01 : contrôle de collision de grouping indisponible : %r", _e)
            st["grouping_collisions"] = {"mesure": False, "n": None, "couples": [],
                                         "erreur": repr(_e)}
        return jsonify(st)

    @bp.route("/api/nmos/asset", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_asset_apply():
        """Information distinctive BCP-002-02. `instance_id` n'est PAS modifiable : c'est l'UUID de
        Node, et la BCP exige que le triplet reste unique — le laisser saisir, c'est offrir de le
        rendre ambigu."""
        data = request.json or {}
        for cle, defaut in (("manufacturer", ASSET_MANUFACTURER_DEFAUT),
                            ("product", ASSET_PRODUCT_DEFAUT),
                            ("function", ASSET_FUNCTION_DEFAUT)):
            if cle in data:
                v = str(data.get(cle) or "").strip() or defaut
                db_set_setting("nmos_asset_" + cle, v)
        notify_state_change()      # Node et Device changent de tags → republier au registry
        return jsonify(asset_info())

    # ─── IS-12 / BCP-008 : supervision des Receivers et Senders ──────────────────
    @bp.route("/api/nmos/is12/status", methods=["GET"])
    @require_login
    def nmos_is12_status():
        from . import is12
        st = is12.status_dict()
        st["enabled_setting"] = bool(db_get_setting("nmos_is12_enabled", False))
        st["port_setting"]    = int(db_get_setting("nmos_is12_port", is12.PORT_DEFAUT)
                                    or is12.PORT_DEFAUT)
        # IS-12 supervise les ressources IS-04 : sans provider NMOS, il n'y a rien à superviser.
        # Le dire, plutôt que d'afficher une table vide sans explication.
        st["nmos_enabled_setting"] = bool(db_get_setting("nmos_enabled", False))
        from . import is14
        st["is14"] = is14.status_dict()
        return jsonify(st)

    @bp.route("/api/nmos/is12/monitors", methods=["GET"])
    @require_login
    def nmos_is12_monitors():
        """État courant de chaque monitor — la MÊME vérité que celle servie aux contrôleurs
        tiers. Si cette page et un contrôleur externe divergeaient, l'un des deux mentirait."""
        from . import is12
        return jsonify({"monitors": is12.etat_monitors()})

    @bp.route("/api/nmos/is14/apply", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_is14_apply():
        from . import is14
        enabled = bool((request.json or {}).get("enabled"))
        db_set_setting("nmos_is14_enabled", enabled)
        if enabled:
            is14.start()
        else:
            is14.stop()
        # Le tableau `controls` du Device gagne ou perd l'entrée `configuration` → republier.
        notify_state_change()
        return jsonify(is14.status_dict())

    @bp.route("/api/nmos/is12/apply", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_is12_apply():
        from . import is12
        data = request.json or {}
        enabled = bool(data.get("enabled"))
        port = data.get("port")
        if port is not None:
            try:
                port = int(port)
            except (TypeError, ValueError):
                return jsonify({"error": "port invalide"}), 400
            if not (1 <= port <= 65535):
                return jsonify({"error": "port hors bornes"}), 400
            db_set_setting("nmos_is12_port", port)
        db_set_setting("nmos_is12_enabled", enabled)
        is12.stop()
        if enabled:
            if not is12.start():
                return jsonify({"error": "le serveur IS-12 n'a pas pu écouter sur ce port",
                                **is12.status_dict()}), 500
        # Le tableau `controls` des Devices change avec l'état d'IS-12 : il faut le republier au
        # registre, sinon les contrôleurs continuent de voir l'ancienne annonce.
        notify_state_change()
        return jsonify(is12.status_dict())

    @bp.route("/api/nmos/apply", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_apply():
        data     = request.json or {}
        enabled  = bool(data.get("enabled"))
        registry = (data.get("registry_url") or "").strip()
        label    = (data.get("node_label") or "Bobi.Studio").strip()
        desc     = (data.get("node_description") or "").strip()
        mdns     = bool(data.get("mdns_enabled"))
        auto_act = bool(data.get("auto_activate_senders"))
        sdp_sf   = bool(data.get("sdp_source_filter", True))
        db_set_setting("nmos_enabled",          enabled)
        db_set_setting("nmos_registry_url",     registry)
        db_set_setting("nmos_node_label",       label)
        db_set_setting("nmos_node_description", desc)
        # Préfixe des libellés Rx/Tx (affichage seul ; override par nœud via /api/settings/node).
        if "label_prefix" in data:
            db_set_setting("nmos_label_prefix", (data.get("label_prefix") or "").strip())
        db_set_setting("nmos_mdns_enabled",     mdns)
        db_set_setting("nmos_auto_activate_senders", auto_act)
        db_set_setting("nmos_sdp_source_filter", sdp_sf)
        # Mode de gestion des ressources (auto|static) — n'est appliqué qu'au prochain rebuild.
        if "mode" in data:
            mode = "static" if str(data.get("mode")).strip() == "static" else "auto"
            db_set_setting("nmos_mode", mode)
            notify_state_change()
        if enabled:
            start(registry)
        else:
            stop()
        return jsonify(status_dict())

    # ─── C2b : registre de ressources cluster (curation) ─────────────────────────
    @bp.route("/api/nmos/registry", methods=["GET"])
    @require_login
    def nmos_registry_list():
        """Toutes les ressources du registre cluster, enrichies : servie/orpheline, conteneur servant,
        abonnée par un contrôleur. `manual` = créée à la main (sans binding)."""
        import json as _json
        from app.database import db_nmos_resources, db_get_containers
        conts = db_get_containers()
        cont_by_iu = {c["instance_uuid"]: c for c in conts if c.get("instance_uuid")}
        # C2b+ : ids explicitement bindés (présents dans un params.nmos_bind) → distinguer du binding
        # auto (instance_uuid+slot) pour n'offrir « délier » que sur les rebinds explicites.
        explicit = set()
        bound_by_rid = {}            # rid → (vmid, hostname, slot_key) depuis nmos_bind (vérité DB)
        for c in conts:
            try:
                _p = (_json.loads(c.get("deploy_config") or "{}").get("params") or {})
            except Exception:
                _p = {}
            for _sk, _rid in (_p.get("nmos_bind") or {}).items():
                explicit.add(_rid)
                bound_by_rid.setdefault(_rid, (c["vmid"], c.get("hostname"), _sk))
        # B2-3 : ids en collision multicast (groupe partagé par >1 ressource) → badge éditeur.
        from app.allocations import multicast_conflicts
        _conflict_ids = set()
        for _ids in multicast_conflicts().values():
            _conflict_ids.update(_ids)
        out = []
        with _lock:
            for row in db_nmos_resources():
                rid = row["id"]
                pool  = _senders if row["kind"] == "sender" else _receivers
                sub = ((pool.get(rid) or {}).get("subscription") or {})
                # « Servi par » = vérité DB (indépendant du rebuild live) : priorité au binding explicite
                # (nmos_bind), sinon au bind_instance_uuid (auto-seed) résolvant un conteneur live.
                sv = bound_by_rid.get(rid)
                if sv:
                    serving = {"vmid": sv[0], "hostname": sv[1], "slot": sv[2]}
                else:
                    cc = cont_by_iu.get(row.get("bind_instance_uuid"))
                    serving = ({"vmid": cc["vmid"], "hostname": cc.get("hostname"),
                                "slot": row.get("bind_slot")} if cc else None)
                served = serving is not None
                out.append({
                    "id": rid, "kind": row["kind"], "essence": row["essence"],
                    "label": row["label"], "group_name": row.get("group_name"),
                    "role": row.get("role"), "transport": row.get("transport") or {},
                    "bind_instance_uuid": row.get("bind_instance_uuid"),
                    "bind_slot": row.get("bind_slot"),
                    "label_locked": bool(row.get("label_locked")),
                    "manual": not row.get("bind_instance_uuid"),
                    "active": served, "serving": serving,
                    "explicit": rid in explicit,
                    "mcast_conflict": rid in _conflict_ids,
                    "subscribed": bool(sub.get("active")), "present": rid in pool,
                })
        return jsonify({"resources": out,
                        "cluster_label": db_get_setting("nmos_cluster_label", "Bobi.Studio 2110 I/O")})

    @bp.route("/api/nmos/registry", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_registry_create():
        """Création manuelle d'une ressource (réservation cluster, sans conteneur servant)."""
        from app.database import db_nmos_resource_create
        d = request.json or {}
        kind    = (d.get("kind") or "").strip()
        essence = (d.get("essence") or "").strip()
        label   = (d.get("label") or "").strip()
        if kind not in ("sender", "receiver"):
            return jsonify({"ok": False, "error": "kind invalide (sender|receiver)"}), 400
        if essence not in ("video", "audio", "data"):
            return jsonify({"ok": False, "error": "essence invalide (video|audio|data)"}), 400
        if not label:
            return jsonify({"ok": False, "error": "label requis"}), 400
        tr = {}
        for k in ("multicast_ip", "port", "width", "height", "chroma", "bit_depth",
                  "colorspace", "transfer", "scan", "field_order"):
            if d.get(k) not in (None, ""):
                tr[k] = d[k]
        # B2-3 : pas de multicast fourni → allocation cluster-unique depuis le pool (sender seulement ;
        # un receiver n'émet pas). L'opérateur peut toujours imposer un multicast (il prime). L'id est
        # pré-généré pour réserver ATOMIQUEMENT (owner_ref='nmos:<id>') avant même la création de la
        # ressource — sans ça, deux créations concurrentes pourraient recevoir la même adresse.
        import uuid as _uuid
        rid = str(_uuid.uuid4())
        if kind == "sender" and not tr.get("multicast_ip"):
            from app.allocations import allocate_multicast
            mip, mport = allocate_multicast(tr.get("port"), owner_ref=f"nmos:{rid}")
            if mip:
                tr["multicast_ip"] = mip
                tr["port"] = mport
        rid = db_nmos_resource_create(kind, essence, label,
                                      (d.get("group_name") or label), (d.get("role") or essence), tr, id=rid)
        notify_state_change()
        return jsonify({"ok": True, "id": rid})

    @bp.route("/api/nmos/registry/bulk", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_registry_create_bulk():
        """Création EN MASSE d'un pool fixe : N ressources d'un même kind/essence, labellisées
        `{label_prefix} {n}`. Multicast cluster-unique alloué par ressource (senders). 1 rebuild.
        Body: {kind, essence, count, label_prefix, group_prefix?, base_index?}."""
        from app.database import db_nmos_resource_create
        d = request.json or {}
        kind    = (d.get("kind") or "").strip()
        essence = (d.get("essence") or "").strip()
        prefix  = (d.get("label_prefix") or "").strip()
        try:
            count = int(d.get("count") or 0)
        except (TypeError, ValueError):
            count = 0
        base_index = int(d.get("base_index") or 0)
        grp_prefix = (d.get("group_prefix") or prefix).strip()
        if kind not in ("sender", "receiver"):
            return jsonify({"ok": False, "error": "kind invalide (sender|receiver)"}), 400
        if essence not in ("video", "audio", "data"):
            return jsonify({"ok": False, "error": "essence invalide (video|audio|data)"}), 400
        if not prefix:
            return jsonify({"ok": False, "error": "label_prefix requis"}), 400
        if not (1 <= count <= 256):
            return jsonify({"ok": False, "error": "count hors plage (1..256)"}), 400
        from app.allocations import allocate_multicast
        import uuid as _uuid
        ids = []
        for i in range(count):
            n = base_index + i + 1
            tr = {}
            rid = str(_uuid.uuid4())
            if kind == "sender":                  # multicast cluster-unique par sender, réservé
                                                    # atomiquement avant la création (id pré-généré)
                mip, mport = allocate_multicast(None, owner_ref=f"nmos:{rid}")
                if mip:
                    tr["multicast_ip"] = mip
                    tr["port"] = mport
            rid = db_nmos_resource_create(kind, essence, f"{prefix} {n}",
                                          grp_prefix or prefix, essence, tr, id=rid)
            ids.append(rid)
        notify_state_change()
        return jsonify({"ok": True, "ids": ids, "count": len(ids)})

    @bp.route("/api/nmos/registry/bulk_channels", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_registry_create_channels():
        """Création EN MASSE PAR CANAL pour UN kind (Rx OU Tx). N canaux, chacun = (Vidéo) + N audio +
        (ANC). Libellés `{Rx|Tx}_{prefix}_{ID}_{Video|Audio|Anc}` avec ID zéro-paddé (ex. 23 canaux →
        `Tx_Toto_05_Video`) pour un tri alphabétique correct ; audio `…_Audio` (×1) ou `…_Audio1/2`
        (×2), ANC `…_Anc` ; group_name = base du canal → bundle NMOS naturel.
        Body: {kind, count, label_prefix, video:bool, audio_count:0|1|2, anc:bool, base_index?}."""
        from app.database import db_nmos_resource_create
        from app.allocations import allocate_multicast
        d = request.json or {}
        kind   = (d.get("kind") or "").strip()
        prefix = (d.get("label_prefix") or "").strip()
        inc_v  = bool(d.get("video"))
        ac     = max(0, min(2, int(d.get("audio_count") or 0)))
        inc_d  = bool(d.get("anc"))
        base_index = int(d.get("base_index") or 0)
        try:
            count = int(d.get("count") or 0)
        except (TypeError, ValueError):
            count = 0
        if kind not in ("sender", "receiver"):
            return jsonify({"ok": False, "error": "kind invalide (sender|receiver)"}), 400
        if not (1 <= count <= 256):
            return jsonify({"ok": False, "error": "count hors plage (1..256)"}), 400
        if not (inc_v or ac or inc_d):
            return jsonify({"ok": False, "error": "cocher au moins une essence"}), 400
        klabel = "Tx" if kind == "sender" else "Rx"
        # Largeur de l'ID zéro-paddé = nb de chiffres du plus grand ID (min 2) → tri alpha correct.
        width = max(2, len(str(base_index + count)))

        def _mk(essence, label, group):
            import uuid as _uuid
            tr = {}
            rid = str(_uuid.uuid4())
            if kind == "sender":               # multicast cluster-unique par sender, réservé
                                                # atomiquement avant la création (id pré-généré)
                mip, mport = allocate_multicast(None, owner_ref=f"nmos:{rid}")
                if mip:
                    tr["multicast_ip"] = mip; tr["port"] = mport
            return db_nmos_resource_create(kind, essence, label, group, essence, tr, id=rid)

        ids, per = [], {"video": 0, "audio": 0, "data": 0}
        for i in range(count):
            cid = f"{base_index + i + 1:0{width}d}"
            # Préfixe facultatif : un seul « _ » s'il est absent (Tx_05 au lieu de Tx__05).
            base = "_".join(p for p in (klabel, prefix, cid) if p)
            if inc_v:
                ids.append(_mk("video", f"{base}_Video", base)); per["video"] += 1
            for j in range(ac):
                lbl = f"{base}_Audio{j+1}" if ac > 1 else f"{base}_Audio"
                ids.append(_mk("audio", lbl, base)); per["audio"] += 1
            if inc_d:
                ids.append(_mk("data", f"{base}_Anc", base)); per["data"] += 1
        notify_state_change()
        return jsonify({"ok": True, "ids": ids, "count": len(ids), "per_essence": per, "channels": count})

    @bp.route("/api/nmos/registry/<rid>", methods=["PATCH"])
    @require_perm("settings.edit")
    def nmos_registry_patch(rid):
        """Relabel (op-owné) et/ou édition transport (ressources manuelles seulement)."""
        from app.database import (db_nmos_resource_get, db_nmos_resource_set_label,
                                  db_nmos_resource_set_transport)
        row = db_nmos_resource_get(rid)
        if not row:
            return jsonify({"ok": False, "error": "ressource introuvable"}), 404
        d = request.json or {}
        if "label" in d:
            lbl = (d.get("label") or "").strip()
            if not lbl:
                return jsonify({"ok": False, "error": "label vide"}), 400
            db_nmos_resource_set_label(rid, lbl)
        if "transport" in d and isinstance(d["transport"], dict):
            if row.get("bind_instance_uuid"):
                return jsonify({"ok": False,
                                "error": "transport non éditable (ressource servie par un conteneur)"}), 409
            db_nmos_resource_set_transport(rid, d["transport"])
        notify_state_change()
        return jsonify({"ok": True})

    @bp.route("/api/nmos/registry/<rid>", methods=["DELETE"])
    @require_perm("settings.edit")
    def nmos_registry_delete(rid):
        """Suppression — refusée (409) si la ressource est servie par un conteneur live."""
        from app.database import db_nmos_resource_get, db_nmos_resource_delete, db_get_containers
        row = db_nmos_resource_get(rid)
        if not row:
            return jsonify({"ok": False, "error": "ressource introuvable"}), 404
        iu = row.get("bind_instance_uuid")
        if iu and any(c.get("instance_uuid") == iu for c in db_get_containers()):
            return jsonify({"ok": False,
                            "error": "ressource servie par un conteneur actif — retirez le conteneur d'abord"}), 409
        db_nmos_resource_delete(rid)
        notify_state_change()
        return jsonify({"ok": True})

    @bp.route("/api/nmos/registry/purge_orphans", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_registry_purge_orphans():
        """Purge (ou aperçu si ?dry_run=1) les ressources auto-seedées orphelines. Garde le pool
        fixe (label_locked), les servies, les bindées et les abonnées."""
        dry = request.args.get("dry_run") in ("1", "true", "yes")
        return jsonify({"ok": True, **purge_orphan_resources(dry_run=dry)})

    @bp.route("/api/nmos/registry/delete_all", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_registry_delete_all():
        """TOUT supprimer : vide le registre NMOS (toutes les ressources, pool compris) ET les
        nmos_bind de tous les conteneurs. Destructif (les UUID sont perdus → le contrôleur perd son
        routage). En mode auto, les ressources auto-seedées se régénèrent au rebuild."""
        import json as _json
        from app.database import (db_nmos_resources, db_nmos_resource_delete, db_get_containers,
                                  db_update_deploy_config)
        n = 0
        for r in db_nmos_resources():
            db_nmos_resource_delete(r["id"]); n += 1
        cleared = 0
        for c in db_get_containers():
            try:
                dc = _json.loads(c.get("deploy_config") or "{}")
            except Exception:
                continue
            params = dc.get("params") or {}
            if params.get("nmos_bind"):
                params["nmos_bind"] = {}
                db_update_deploy_config(c["vmid"], dc.get("type"), params)
                cleared += 1
        notify_state_change()
        return jsonify({"ok": True, "deleted": n, "containers_cleared": cleared})

    @bp.route("/api/nmos/config/export", methods=["GET"])
    @require_login
    def nmos_config_export():
        """Exporte la config NMOS en JSON (UUID préservés). Téléchargement fichier."""
        import datetime as _dt
        resp = jsonify(nmos_config_snapshot())
        resp.headers["Content-Disposition"] = "attachment; filename=nmos-config-%s.json" % _dt.date.today()
        return resp

    @bp.route("/api/nmos/config/import", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_config_import():
        """Rappel de config depuis un fichier. Body: {config:{…}, mode, restore_bindings, apply_mode}."""
        d = request.json or {}
        ok, payload = nmos_config_apply(d.get("config") or {}, (d.get("mode") or "merge").strip(),
                                        bool(d.get("restore_bindings", True)), bool(d.get("apply_mode")))
        return jsonify({"ok": ok, **payload}), (200 if ok else 400)

    # ─── Snapshots nommés (sauvegardes serveur rappelables) ──────────────────────
    @bp.route("/api/nmos/snapshots", methods=["GET"])
    @require_login
    def nmos_snapshots_list():
        from app.database import db_nmos_snapshots_list
        return jsonify({"snapshots": db_nmos_snapshots_list()})

    @bp.route("/api/nmos/snapshots", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_snapshots_save():
        from app.database import db_nmos_snapshot_save
        d = request.json or {}
        name = (d.get("name") or "").strip()
        if not name:
            return jsonify({"ok": False, "error": "nom requis"}), 400
        cfg = nmos_config_snapshot()
        sid = db_nmos_snapshot_save(name, cfg)
        n = len(cfg.get("resources") or [])
        return jsonify({"ok": True, "id": sid, "name": name, "resources": n})

    @bp.route("/api/nmos/snapshots/<int:sid>/recall", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_snapshots_recall(sid):
        from app.database import db_nmos_snapshot_get
        snap = db_nmos_snapshot_get(sid)
        if not snap:
            return jsonify({"ok": False, "error": "snapshot introuvable"}), 404
        d = request.json or {}
        ok, payload = nmos_config_apply(snap.get("config") or {}, (d.get("mode") or "merge").strip(),
                                        bool(d.get("restore_bindings", True)), bool(d.get("apply_mode")))
        return jsonify({"ok": ok, "name": snap.get("name"), **payload}), (200 if ok else 400)

    @bp.route("/api/nmos/snapshots/<int:sid>", methods=["DELETE"])
    @require_perm("settings.edit")
    def nmos_snapshots_delete(sid):
        from app.database import db_nmos_snapshot_delete
        db_nmos_snapshot_delete(sid)
        return jsonify({"ok": True})

    @bp.route("/api/nmos/cluster_label", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_cluster_label():
        d = request.json or {}
        lbl = (d.get("label") or "").strip() or "Bobi.Studio 2110 I/O"
        db_set_setting("nmos_cluster_label", lbl)
        notify_state_change()
        return jsonify({"ok": True, "label": lbl})

    # ─── C2b+ : rebinding explicite (un slot conteneur sert une ressource du registre) ───────────
    def _slot_essence_kind(slot_key, res_kind):
        """(slot_key, kind ressource) → (essence, kind, recv_idx|None). 'a:N' est ambigu
        (receiver audio vs sender audio global) → désambiguïsé par le kind de la ressource ciblée."""
        sk = slot_key
        if sk.startswith("tx"):                      # sender moteur : tx{i}:v / tx{i}:a{ai} / tx{i}:d
            rest = sk.split(":", 1)[1] if ":" in sk else ""
            if rest == "v": return ("video", "sender", None)
            if rest == "d": return ("data",  "sender", None)
            if rest.startswith("a"): return ("audio", "sender", None)
            return (None, None, None)
        if res_kind == "receiver":                   # v:{idx} / a:{idx} / d:{idx}
            for pfx, ess in (("v:", "video"), ("a:", "audio"), ("d:", "data")):
                if sk.startswith(pfx):
                    try: return (ess, "receiver", int(sk[len(pfx):]))
                    except ValueError: return (None, None, None)
            return (None, None, None)
        if sk == "v": return ("video", "sender", None)       # sender vidéo global
        if sk.startswith("a:"): return ("audio", "sender", None)  # sender audio global
        return (None, None, None)

    def _resync_after_bind(vmid, params, ess=None, recv_idx=None, rid=None, rx_slots=None):
        """Resync l'émission/souscription après (dé)liaison. TX : repousse les slots (1 fois). RX :
        ré-applique le SDP actif de chaque ressource au conteneur servant (l'agent re-souscrit).
        `rx_slots` = liste optionnelle de (ess, recv_idx, rid) pour le bind en masse (1 push TX + N
        re-souscriptions ciblées) ; le triplet (ess, recv_idx, rid) reste accepté pour le cas unitaire."""
        try:
            from app import docker_driver
            if params.get("tx_slots"):
                docker_driver.push_tx_slots(vmid, params)
        except Exception as e:
            log.warning("resync push_tx_slots %s: %s", vmid, e)
        slots = list(rx_slots or [])
        if rid is not None and recv_idx is not None and ess:
            slots.append((ess, recv_idx, rid))
        for (e, ri, r) in slots:
            sdp = (((_recv_state.get(r) or {}).get("active") or {}).get("transport_file") or {}).get("data")
            if sdp:
                try:
                    manual_subscribe(vmid, ri, {"data": "anc"}.get(e, e), sdp, enable=True)
                except Exception as exc:
                    log.warning("resync re-subscribe %s/%s: %s", vmid, ri, exc)

    @bp.route("/api/nmos/slots", methods=["GET"])
    @require_login
    def nmos_slots():
        return jsonify({"slots": describe_slots()})

    @bp.route("/api/nmos/bindable_containers", methods=["GET"])
    @require_login
    def nmos_bindable_containers():
        """Conteneurs 2110 + slots potentiels pour l'UI d'affectation en masse. ?kind=receiver|sender."""
        kind = request.args.get("kind") or "receiver"
        if kind not in ("receiver", "sender"):
            kind = "receiver"
        return jsonify({"kind": kind, "containers": bindable_containers(kind)})

    def _bind_validate(rid, slot_key):
        """(ess, kind, recv_idx) | (None, erreur). Valide ressource + compat essence/kind."""
        from app.database import db_nmos_resource_get
        res = db_nmos_resource_get(rid)
        if not res:
            return None, "ressource introuvable"
        ess, kind, recv_idx = _slot_essence_kind(slot_key, res["kind"])
        if kind is None:
            return None, f"slot_key invalide: {slot_key}"
        if res["kind"] != kind or res["essence"] != ess:
            return None, "essence/kind incompatibles (ressource vs slot)"
        return (ess, kind, recv_idx), None

    def _strip_rid_from_others(rid, keep_vmid, touched):
        """Exclusivité : retire `rid` du nmos_bind de tout AUTRE conteneur (écriture DB). Accumule
        {vmid: params} dans `touched` pour un resync unique par conteneur côté appelant."""
        import json as _json
        from app.database import db_get_containers, db_update_deploy_config
        for oc in db_get_containers():
            if oc["vmid"] == keep_vmid:
                continue
            try:
                odc = _json.loads(oc.get("deploy_config") or "{}")
            except Exception:
                continue
            op = odc.get("params") or {}
            ob = op.get("nmos_bind") or {}
            stale = [k for k, v in ob.items() if v == rid]
            if stale:
                for k in stale:
                    ob.pop(k, None)
                op["nmos_bind"] = ob
                db_update_deploy_config(oc["vmid"], odc.get("type"), op)
                touched[oc["vmid"]] = op

    @bp.route("/api/nmos/bind", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_bind_route():
        import json as _json
        from app.database import db_get_container, db_update_deploy_config
        d = request.json or {}
        rid = (d.get("resource_id") or "").strip()
        vmid = d.get("vmid")
        slot_key = (d.get("slot_key") or "").strip()
        if not rid or vmid is None or not slot_key:
            return jsonify({"ok": False, "error": "resource_id, vmid, slot_key requis"}), 400
        meta, err = _bind_validate(rid, slot_key)
        if err:
            return jsonify({"ok": False, "error": err}), (404 if "introuvable" in err else 400)
        ess, kind, recv_idx = meta
        c = db_get_container(int(vmid))
        if not c:
            return jsonify({"ok": False, "error": "conteneur introuvable"}), 404
        touched = {}
        _strip_rid_from_others(rid, int(vmid), touched)
        try:
            dc = _json.loads(c.get("deploy_config") or "{}")
        except Exception:
            dc = {}
        params = dc.get("params") or {}
        nb = params.get("nmos_bind") or {}
        nb[slot_key] = rid
        params["nmos_bind"] = nb
        db_update_deploy_config(int(vmid), dc.get("type"), params)
        notify_state_change()   # rebuild → le slot sert l'UUID/transport de la ressource
        _resync_after_bind(int(vmid), params, ess=ess, recv_idx=recv_idx, rid=rid)
        for ovmid, op in touched.items():
            _resync_after_bind(ovmid, op)
        return jsonify({"ok": True})

    def _bind_bulk_apply(vmid, mappings, unbind_missing=False, clear_scope=None):
        """Cœur du bind en masse : applique tous les mappings d'UN conteneur en 1 rebuild + 1 resync.
        Renvoie (code, payload). Partagé par /bind_bulk et /automap.
        `clear_scope` (set de slot_keys) borne `unbind_missing` : on ne retire QUE les clés de ce
        périmètre non re-mappées → un re-map Tx n'efface pas les binds Rx (et inversement)."""
        import json as _json
        from app.database import db_get_container, db_update_deploy_config
        c = db_get_container(int(vmid))
        if not c:
            return 404, {"ok": False, "error": "conteneur introuvable"}
        try:
            dc = _json.loads(c.get("deploy_config") or "{}")
        except Exception:
            dc = {}
        params = dc.get("params") or {}
        nb = params.get("nmos_bind") or {}
        touched = {}
        results, rx_slots, bound_keys = [], [], []
        for m in mappings:
            rid = (m or {}).get("resource_id")
            slot_key = ((m or {}).get("slot_key") or "").strip()
            if not rid or not slot_key:
                results.append({"slot_key": slot_key, "ok": False, "error": "resource_id/slot_key manquant"})
                continue
            meta, err = _bind_validate(rid, slot_key)
            if err:
                results.append({"slot_key": slot_key, "ok": False, "error": err})
                continue
            ess, kind, recv_idx = meta
            _strip_rid_from_others(rid, int(vmid), touched)
            nb[slot_key] = rid
            bound_keys.append(slot_key)
            if kind == "receiver" and recv_idx is not None:
                rx_slots.append((ess, recv_idx, rid))
            results.append({"slot_key": slot_key, "ok": True})
        if unbind_missing:                      # re-map propre : retirer les binds non re-spécifiés
            scope = set(clear_scope) if clear_scope is not None else None
            for k in [k for k in list(nb) if k not in bound_keys and (scope is None or k in scope)]:
                nb.pop(k, None)
        params["nmos_bind"] = nb
        db_update_deploy_config(int(vmid), dc.get("type"), params)
        notify_state_change()                   # 1 seul rebuild pour tout le lot
        _resync_after_bind(int(vmid), params, rx_slots=rx_slots)
        for ovmid, op in touched.items():
            _resync_after_bind(ovmid, op)
        return 200, {"ok": True, "results": results,
                     "bound": len([r for r in results if r.get("ok")])}

    @bp.route("/api/nmos/bind_bulk", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_bind_bulk_route():
        """Bind en masse de plusieurs slots d'UN conteneur en 1 rebuild + 1 resync.
        Body: {vmid, mappings:[{slot_key, resource_id}], unbind_missing?}. Renvoie un statut
        par mapping ; l'exclusivité déplace une ressource déjà servie ailleurs."""
        d = request.json or {}
        vmid = d.get("vmid")
        mappings = d.get("mappings") or []
        if vmid is None or not isinstance(mappings, list):
            return jsonify({"ok": False, "error": "vmid et mappings requis"}), 400
        code, payload = _bind_bulk_apply(int(vmid), mappings, bool(d.get("unbind_missing")))
        return jsonify(payload), code

    @bp.route("/api/nmos/automap", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_automap_route():
        """Affecte EN MASSE les flux d'un conteneur à un pool fixe, PAR CANAL (bundle). Body:
        {vmid, kind:"receiver"|"sender", pool:{video:[rid...],audio:[...],data:[...]},
         include:{video:bool, audio_count:0|1|2, anc:bool}, clear_first?}.
        Pour chaque vidéo : câble vidéo + N audio + ANC ensemble (pool consommé par canal).
        `include` absent ⇒ tout coché, audio_count=1 (≈ ancien zip par essence)."""
        import json as _json
        from app.database import db_get_container
        d = request.json or {}
        vmid = d.get("vmid")
        kind = (d.get("kind") or "receiver").strip()
        pool = d.get("pool") or {}
        include = d.get("include") or {"video": True, "audio_count": 1, "anc": True}
        if vmid is None or kind not in ("receiver", "sender"):
            return jsonify({"ok": False, "error": "vmid et kind (receiver|sender) requis"}), 400
        c = db_get_container(int(vmid))
        if not c:
            return jsonify({"ok": False, "error": "conteneur introuvable"}), 404
        try:
            _params = (_json.loads(c.get("deploy_config") or "{}").get("params") or {})
        except Exception:
            _params = {}
        mappings, unmatched = _bundle_mappings(_params, kind, pool, include)
        if not mappings:
            return jsonify({"ok": False, "error": "aucun canal à mapper (conteneur déployé ? essences cochées ? pool dispo ?)"}), 400
        # clear_first ne purge QUE les slots du sens courant (sinon un re-map Tx efface les binds Rx).
        _scope = set()
        for _ks in _container_slot_keys(_params, kind).values():
            _scope.update(_ks)
        code, payload = _bind_bulk_apply(int(vmid), mappings, bool(d.get("clear_first")), clear_scope=_scope)
        payload["unmatched"] = unmatched
        return jsonify(payload), code

    @bp.route("/api/nmos/unbind", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_unbind_route():
        import json as _json
        from app.database import db_get_container, db_update_deploy_config
        d = request.json or {}
        vmid = d.get("vmid")
        slot_key = (d.get("slot_key") or "").strip()
        if vmid is None or not slot_key:
            return jsonify({"ok": False, "error": "vmid, slot_key requis"}), 400
        c = db_get_container(int(vmid))
        if not c:
            return jsonify({"ok": False, "error": "conteneur introuvable"}), 404
        try:
            dc = _json.loads(c.get("deploy_config") or "{}")
        except Exception:
            dc = {}
        params = dc.get("params") or {}
        nb = params.get("nmos_bind") or {}
        if slot_key in nb:
            nb.pop(slot_key, None)
            params["nmos_bind"] = nb
            db_update_deploy_config(int(vmid), dc.get("type"), params)
            notify_state_change()
            _resync_after_bind(int(vmid), params)
        return jsonify({"ok": True})

    @bp.route("/api/nmos/unbind_bulk", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_unbind_bulk_route():
        """Délie EN MASSE plusieurs slots d'UN conteneur en 1 rebuild + 1 resync (symétrique de
        bind_bulk). Body: {vmid, slot_keys:[...]}."""
        import json as _json
        from app.database import db_get_container, db_update_deploy_config
        d = request.json or {}
        vmid = d.get("vmid")
        slot_keys = d.get("slot_keys") or []
        if vmid is None or not isinstance(slot_keys, list):
            return jsonify({"ok": False, "error": "vmid et slot_keys requis"}), 400
        c = db_get_container(int(vmid))
        if not c:
            return jsonify({"ok": False, "error": "conteneur introuvable"}), 404
        try:
            dc = _json.loads(c.get("deploy_config") or "{}")
        except Exception:
            dc = {}
        params = dc.get("params") or {}
        nb = params.get("nmos_bind") or {}
        removed = [k for k in slot_keys if nb.pop(k, None) is not None]
        if removed:
            params["nmos_bind"] = nb
            db_update_deploy_config(int(vmid), dc.get("type"), params)
            notify_state_change()
            _resync_after_bind(int(vmid), params)
        return jsonify({"ok": True, "removed": len(removed)})

    @bp.route("/api/nmos/container_reset", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_container_reset_route():
        """Dé-assigne un conteneur : vide ses binds + supprime ses ressources auto-seedées (le pool
        fixe est conservé). Body: {vmid}."""
        d = request.json or {}
        vmid = d.get("vmid")
        if vmid is None:
            return jsonify({"ok": False, "error": "vmid requis"}), 400
        res = reset_container_resources(int(vmid))
        return jsonify(res), (200 if res.get("ok") else 404)

    @bp.route("/api/nmos/sriov/status", methods=["GET"])
    @require_login
    def nmos_sriov_status():
        from app import settings as _st
        from app.addressing import primary_host
        from app.host_ops import list_vfs
        from app.database import db_get_assigned_vfs
        pf       = _st.get("nmos_2110_pf") or ""
        enabled  = bool(_st.get("nmos_2110_enabled"))
        vf_count = int(_st.get("nmos_2110_vf_count") or 0)
        vfs = []
        err = None
        if enabled and pf:
            ok, lst, msg = list_vfs(primary_host(), pf)
            if ok:
                vfs = lst
            else:
                err = msg
        used       = db_get_assigned_vfs()
        used_by_vf = {v: vmid for vmid, v in used.items()}
        return jsonify({
            "enabled":        enabled,
            "pf":             pf,
            "vf_count_target": vf_count,
            "vfs":            [{"name": v, "assigned_to": used_by_vf.get(v)} for v in vfs],
            "error":          err,
        })

    @bp.route("/api/nmos/sriov/init", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_sriov_init():
        from app import settings as _st
        from app.addressing import primary_host
        from app.host_ops import ensure_sriov_pool
        pf = _st.get("nmos_2110_pf") or ""
        n  = int(_st.get("nmos_2110_vf_count") or 0)
        if not pf:
            return jsonify({"ok": False, "error": "nmos_2110_pf non renseigné"}), 400
        ok, msg = ensure_sriov_pool(primary_host(), pf, n)
        return jsonify({"ok": ok, "msg": msg})

    @bp.route("/api/nmos/sriov/reconcile", methods=["GET"])
    @require_login
    def nmos_sriov_reconcile():
        from app import settings as _st
        from app.addressing import primary_host
        from app.host_ops import reconcile_vf_assignments
        pf = _st.get("nmos_2110_pf") or ""
        if not pf:
            return jsonify({"ok": False, "error": "nmos_2110_pf non renseigné"}), 400
        return jsonify(reconcile_vf_assignments(primary_host(), pf))

    @bp.route("/api/nmos/sriov/fix", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_sriov_fix():
        from app import settings as _st
        from app.addressing import primary_host
        from app.host_ops import fix_vf_assignments
        pf = _st.get("nmos_2110_pf") or ""
        if not pf:
            return jsonify({"ok": False, "error": "nmos_2110_pf non renseigné"}), 400
        return jsonify(fix_vf_assignments(primary_host(), pf))


# Les endpoints IS-14 vivent sur CE blueprint (celui d'IS-04/IS-05, monté par main.py), pas sur
# celui de l'API interne : c'est du NMOS, il doit être sous /x-nmos/. L'import est en fin de
# module pour que les routes soient greffées AVANT que main.py n'enregistre le blueprint — et il
# doit rester ici, pas en tête : `is14` importe `modele`, qui importe `monitors`, qui remonte à
# `app.metrics`. Un import en tête de fichier boucle.
from . import is14 as _is14          # noqa: E402
_is14.enregistrer(bp)

# Registre IS-04 embarqué (Registration + Query API). Même raison d'être ici qu'IS-14 : c'est du
# NMOS, donc sous /x-nmos/, et les routes doivent être greffées avant que main.py n'enregistre le
# blueprint. Servi seulement si le réglage `nmos_registre` est posé — les routes existent
# toujours, mais répondent 501 tant que l'exploitant n'a pas ouvert cette surface externe.
from . import registre as _registre  # noqa: E402
_registre.enregistrer(bp)

from . import supervision_tiers as _sup_tiers  # noqa: E402
_sup_tiers.enregistrer(bp)

from . import is07 as _is07  # noqa: E402
_is07.enregistrer(bp)

from . import is09 as _is09  # noqa: E402
_is09.enregistrer(bp)
