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

bp = Blueprint("nmos", __name__)

# ═════════════════════════════════════════════════════════════════════
# État global (singleton process-level)
# ═════════════════════════════════════════════════════════════════════

_lock = threading.RLock()
_running = False
_register_thread = None
_mdns_zc = None         # zeroconf instance
_mdns_service = None    # ServiceInfo enregistré
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


def _build_sender_resource(snd_id, did, fid, vmid, label, version):
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
        "interface_bindings": [_primary_iface()],
        "subscription": {"receiver_id": None, "active": False},
        "caps": {},
    }


def _build_receiver_resource(rid, did, vmid, recv_idx, label, version, fmt="video"):
    media_types = {"video": ["video/raw"], "audio": ["audio/L24"], "data": ["video/smpte291"]}[fmt]
    return {
        "id": rid,
        "version": version,
        "label": label,
        "description": f"Receiver {fmt} #{recv_idx} sur container {vmid}",
        "tags": {"urn:x-mxl:vmid": [str(vmid)], "urn:x-mxl:receiver_index": [str(recv_idx)]},
        "device_id": did,
        "transport": "urn:x-nmos:transport:rtp.mcast",
        "format": f"urn:x-nmos:format:{fmt}",
        "subscription": {"sender_id": None, "active": False},
        "caps": {"media_types": media_types},
        "interface_bindings": [_primary_iface()],
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
                 label="", transport=None, group_name="", role="", bind_override=None):
    """C2a : id NMOS STABLE depuis le registre cluster (table nmos_resources), découplé du vmid.
    Lookup par (instance_uuid, slot_key, essence, kind) :
      - 1re fois (registre vide pour ce slot) → id = formule ACTUELLE (`_stable_uuid(current_seed)`,
        seedée vmid) → PRÉSERVE l'UUID existant (abonnements IS-05 intacts) ;
      - ensuite → id stable retrouvé via l'instance_uuid (survit recreate/projet, vmid changé).
    Rafraîchit aussi label/transport/binding (le slot peut changer de conteneur). Sans instance_uuid
    (ne devrait pas arriver post-C1) → fallback formule actuelle, hors registre.
    Renvoie `(id, label_effectif)` : si l'op a figé le libellé (label_locked), `label_effectif` est
    celui du registre → le caller construit la ressource avec, pas avec le hostname du conteneur."""
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
    rid = ex["id"] if ex else _stable_uuid(current_seed)
    # C2b : si le libellé a été figé par l'op (label_locked), on le PRÉSERVE (ne pas réécrire avec le
    # hostname du conteneur) ; sinon il suit le conteneur servant.
    eff_label = ex["label"] if (ex and ex.get("label_locked")) else label
    db_nmos_resource_upsert(rid, kind, essence, eff_label, group_name, role,
                            transport or {}, instance_uuid, slot_key)
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
        "tags": {},
        "type": "urn:x-nmos:device:generic",
        "node_id": _state["node_id"],
        "senders": [],
        "receivers": [],
        "controls": [{
            "href": f"http://{_get_host_address()}:5000/x-nmos/connection/{IS05_VERSION}/",
            "type": f"urn:x-nmos:control:sr-ctrl/{IS05_VERSION}",
        }],
    }

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
        "tags": {"urn:x-mxl:vmid": [str(vmid)]},
        "type": "urn:x-nmos:device:generic",
        "node_id": _state["node_id"],
        "senders": [],     # Phase 2 : peuplé pour les workers 2110_sender
        "receivers": [],
        "controls": [{
            "href": f"http://{_get_host_address()}:5000/x-nmos/connection/{IS05_VERSION}/",
            "type": f"urn:x-nmos:control:sr-ctrl/{IS05_VERSION}",
        }],
    }

def _build_node_resource(version):
    host = _get_host_address()
    return {
        "id": _state["node_id"],
        "version": version,
        "label": _setting("nmos_node_label", "MXL Orchestrator"),
        "description": _setting("nmos_node_description", "Bobi.Studio — provider NMOS centralisé"),
        "tags": {},
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
    group_name sur le même device ; les contrôleurs NMOS regroupent dessus."""
    gn = str(group_name).replace(":", " ")
    rl = str(role).replace(":", " ")
    resource.setdefault("tags", {})[GROUPHINT_TAG] = [f"{gn}:{rl}"]

def rebuild_model():
    """Reconstruit la liste des devices / receivers / senders / sources / flows
    depuis l'état DB. Préserve les staged/active states existants (matché par id).

    Natural grouping (BCP-002-01) : chaque ensemble « 1 vidéo + ses audios » porte
    un même group_name → le contrôleur l'affiche comme un bundle distinct. Côté
    receiver, les audios sont répartis sur les groupes vidéo (audio j → vidéo j %
    n_video) ; côté sender (≤1 vidéo) tout le container forme un seul bundle."""
    from app.database import db_get_containers
    version = _tai_version()
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
        # Counts par essence — lus depuis deploy_config.params (source de vérité unique).
        # active_rx_count / active_tx_count limitent combien de slots apparaissent dans NMOS
        # (les queues MTL sous-jacentes sont toutes allouées ; c'est une fenêtre de visibilité).
        n_video_full = int(dc_params.get("video_count", 0) or 0) if is_receiver else 0
        _arc = dc_params.get("active_rx_count")
        n_video = min(int(_arc if _arc is not None else n_video_full), n_video_full) if is_receiver else 0
        aper = int(dc_params.get("audio_per_video") or 0)
        n_audio_full = int(dc_params.get("audio_count", 0) or 0) if is_receiver else 0
        if aper > 0 and is_receiver:
            n_audio = n_video * aper          # audio suit la vidéo proportionnellement
        else:
            n_audio = min(int(dc_params.get("active_rx_count") or n_audio_full), n_audio_full) if is_receiver else 0
        n_data_full = int(dc_params.get("anc_count", 0) or 0) if is_receiver else 0
        n_data = min(n_video, n_data_full) if is_receiver else 0   # 1 ANC par groupe vidéo, ≤ anc_count
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
        base = c.get("hostname") or f"container {vmid}"

        # Nom de bundle d'un ensemble : si plusieurs vidéos sur le container, on
        # suffixe l'index pour distinguer les bundles ; sinon le container = 1 bundle.
        def _grp_name(group_idx):
            return f"{base} {group_idx + 1}" if n_video > 1 else base

        # Receivers vidéo — un bundle (group_name) par vidéo
        for idx in range(n_video):
            label = f"{c.get('hostname') or vmid} #r{idx} (video)"
            rid, label = _registry_id(f"receiver:v:{vmid}:{idx}", instance_uuid, f"v:{idx}", "video",
                                      "receiver", label, {}, _grp_name(idx), "video",
                                      bind_override=nmos_bind.get(f"v:{idx}"))
            new_receivers[rid] = _build_receiver_resource(rid, did, vmid, idx, label, version, fmt="video")
            _set_grouphint(new_receivers[rid], _grp_name(idx), "video")
            new_devices[did]["receivers"].append(rid)
            cur = _recv_state.get(rid)
            if not cur or len(cur["staged"]["transport_params"]) != n_legs:
                _recv_state[rid] = {
                    "staged": _empty_staged(n_legs), "active": _empty_staged(n_legs),
                    "vmid": vmid, "recv_idx": idx, "essence": "video",
                }

        # Receivers audio — rattachés au bundle vidéo (audio j → vidéo j % n_video)
        _audio_role_ctr = {}
        for idx in range(n_audio):
            label = f"{c.get('hostname') or vmid} #ra{idx} (audio)"
            grp = _grp_name(idx % n_video) if n_video > 0 else base
            n = _audio_role_ctr.get(grp, 0) + 1
            _audio_role_ctr[grp] = n
            rid, label = _registry_id(f"receiver:a:{vmid}:{idx}", instance_uuid, f"a:{idx}", "audio",
                                      "receiver", label, {}, grp, f"audio {n}",
                                      bind_override=nmos_bind.get(f"a:{idx}"))
            new_receivers[rid] = _build_receiver_resource(rid, did, vmid, idx, label, version, fmt="audio")
            _set_grouphint(new_receivers[rid], grp, f"audio {n}")
            new_devices[did]["receivers"].append(rid)
            cur = _recv_state.get(rid)
            if not cur or len(cur["staged"]["transport_params"]) != n_legs:
                _recv_state[rid] = {
                    "staged": _empty_staged(n_legs), "active": _empty_staged(n_legs),
                    "vmid": vmid, "recv_idx": idx, "essence": "audio",
                }

        # Receivers ANC (2110-40 / data) — rattachés au bundle vidéo (anc j → vidéo j % n_video)
        _data_role_ctr = {}
        for idx in range(n_data):
            label = f"{c.get('hostname') or vmid} #rd{idx} (anc)"
            grp = _grp_name(idx % n_video) if n_video > 0 else base
            n = _data_role_ctr.get(grp, 0) + 1
            _data_role_ctr[grp] = n
            rid, label = _registry_id(f"receiver:d:{vmid}:{idx}", instance_uuid, f"d:{idx}", "data",
                                      "receiver", label, {}, grp, f"anc {n}",
                                      bind_override=nmos_bind.get(f"d:{idx}"))
            new_receivers[rid] = _build_receiver_resource(rid, did, vmid, idx, label, version, fmt="data")
            _set_grouphint(new_receivers[rid], grp, f"anc {n}")
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
            mcast = v.get("multicast_ip") or "239.10.10.1"
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
            label  = f"{c.get('hostname') or vmid} 2110-20"
            _scan = str(_pp.get("scan") or v.get("scan") or "p")
            _fo   = str(_pp.get("field_order") or v.get("field_order") or "")
            _tr = {"multicast_ip": mcast, "port": port, "width": width, "height": height,
                   "chroma": chroma, "bit_depth": bit_depth, "colorspace": cs, "transfer": transfer,
                   "scan": _scan, "field_order": _fo, "fps": v.get("fps")}
            snd_id, label = _registry_id(f"sender:v:{vmid}", instance_uuid, "v", "video",
                                         "sender", label, _tr, base, "video",
                                         bind_override=nmos_bind.get("v"))
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
            mcast = tslot.get("multicast_ip") or "239.10.10.1"
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
            _tx_shm = dc_params.get(f"tx{tx_idx}_shm") or ""
            _src_fmt = {}
            if _tx_shm:
                try:
                    from app.monitor import _shm_fmt as _mtl_shm_fmt
                    _src_fmt = _mtl_shm_fmt(_tx_shm) or {}
                except Exception:
                    _src_fmt = {}
            _scan = str(_src_fmt.get("scan") or dc_params.get("scan") or "p")
            _fo   = str(_src_fmt.get("field_order") or dc_params.get("field_order") or "")
            label  = f"{c.get('hostname') or vmid} TX{tx_idx} 2110-20"
            _tr = {"multicast_ip": mcast, "port": port, "width": width, "height": height,
                   "chroma": chroma, "bit_depth": bit_depth, "colorspace": cs, "transfer": transfer,
                   "scan": _scan, "field_order": _fo, "fps": tslot.get("fps")}
            snd_id, label = _registry_id(f"sender:v:{vmid}:tx{tx_idx}", instance_uuid, f"tx{tx_idx}:v",
                                         "video", "sender", label, _tr, f"{base} TX {tx_idx + 1}", "video",
                                         bind_override=nmos_bind.get(f"tx{tx_idx}:v"))
            src_id = _stable_uuid(f"source:{snd_id}")
            fid    = _stable_uuid(f"flow:{snd_id}")
            new_sources[src_id] = _build_source_resource(src_id, did, vmid, label, version)
            new_flows[fid]      = _build_flow_resource(fid, did, src_id, vmid, label, width, height,
                                                       version, chroma, bit_depth, cs, transfer, _scan, _fo)
            new_senders[snd_id] = _build_sender_resource(snd_id, did, fid, vmid, label, version)
            # Groupé PAR SLOT TX (comme les receivers le sont par canal) : vidéo + audio(s)
            # + ANC d'un même slot partagent le group_name, le rôle = essence.
            _set_grouphint(new_senders[snd_id], f"{base} TX {tx_idx + 1}", "video")
            new_devices[did]["senders"].append(snd_id)
            smpte_v = bool(tslot.get("smpte_2022_7") or dc_params.get("smpte_2022_7"))
            mcast1_v = tslot.get("multicast_ip_leg1")
            port1_v  = tslot.get("dest_port_leg1")
            leg1_v   = (mcast1_v, port1_v) if (smpte_v and mcast1_v and port1_v) else None
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
                cur_legs = len(_send_state[snd_id]["staged"].get("transport_params") or [])
                want_legs = 2 if leg1_v else 1
                if cur_legs != want_legs:
                    empty = _empty_sender_staged(mcast, port, leg1=leg1_v)
                    _send_state[snd_id]["staged"] = empty
                    _send_state[snd_id]["active"] = json.loads(json.dumps(empty))

        # Senders audio (0, 1 ou 2)
        for a_idx, a in enumerate((dc.get("params") or {}).get("audios") or []):
            mcast = a.get("multicast_ip") or "239.10.20.1"
            port  = int(a.get("dest_port") or (5004 + 2 * a_idx))
            label  = f"{c.get('hostname') or vmid} 2110-30 #{a_idx}"
            snd_id, label = _registry_id(f"sender:a:{vmid}:{a_idx}", instance_uuid, f"a:{a_idx}", "audio",
                                         "sender", label, {"multicast_ip": mcast, "port": port}, base, f"audio {a_idx + 1}",
                                         bind_override=nmos_bind.get(f"a:{a_idx}"))
            src_id = _stable_uuid(f"source:{snd_id}")
            fid    = _stable_uuid(f"flow:{snd_id}")
            new_sources[src_id] = _build_audio_source_resource(src_id, did, vmid, label, version)
            new_flows[fid]      = _build_audio_flow_resource(fid, did, src_id, vmid, label, version)
            new_senders[snd_id] = _build_sender_resource(snd_id, did, fid, vmid, label, version)
            _set_grouphint(new_senders[snd_id], base, f"audio {a_idx + 1}")
            new_devices[did]["senders"].append(snd_id)
            smpte_2022_7_a = bool(a.get("smpte_2022_7"))
            mcast1_a = a.get("multicast_ip_leg1")
            port1_a  = a.get("dest_port_leg1")
            leg1_a   = (mcast1_a, port1_a) if (smpte_2022_7_a and mcast1_a and port1_a) else None
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
        # audio globaux (sender_2110). Chaque tx_slot peut porter jusqu'à 2 flux audio 2110-30.
        for tx_idx, tslot in enumerate(tx_slots):
            for ai, acfg in enumerate((tslot.get("audios") or [])[:2]):
                mcast = acfg.get("multicast_ip")
                if not mcast:
                    continue
                port   = int(acfg.get("dest_port") or 0)
                label  = f"{c.get('hostname') or vmid} TX{tx_idx} 2110-30 #{ai}"
                snd_id, label = _registry_id(f"sender:a:{vmid}:tx{tx_idx}:{ai}", instance_uuid,
                                             f"tx{tx_idx}:a{ai}", "audio", "sender", label,
                                             {"multicast_ip": mcast, "port": port},
                                             f"{base} TX {tx_idx + 1}", f"audio {ai + 1}",
                                             bind_override=nmos_bind.get(f"tx{tx_idx}:a{ai}"))
                src_id = _stable_uuid(f"source:{snd_id}")
                fid    = _stable_uuid(f"flow:{snd_id}")
                new_sources[src_id] = _build_audio_source_resource(src_id, did, vmid, label, version)
                new_flows[fid]      = _build_audio_flow_resource(fid, did, src_id, vmid, label, version)
                new_senders[snd_id] = _build_sender_resource(snd_id, did, fid, vmid, label, version)
                _set_grouphint(new_senders[snd_id], f"{base} TX {tx_idx + 1}", f"audio {ai + 1}")
                new_devices[did]["senders"].append(snd_id)
                smpte_a = bool(tslot.get("smpte_2022_7") or dc_params.get("smpte_2022_7"))
                mcast1_a = acfg.get("multicast_ip_leg1")
                port1_a  = acfg.get("dest_port_leg1")
                leg1_a   = (mcast1_a, port1_a) if (smpte_a and mcast1_a and port1_a) else None
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

        # Senders ANC MOTEUR (slots TX) — un sender data (2110-40) par slot TX porteur d'une dest ANC.
        # Le TX ANC suit la vidéo câblée (shm dérivé) → même tx_idx que le sender vidéo, essence "anc".
        for tx_idx, tslot in enumerate(tx_slots):
            mcast = tslot.get("anc_multicast_ip")
            if not mcast:
                continue
            port = int(tslot.get("anc_dest_port") or 0)
            label  = f"{c.get('hostname') or vmid} TX{tx_idx} 2110-40"
            snd_id, label = _registry_id(f"sender:d:{vmid}:tx{tx_idx}", instance_uuid, f"tx{tx_idx}:d",
                                         "data", "sender", label, {"multicast_ip": mcast, "port": port},
                                         f"{base} TX {tx_idx + 1}", "anc",
                                         bind_override=nmos_bind.get(f"tx{tx_idx}:d"))
            src_id = _stable_uuid(f"source:{snd_id}")
            fid    = _stable_uuid(f"flow:{snd_id}")
            new_sources[src_id] = _build_data_source_resource(src_id, did, vmid, label, version)
            new_flows[fid]      = _build_data_flow_resource(fid, did, src_id, vmid, label, version)
            new_senders[snd_id] = _build_sender_resource(snd_id, did, fid, vmid, label, version)
            _set_grouphint(new_senders[snd_id], f"{base} TX {tx_idx + 1}", "anc")
            new_devices[did]["senders"].append(snd_id)
            smpte_d = bool(tslot.get("smpte_2022_7") or dc_params.get("smpte_2022_7"))
            mcast1_d = tslot.get("anc_multicast_ip_leg1")
            port1_d  = tslot.get("anc_dest_port_leg1")
            leg1_d   = (mcast1_d, port1_d) if (smpte_d and mcast1_d and port1_d) else None
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
            return (f"tx{tx}:v" if tx is not None else "v"), "video"
        if ess == "audio":
            ai = s.get("audio_idx", 0)
            return (f"tx{tx}:a{ai}" if tx is not None else f"a:{ai}"), "audio"
        if ess == "anc":
            return ((f"tx{tx}:d" if tx is not None else None), "data")
    else:
        idx = s.get("recv_idx", 0)
        if ess == "video": return f"v:{idx}", "video"
        if ess == "audio": return f"a:{idx}", "audio"
        if ess == "anc":   return f"d:{idx}", "data"
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


def _apply_receiver_staged(rid, body):
    """Logique de merge staged + activation pour un receiver. Renvoie (code, dict).
    Utilisé par le PATCH single ET le bulk POST."""
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
    threading.Thread(
        target=_notify_agent,
        args=(vmid, recv_idx, essence, master_enable, sdp, mcast_info),
        daemon=True
    ).start()
    # Propage le format RÉEL (lu du SDP) dans le deploy_config → topologie/câblage/consommateurs
    # (mixer, multiview…) voient le vrai WxH/fps reçu, pas seulement l'affichage I/O. Stream
    # auto-détecte déjà depuis le shm ; ceci aligne les autres. En thread (hors _lock NMOS).
    if essence == "video" and master_enable and sdp:
        threading.Thread(target=_propagate_sdp_format, args=(vmid, sdp), daemon=True).start()
    # Persiste l'état d'abonnement (survie au redémarrage de l'orchestrateur).
    _persist_subscriptions()

def _propagate_sdp_format(vmid, sdp):
    """Écrit width/height/fps/scan lus du SDP dans le deploy_config du receiver (si différent),
    puis notify_state_change → la topologie et les consommateurs voient le vrai format reçu.
    Multi-slot : modèle width/height par container (le dernier flux activé fait foi)."""
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
    changed = (int(params.get("width") or 0) != new_w
               or int(params.get("height") or 0) != new_h
               or str(params.get("scan") or "") != scan
               or str(params.get("field_order") or "") != new_fo)
    if not changed:
        return
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
    Retourne un unique SDP avec deux blocs media (leg0 + leg1)."""
    if not sdp_leg0 or not mcast1 or not port1:
        return sdp_leg0 or ""
    m = re.search(r"(m=(?:video|audio|application) )", sdp_leg0)
    if not m:
        return sdp_leg0
    media_block = sdp_leg0[m.start():]
    leg1 = re.sub(r"(m=(?:video|audio|application) )\d+", rf"\g<1>{int(port1)}", media_block)
    leg1 = re.sub(r"(c=IN IP4 )[\d.]+", rf"\g<1>{mcast1}", leg1)
    leg1 = re.sub(r"(a=source-filter: incl IN IP4 )[\d.]+", rf"\g<1>{mcast1}", leg1)
    return sdp_leg0 + leg1

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
        if m_c and not info0["multicast_ip"]:
            info0["multicast_ip"] = m_c.group(1)
        if m_m and not info0["destination_port"]:
            info0["destination_port"] = int(m_m.group(1))
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
    """POST l'info de subscription à l'agent du container (port 8081)."""
    from app.addressing import get_container_ip
    from app.database import db_add_alert
    ip = get_container_ip(vmid)
    if not ip:
        log.warning(f"nmos: pas d'IP pour container {vmid}, subscription ignorée")
        db_add_alert(f"NMOS subscription receiver #{recv_idx}/{essence} container {vmid}: IP introuvable", "warning")
        return
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
        r = requests.post(f"http://{ip}:8081/nmos/subscribe",
                          json=payload, timeout=5)
        if r.status_code == 200:
            db_add_alert(
                f"NMOS receiver #{recv_idx}/{essence} container {vmid} → "
                f"{'subscribe' if enable else 'unsubscribe'}", "info")
        else:
            log.warning(f"nmos: agent {vmid} a renvoyé {r.status_code}")
            db_add_alert(
                f"NMOS subscription container {vmid} : agent retour {r.status_code}",
                "warning")
    except Exception as e:
        log.warning(f"nmos: notification agent {vmid} échouée : {e}")
        db_add_alert(f"NMOS subscription container {vmid} : agent injoignable ({e})", "warning")


def repush_subscriptions(vmid):
    """Re-pousse vers le contrôleur (:8081/nmos/subscribe) TOUS les abonnements RX ACTIFS du
    container `vmid`. Appelé après une (re)création du conteneur : ses fichiers SDP repartent de
    zéro, mais `_recv_state` survit en mémoire (l'orchestrateur, lui, ne redémarre pas) → on
    restaure les sessions RX SANS intervention du contrôleur NMOS externe (corrige la perte des
    flux à chaque redéploiement). Attend d'abord que l'agent :8081 réponde."""
    from app.addressing import get_container_ip
    import time as _t
    ip = get_container_ip(vmid)
    if not ip:
        return 0
    for _ in range(30):   # readiness :8081 (le contrôleur met quelques s à (re)démarrer)
        try:
            if requests.get(f"http://{ip}:8081/status", timeout=2).status_code == 200:
                break
        except Exception:
            pass
        _t.sleep(1)
    n = 0
    for rid, state in list(_recv_state.items()):
        if state.get("vmid") != vmid:
            continue
        active = state.get("active") or {}
        if not bool(active.get("master_enable")):
            continue
        sdp = (active.get("transport_file") or {}).get("data")
        dual = len(active.get("transport_params") or []) >= 2
        mcast_info = _extract_mcast_info(active, smpte_2022_7=dual)
        _notify_agent(state["vmid"], state["recv_idx"], state.get("essence", "video"),
                      True, sdp, mcast_info)
        n += 1
    if n:
        log.info(f"nmos: {n} abonnement(s) RX re-poussé(s) sur container {vmid} après (re)déploiement")
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
    for vmid in vmids:
        threading.Thread(target=repush_subscriptions, args=(vmid,), daemon=True).start()
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

def _register_all(reg_base):
    """Enregistre node + devices + sources + flows + senders + receivers en cascade.
    L'ordre compte côté RDS : source avant flow, flow avant sender."""
    version = _tai_version()
    node = _build_node_resource(version)
    _register_one(reg_base, "node", node)
    with _lock:
        devs    = list(_devices.values())
        srcs    = list(_sources.values())
        flows   = list(_flows.values())
        sends   = list(_senders.values())
        recvs   = list(_receivers.values())
    for d in devs:   _register_one(reg_base, "device",   d)
    for s in srcs:   _register_one(reg_base, "source",   s)
    for f in flows:  _register_one(reg_base, "flow",     f)
    for s in sends:  _register_one(reg_base, "sender",   s)
    for r in recvs:  _register_one(reg_base, "receiver", r)

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
            while _running and _state["registry_url"] == reg:
                time.sleep(HEARTBEAT_S)
                try:
                    _heartbeat(reg)
                except Exception as e:
                    log.warning(f"nmos: heartbeat échoué : {e}")
                    _state["registered"] = False
                    _state["last_register_error"] = str(e)
                    break  # ré-enregistrer
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

def _mdns_start():
    """Publie le service _nmos-node._tcp.local. avec les TXT records IS-04."""
    global _mdns_zc, _mdns_service
    try:
        from zeroconf import IPVersion, ServiceInfo, Zeroconf
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
    label = _setting("nmos_node_label", "MXL Orchestrator")
    # Le nom DOIT être unique sur le LAN ; on inclut le node_id pour éviter les collisions.
    instance = f"{label} {(_state.get('node_id') or '')[:8]}._nmos-node._tcp.local."
    info = ServiceInfo(
        type_="_nmos-node._tcp.local.",
        name=instance,
        addresses=[addr_bytes],
        port=5000,
        weight=0,
        priority=0,
        properties={
            "api_proto": "http",
            "api_ver":   IS04_VERSION,
            "api_auth":  "false",
        },
        server=f"{socket.gethostname()}.local.",
    )
    try:
        zc = Zeroconf(ip_version=IPVersion.V4Only)
        zc.register_service(info)
    except Exception as e:
        log.warning(f"nmos: échec register mDNS: {e}")
        return False, str(e)
    _mdns_zc = zc
    _mdns_service = info
    _state["mdns_active"] = True
    log.info(f"nmos: mDNS annoncé ({instance} @ {host}:5000)")
    return True, "ok"


def _mdns_stop():
    global _mdns_zc, _mdns_service
    if _mdns_zc and _mdns_service:
        try: _mdns_zc.unregister_service(_mdns_service)
        except Exception: pass
    if _mdns_zc:
        try: _mdns_zc.close()
        except Exception: pass
    _mdns_zc = None
    _mdns_service = None
    _state["mdns_active"] = False


def start(registry_url):
    """Démarre/redémarre le client de registration vers `registry_url` (peut être '' ou None)."""
    global _register_thread, _running
    stop()
    _state["node_id"] = _get_node_id()
    _state["registry_url"] = (registry_url or "").rstrip("/") or None
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
        }

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
        "nmos_mdns_enabled":     {"type": "bool", "default": False},
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
        st["node_description_setting"] = db_get_setting("nmos_node_description", "")
        st["mdns_enabled_setting"]   = bool(db_get_setting("nmos_mdns_enabled", False))
        return jsonify(st)

    @bp.route("/api/nmos/apply", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_apply():
        data     = request.json or {}
        enabled  = bool(data.get("enabled"))
        registry = (data.get("registry_url") or "").strip()
        label    = (data.get("node_label") or "Bobi.Studio").strip()
        desc     = (data.get("node_description") or "").strip()
        mdns     = bool(data.get("mdns_enabled"))
        db_set_setting("nmos_enabled",          enabled)
        db_set_setting("nmos_registry_url",     registry)
        db_set_setting("nmos_node_label",       label)
        db_set_setting("nmos_node_description", desc)
        db_set_setting("nmos_mdns_enabled",     mdns)
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
        for c in conts:
            try:
                _p = (_json.loads(c.get("deploy_config") or "{}").get("params") or {})
            except Exception:
                _p = {}
            explicit.update((_p.get("nmos_bind") or {}).values())
        out = []
        with _lock:
            for row in db_nmos_resources():
                rid = row["id"]
                pool  = _senders if row["kind"] == "sender" else _receivers
                state = _send_state if row["kind"] == "sender" else _recv_state
                live_vmid = (state.get(rid) or {}).get("vmid")
                served = (rid in pool) and (live_vmid is not None)
                sub = ((pool.get(rid) or {}).get("subscription") or {})
                cc = cont_by_iu.get(row.get("bind_instance_uuid"))
                serving = ({"vmid": cc["vmid"], "hostname": cc.get("hostname"),
                            "slot": row.get("bind_slot")} if (cc and served) else None)
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
        rid = db_nmos_resource_create(kind, essence, label,
                                      (d.get("group_name") or label), (d.get("role") or essence), tr)
        notify_state_change()
        return jsonify({"ok": True, "id": rid})

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

    def _resync_after_bind(vmid, params, ess=None, recv_idx=None, rid=None):
        """Resync l'émission/souscription après (dé)liaison. TX : repousse les slots. RX (rid fourni) :
        ré-applique le SDP actif de la ressource au conteneur servant (l'agent re-souscrit)."""
        try:
            from app import docker_driver
            if params.get("tx_slots"):
                docker_driver.push_tx_slots(vmid, params)
        except Exception as e:
            log.warning("resync push_tx_slots %s: %s", vmid, e)
        if rid is not None and recv_idx is not None and ess:
            sdp = (((_recv_state.get(rid) or {}).get("active") or {}).get("transport_file") or {}).get("data")
            if sdp:
                try:
                    manual_subscribe(vmid, recv_idx, {"data": "anc"}.get(ess, ess), sdp, enable=True)
                except Exception as e:
                    log.warning("resync re-subscribe %s/%s: %s", vmid, recv_idx, e)

    @bp.route("/api/nmos/slots", methods=["GET"])
    @require_login
    def nmos_slots():
        return jsonify({"slots": describe_slots()})

    @bp.route("/api/nmos/bind", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_bind_route():
        import json as _json
        from app.database import (db_nmos_resource_get, db_get_container, db_get_containers,
                                  db_update_deploy_config)
        d = request.json or {}
        rid = (d.get("resource_id") or "").strip()
        vmid = d.get("vmid")
        slot_key = (d.get("slot_key") or "").strip()
        if not rid or vmid is None or not slot_key:
            return jsonify({"ok": False, "error": "resource_id, vmid, slot_key requis"}), 400
        res = db_nmos_resource_get(rid)
        if not res:
            return jsonify({"ok": False, "error": "ressource introuvable"}), 404
        ess, kind, recv_idx = _slot_essence_kind(slot_key, res["kind"])
        if kind is None:
            return jsonify({"ok": False, "error": f"slot_key invalide: {slot_key}"}), 400
        if res["kind"] != kind or res["essence"] != ess:
            return jsonify({"ok": False, "error": "essence/kind incompatibles (ressource vs slot)"}), 400
        c = db_get_container(int(vmid))
        if not c:
            return jsonify({"ok": False, "error": "conteneur introuvable"}), 404
        # Exclusivité : une ressource = un seul émetteur. Retirer rid de tout autre conteneur/slot.
        for oc in db_get_containers():
            if oc["vmid"] == int(vmid):
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
                _resync_after_bind(oc["vmid"], op)
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
        return jsonify({"ok": True})

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

    @bp.route("/api/nmos/sriov/status", methods=["GET"])
    @require_login
    def nmos_sriov_status():
        from app import settings as _st
        from app.addressing import primary_host
        from app.template_recreate import list_vfs
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
        from app.template_recreate import ensure_sriov_pool
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
        from app.template_recreate import reconcile_vf_assignments
        pf = _st.get("nmos_2110_pf") or ""
        if not pf:
            return jsonify({"ok": False, "error": "nmos_2110_pf non renseigné"}), 400
        return jsonify(reconcile_vf_assignments(primary_host(), pf))

    @bp.route("/api/nmos/sriov/fix", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_sriov_fix():
        from app import settings as _st
        from app.addressing import primary_host
        from app.template_recreate import fix_vf_assignments
        pf = _st.get("nmos_2110_pf") or ""
        if not pf:
            return jsonify({"ok": False, "error": "nmos_2110_pf non renseigné"}), 400
        return jsonify(fix_vf_assignments(primary_host(), pf))
