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
                         bit_depth=10, colorspace="BT709", transfer="SDR"):
    from app.scripts import CHROMA_DIV, normalize_bit_depth
    cw, ch = CHROMA_DIV.get(str(chroma), CHROMA_DIV["422"])
    half_w   = max(1, width // cw)
    chroma_h = max(1, height // ch)
    bd = normalize_bit_depth(bit_depth)
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
        "interlace_mode": "progressive",
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
    media_types = {"video": ["video/raw"], "audio": ["audio/L24"]}[fmt]
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
    for c in db_get_containers():
        vmid = c["vmid"]
        dc = c.get("deploy_config")
        try:
            dc = json.loads(dc) if isinstance(dc, str) else dc
        except Exception:
            dc = None
        dc_type = dc.get("type") if dc else None
        from app import plugins as _plg
        _manifest   = _plg.REGISTRY.get(dc_type) or {}
        is_receiver = _manifest.get("nmos_role") == "receiver"
        is_sender   = _manifest.get("nmos_role") == "sender"
        dc_params = (dc.get("params") or {}) if dc else {}
        # Counts par essence — lus depuis deploy_config.params (source de vérité unique)
        n_video = int(dc_params.get("video_count", 0) or 0) if is_receiver else 0
        n_audio = int(dc_params.get("audio_count", 0) or 0) if is_receiver else 0
        has_video_send = is_sender and bool(dc_params.get("video"))
        n_audio_send = len(dc_params.get("audios") or []) if is_sender else 0
        smpte_2022_7 = bool(dc_params.get("smpte_2022_7")) if is_receiver else False
        n_legs = 2 if smpte_2022_7 else 1
        # Container exposé en NMOS s'il a au moins un receiver ou sender
        if (n_video + n_audio) <= 0 and not has_video_send and n_audio_send <= 0:
            continue
        did = _stable_uuid(f"device:{vmid}")
        new_devices[did] = _build_device_resource(did, vmid, c.get("hostname"), version)
        base = c.get("hostname") or f"container {vmid}"

        # Nom de bundle d'un ensemble : si plusieurs vidéos sur le container, on
        # suffixe l'index pour distinguer les bundles ; sinon le container = 1 bundle.
        def _grp_name(group_idx):
            return f"{base} {group_idx + 1}" if n_video > 1 else base

        # Receivers vidéo — un bundle (group_name) par vidéo
        for idx in range(n_video):
            rid = _stable_uuid(f"receiver:v:{vmid}:{idx}")
            label = f"{c.get('hostname') or vmid} #r{idx} (video)"
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
            rid = _stable_uuid(f"receiver:a:{vmid}:{idx}")
            label = f"{c.get('hostname') or vmid} #ra{idx} (audio)"
            new_receivers[rid] = _build_receiver_resource(rid, did, vmid, idx, label, version, fmt="audio")
            grp = _grp_name(idx % n_video) if n_video > 0 else base
            n = _audio_role_ctr.get(grp, 0) + 1
            _audio_role_ctr[grp] = n
            _set_grouphint(new_receivers[rid], grp, f"audio {n}")
            new_devices[did]["receivers"].append(rid)
            cur = _recv_state.get(rid)
            if not cur or len(cur["staged"]["transport_params"]) != n_legs:
                _recv_state[rid] = {
                    "staged": _empty_staged(n_legs), "active": _empty_staged(n_legs),
                    "vmid": vmid, "recv_idx": idx, "essence": "audio",
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
            src_id = _stable_uuid(f"source:v:{vmid}")
            fid    = _stable_uuid(f"flow:v:{vmid}")
            snd_id = _stable_uuid(f"sender:v:{vmid}")
            label  = f"{c.get('hostname') or vmid} 2110-20"
            new_sources[src_id] = _build_source_resource(src_id, did, vmid, label, version)
            new_flows[fid]      = _build_flow_resource(fid, did, src_id, vmid, label, width, height,
                                                       version, chroma, bit_depth, cs, transfer)
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

        # Senders audio (0, 1 ou 2)
        for a_idx, a in enumerate((dc.get("params") or {}).get("audios") or []):
            mcast = a.get("multicast_ip") or "239.10.20.1"
            port  = int(a.get("dest_port") or (5004 + 2 * a_idx))
            src_id = _stable_uuid(f"source:a:{vmid}:{a_idx}")
            fid    = _stable_uuid(f"flow:a:{vmid}:{a_idx}")
            snd_id = _stable_uuid(f"sender:a:{vmid}:{a_idx}")
            label  = f"{c.get('hostname') or vmid} 2110-30 #{a_idx}"
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

    with _lock:
        _devices.clear();   _devices.update(new_devices)
        _receivers.clear(); _receivers.update(new_receivers)
        _senders.clear();   _senders.update(new_senders)
        _sources.clear();   _sources.update(new_sources)
        _flows.clear();     _flows.update(new_flows)
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
    from app.proxmox import get_container_ip
    ip = get_container_ip(vmid)
    if not ip:
        return ("container IP introuvable", 503)
    try:
        r = requests.get(f"http://{ip}:8080", timeout=2)
        if r.status_code != 200:
            return (f"metrics HTTP {r.status_code}", 503)
        sdp = (r.json() or {}).get("sdp") or ""
    except Exception as e:
        return (f"metrics fetch failed: {e}", 503)
    if not sdp:
        return ("sdp non encore disponible (ffmpeg pas démarré ?)", 404)

    # Injection PTP : insère a=ts-refclk + a=mediaclk juste après la dernière
    # ligne d'attribut media-level (juste avant la fin du SDP). ffmpeg ne les
    # ajoute pas, mais l'interop 2110 broadcast les attend.
    try:
        from app import settings as st, ptp as _ptp
        if st.get("ptp_enabled"):
            refclk = _ptp.sdp_refclk_lines(st.get("proxmox_host"))
            if refclk and "ts-refclk" not in sdp:
                # Append à la fin (les attributs media-level peuvent être en queue)
                if not sdp.endswith("\n"):
                    sdp += "\r\n"
                sdp += refclk
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

def _build_leg1_sdp(leg0_sdp, leg1_tp):
    """Construit un SDP leg1 depuis leg0 en remplaçant adresse multicast et port."""
    if not leg0_sdp or not leg1_tp:
        return ""
    sdp = leg0_sdp
    mcast1 = leg1_tp.get("multicast_ip")
    port1  = leg1_tp.get("destination_port")
    if mcast1:
        sdp = re.sub(r"(c=IN IP4 )[\d.]+", rf"\g<1>{mcast1}", sdp)
    if port1:
        sdp = re.sub(r"(m=(?:video|audio) )\d+", rf"\g<1>{port1}", sdp)
    return sdp

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
    from app.proxmox import get_container_ip
    from app.database import db_add_alert
    ip = get_container_ip(vmid)
    if not ip:
        log.warning(f"nmos: pas d'IP pour container {vmid}, subscription ignorée")
        db_add_alert(f"NMOS subscription receiver #{recv_idx}/{essence} container {vmid}: IP introuvable", "warning")
        return
    if isinstance(mcast_info, list):
        # SMPTE 2022-7 : two legs
        info0, info1 = mcast_info[0], mcast_info[1]
        sdp1 = _build_leg1_sdp(sdp, info1) if sdp else ""
        payload = {
            "receiver_index": recv_idx,
            "essence": essence,
            "enabled": enable,
            "sdp": [sdp or "", sdp1],
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

    @bp.route("/api/nmos/sriov/status", methods=["GET"])
    @require_login
    def nmos_sriov_status():
        from app import settings as _st
        from app.template_recreate import list_vfs
        from app.database import db_get_assigned_vfs
        pf       = _st.get("nmos_2110_pf") or ""
        enabled  = bool(_st.get("nmos_2110_enabled"))
        vf_count = int(_st.get("nmos_2110_vf_count") or 0)
        vfs = []
        err = None
        if enabled and pf:
            ok, lst, msg = list_vfs(_st.get("proxmox_host"), pf)
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
        from app.template_recreate import ensure_sriov_pool
        pf = _st.get("nmos_2110_pf") or ""
        n  = int(_st.get("nmos_2110_vf_count") or 0)
        if not pf:
            return jsonify({"ok": False, "error": "nmos_2110_pf non renseigné"}), 400
        ok, msg = ensure_sriov_pool(_st.get("proxmox_host"), pf, n)
        return jsonify({"ok": ok, "msg": msg})

    @bp.route("/api/nmos/sriov/reconcile", methods=["GET"])
    @require_login
    def nmos_sriov_reconcile():
        from app import settings as _st
        from app.template_recreate import reconcile_vf_assignments
        pf = _st.get("nmos_2110_pf") or ""
        if not pf:
            return jsonify({"ok": False, "error": "nmos_2110_pf non renseigné"}), 400
        return jsonify(reconcile_vf_assignments(_st.get("proxmox_host"), pf))

    @bp.route("/api/nmos/sriov/fix", methods=["POST"])
    @require_perm("settings.edit")
    def nmos_sriov_fix():
        from app import settings as _st
        from app.template_recreate import fix_vf_assignments
        pf = _st.get("nmos_2110_pf") or ""
        if not pf:
            return jsonify({"ok": False, "error": "nmos_2110_pf non renseigné"}), 400
        return jsonify(fix_vf_assignments(_st.get("proxmox_host"), pf))
