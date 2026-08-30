"""BCP-007-03 « NMOS With MXL » — exposition du bus MXL interne en IS-04 / IS-05.

Jusqu'ici, tout notre graphe interne (mixer, multiview, streamer, player, recorder, ports shm du
moteur 2110) n'était routable que par NOTRE page Câbles. Ce module l'expose en ressources NMOS
standard, transport ``urn:x-nmos:transport:mxl`` : un contrôleur du marché patche nos flux MXL
comme il patche déjà nos flux 2110.

Correspondance retenue — un port de plugin = une ressource NMOS :

    wiring `produces[]`  → Source + Flow + **Sender** (ce conteneur ÉCRIT ce flux MXL)
    wiring `consumes[]`  → **Receiver**                (ce conteneur LIT un flux MXL)

et un PATCH IS-05 sur un Receiver revient à poser un câble : il délègue à ``_apply_wire``, le
même chemin que la page Câbles. Il n'y a pas deux vérités de routage.

═══ Ce que la BCP impose, et qu'on ne peut pas improviser ═════════════════════════════════════

Relevé sur `AMWA-TV/bcp-007-03` v1.0.0 (publiée le 2026-08-18) le 2026-08-30 :

- Sender et Receiver ``transport`` = ``urn:x-nmos:transport:mxl`` — la BCP impose le LITTÉRAL,
  elle ne délègue pas au registre des transports (contrairement à ``format`` et ``media_type``).
  ⚠ Ce transport n'est TOUJOURS PAS au registre AMWA (vérifié le 2026-08-30, cf. TODO.md
  § BCP-007-03) : un contrôleur strict peut refuser la ressource. Risque assumé.
- ``interface_bindings`` VIDE : MXL est de la mémoire partagée, il ne traverse aucune NIC.
- ``manifest_href: null`` et ``/transportfile`` → 404 : MXL n'a pas de fichier de transport.
- ``transport_params`` = ``{mxl_domain_id, mxl_flow_id}``, avec une ASYMÉTRIE qu'il faut
  respecter à la lettre (schémas ``sender_transport_params_mxl.json`` /
  ``receiver_transport_params_mxl.json``) :

      | paramètre                | null | "auto" | UUID |
      | sender.mxl_domain_id     |  ✔   |   ✔    |  ✔   |
      | sender.mxl_flow_id       |  ✔   |   ✔    |  ✔   |
      | receiver.mxl_domain_id   |  ✔   |   ✔    |  ✔   |
      | receiver.mxl_flow_id     |  ✔   |   ✘    |  ✔   |   ← "auto" INTERDIT ici

  et « The constraints endpoint does not list `auto` as an available option » : ``auto`` est
  accepté en /staged mais ne s'énumère JAMAIS dans /constraints.

═══ Identité des ressources ══════════════════════════════════════════════════════════════════

Les ressources MXL n'entrent PAS dans le registre `nmos_resources` (`_registry_id`) : ce registre
et ses `bind_slot` sont la machinerie des slots du moteur 2110 (rebinding explicite, ressources
orphelines, grouping figé). Ici l'identité est DÉRIVÉE, donc reproductible sans état :

    id = uuid5("mxl:<kind>:<instance_uuid>:<essence>:<slot>")

keyée sur ``instance_uuid`` (barreau 2, survit au recreate/restore, cf. CLAUDE.md « Identité d'un
conteneur »), pas sur le vmid ni sur le nom du flux. Un conteneur RENOMMÉ change ses noms de flux
MXL — donc ses ``mxl_flow_id``, ce qui est normal, c'est un paramètre de transport — sans changer
ses identifiants NMOS. Un conteneur REMPLACÉ, lui, donne d'autres ressources : c'est le barreau 3
(emplacements) qui répond à ce besoin-là, pas celui-ci.
"""

import json
import logging
import uuid

log = logging.getLogger(__name__)

TRANSPORT = "urn:x-nmos:transport:mxl"

# ⚠ MIROIR de `script_templates/bobimxl.py:_NS_BOBI` / `flow_id()`. L'UUID d'un flux MXL est
# dérivé de son NOM par uuid5 : ces deux lignes décident de l'identité des flux côté contrôleur,
# et doivent rester identiques à celles du binding. Ne pas « simplifier ».
_NS_BOBI = uuid.uuid5(uuid.NAMESPACE_DNS, "mxl.bobi.studio")


def flow_uuid(name):
    """UUID MXL d'un flux, à partir de son nom maison (= `flow_def["id"]` écrit par le writer)."""
    return str(uuid.uuid5(_NS_BOBI, str(name)))


def _rid(kind, instance_uuid, essence, slot):
    """Identifiant NMOS déterministe d'une ressource MXL (cf. docstring du module)."""
    from . import _stable_uuid
    return _stable_uuid("mxl:%s:%s:%s:%s" % (kind, instance_uuid, essence, slot))


# ── Types de média ────────────────────────────────────────────────────────────────────────────
# BCP-007-03 : « An MXL Flow resource MUST set the `media_type` attribute to a value specified in
# the NMOS media types parameter register ». Notre audio et notre ANC sont conformes D'ORIGINE ;
# la vidéo ne l'est pas, et c'est un arbitrage PRODUIT déjà tranché le 2026-08-15 (planar en
# interne, miroir v210 au cas par cas via `plugins/v210_bridge` quand un tiers doit lire un flux
# précis — cf. docs/reference/MXL_INTEROP.md). On publie donc le type RÉEL du flux : annoncer
# `video/v210` sur un flux planar serait une non-conformité bien pire qu'un type hors registre.
_MEDIA_TYPE = {
    "video": "video/x-mxl-planar",   # ⚠ hors registre — assumé, cf. ci-dessus
    "audio": "audio/float32",        # enregistré
    "anc":   "video/smpte291",       # enregistré
    "data":  "video/smpte291",
}

_FORMAT = {
    "video": "urn:x-nmos:format:video",
    "audio": "urn:x-nmos:format:audio",
    "anc":   "urn:x-nmos:format:data",
    "data":  "urn:x-nmos:format:data",
}


def _essence(port):
    return (port.get("essence") or "video").strip().lower()


def _grain_rate(fmt):
    """`{numerator, denominator}` IS-04 depuis le descripteur de format d'un port, ou None.
    Cadence fractionnaire (29.97/59.94) : `fps_num`/`fps_den` font foi quand ils sont là."""
    if not isinstance(fmt, dict):
        return None
    num, den = fmt.get("fps_num"), fmt.get("fps_den")
    if num and den:
        return {"numerator": int(num), "denominator": int(den)}
    fps = fmt.get("fps")
    if not fps:
        return None
    try:
        f = float(fps)
    except (TypeError, ValueError):
        return None
    if abs(f - round(f)) < 1e-6:
        return {"numerator": int(round(f)), "denominator": 1}
    # 29.97 / 59.94 saisis en décimal → forme rationnelle exacte 30000/1001.
    return {"numerator": int(round(f * 1001)), "denominator": 1001}


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Inventaire : les ports MXL d'un conteneur
# ══════════════════════════════════════════════════════════════════════════════════════════════

def ports_of(container):
    """Ports MXL d'un conteneur → `{"produces": [...], "consumes": [...]}`, chaque entrée
    enrichie de `slot` (index de port, 0 par défaut) et `key` (clé de slot stable).

    Source de vérité : `plugins.derive_wiring`, exactement comme la page Câbles — pas une
    seconde dérivation maison qui divergerait au premier plugin ajouté. Un conteneur sans
    wiring (ou dont le plugin a disparu du registre) donne deux listes vides."""
    from app import plugins as _plg
    dc = container.get("deploy_config")
    try:
        dc = json.loads(dc) if isinstance(dc, str) else dc
    except Exception:
        dc = None
    if not dc:
        return {"produces": [], "consumes": []}
    t = dc.get("type")
    if not t or not _plg.is_plugin(t):
        return {"produces": [], "consumes": []}
    params = dc.get("params") or {}
    try:
        w = _plg.derive_wiring(t, container.get("hostname") or "", params) or {}
    except Exception as e:
        log.warning("nmos/mxl: wiring illisible pour vmid %s (%s) : %s", container.get("vmid"), t, e)
        return {"produces": [], "consumes": []}

    def _norm(entries):
        out = []
        for i, p in enumerate(entries or []):
            if not isinstance(p, dict):
                continue
            c = dict(p)
            slot = c.get("slot")
            c["slot"] = int(slot) if isinstance(slot, int) else i
            c["key"] = "%s:%d" % (_essence(c), c["slot"])
            out.append(c)
        return out

    return {"produces": _norm(w.get("produces")), "consumes": _norm(w.get("consumes"))}


def wire_of(port, params):
    """Nom du flux MXL réellement câblé sur un port d'ENTRÉE, ou "" si le port est libre.

    ⚠ NE PAS lire `port["shm"]` : pour un port d'entrée, `derive_wiring` ne le remplit QUE
    pour la famille « liste » (`from_list`/`shm_field` — multiview). La famille dominante
    déclare un `state_field`, et le câble vit alors dans `params[state_field]`.
    Mesuré le 2026-08-30 sur le parc : `delay.input_v_1 = "2110-io-dl360-1_5"`,
    `2110_io.tx1_shm`, `pyramide.input_1` — 192 des 192 entrées du moteur 2110 sont de
    cette famille, et AUCUNE ne porte `shm`. Lire `shm` faisait voir zéro câble sur un
    parc qui en a : un Receiver éternellement `null`, sans erreur nulle part."""
    champ = port.get("state_field")
    if champ:
        return str(params.get(champ) or "").strip()
    return str(port.get("shm") or "").strip()          # famille « liste » : déjà résolu


def _domain_id(node_id):
    """UUID du domaine MXL du nœud (table `nodes`), ou None. C'est l'identité INDÉPENDANTE DU
    CHEMIN DE MONTAGE qu'exige la BCP — surtout pas le chemin `/dev/shm/mxl`."""
    if not node_id:
        return None
    try:
        from app.database import db_node_mxl_domain_id
        return db_node_mxl_domain_id(node_id)
    except Exception as e:
        log.warning("nmos/mxl: domaine MXL du nœud %s illisible : %s", node_id, e)
        return None


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Ressources IS-04
# ══════════════════════════════════════════════════════════════════════════════════════════════

def _tags(vmid, hostname):
    return {"urn:x-mxl:vmid": [str(vmid)], "urn:x-mxl:hostname": [str(hostname or "")]}


def _build_source(sid, did, vmid, hostname, label, essence, fmt, version):
    r = {
        "id": sid,
        "version": version,
        "label": label,
        "description": "Source MXL %s — conteneur %s" % (essence, vmid),
        "tags": _tags(vmid, hostname),
        "device_id": did,
        "parents": [],
        "format": _FORMAT.get(essence, _FORMAT["data"]),
        "caps": {},
        "clock_name": "clk0",
    }
    gr = _grain_rate(fmt)
    if gr:
        r["grain_rate"] = gr
    if essence == "audio":
        # IS-04 : une Source audio DOIT énumérer ses canaux. Le descripteur de port donne leur
        # nombre ; les symboles restent non déclarés (`NULL`) faute d'un mapping fiable côté bus.
        n = int((fmt or {}).get("channels") or 0) if isinstance(fmt, dict) else 0
        r["channels"] = [{"label": "Channel %d" % (i + 1)} for i in range(max(n, 1))]
    return r


def _build_flow(fid, did, src_id, vmid, hostname, label, essence, fmt, version):
    r = {
        "id": fid,
        "version": version,
        "label": label,
        "description": "Flow MXL %s — conteneur %s" % (essence, vmid),
        "tags": _tags(vmid, hostname),
        "device_id": did,
        "source_id": src_id,
        "parents": [],
        "format": _FORMAT.get(essence, _FORMAT["data"]),
        "media_type": _MEDIA_TYPE.get(essence, _MEDIA_TYPE["data"]),
    }
    gr = _grain_rate(fmt)
    if gr:
        r["grain_rate"] = gr
    f = fmt if isinstance(fmt, dict) else {}
    if essence == "video":
        from app.scripts import CHROMA_DIV, normalize_bit_depth, nmos_interlace_mode
        w = int(f.get("width") or 0)
        h = int(f.get("height") or 0)
        if w and h:
            cw, ch = CHROMA_DIV.get(str(f.get("chroma") or "422"), CHROMA_DIV["422"])
            bd = normalize_bit_depth(f.get("bit_depth"))
            r["frame_width"] = w
            r["frame_height"] = h
            r["interlace_mode"] = nmos_interlace_mode(
                {"scan": f.get("scan") or "p", "field_order": f.get("field_order") or "", "height": h})
            r["colorspace"] = (f.get("colorimetry") or "bt709").upper().replace("BT", "BT")
            r["components"] = [
                {"name": "Y",  "width": w,                    "height": h,            "bit_depth": bd},
                {"name": "Cb", "width": max(1, w // cw),      "height": max(1, h // ch), "bit_depth": bd},
                {"name": "Cr", "width": max(1, w // cw),      "height": max(1, h // ch), "bit_depth": bd},
            ]
    elif essence == "audio":
        r["sample_rate"] = {"numerator": int(f.get("sample_rate") or 48000), "denominator": 1}
        r["bit_depth"] = int(f.get("bit_depth") or 32)
    return r


def _build_sender(snd_id, did, fid, vmid, hostname, label, version):
    return {
        "id": snd_id,
        "version": version,
        "label": label,
        "description": "Sender MXL — conteneur %s" % vmid,
        "tags": _tags(vmid, hostname),
        "device_id": did,
        "flow_id": fid,
        "transport": TRANSPORT,
        # BCP-007-03 : MXL n'a pas de fichier de transport. `manifest_href` null ET
        # `/transportfile` → 404 (cf. `is05_send_transportfile` / `is05_recv_transportfile`).
        "manifest_href": None,
        # BCP-007-03 : « An MXL Sender resource MUST expose an empty interface_bindings array. »
        # Le bus est de la mémoire partagée : il ne sort par aucune interface réseau.
        "interface_bindings": [],
        "subscription": {"receiver_id": None, "active": False},
        "caps": {},
    }


def _build_receiver(rid, did, vmid, hostname, label, essence, version):
    from . import _receiver_caps
    fmt = "video" if essence == "video" else ("audio" if essence == "audio" else "data")
    caps = _receiver_caps(fmt, [_MEDIA_TYPE.get(essence, _MEDIA_TYPE["data"])])
    return {
        "id": rid,
        "version": version,
        "label": label,
        "description": "Receiver MXL — conteneur %s" % vmid,
        "tags": _tags(vmid, hostname),
        "device_id": did,
        "transport": TRANSPORT,
        "interface_bindings": [],
        "subscription": {"sender_id": None, "active": False},
        "format": _FORMAT.get(essence, _FORMAT["data"]),
        "caps": caps,
    }


# ══════════════════════════════════════════════════════════════════════════════════════════════
# État IS-05
# ══════════════════════════════════════════════════════════════════════════════════════════════

def empty_staged(kind, domain_id=None, flow_id=None):
    """`staged`/`active` IS-05 d'une ressource MXL. `kind` = "sender" | "receiver".

    Pas de `transport_file` ici, contrairement au RTP : MXL n'en a pas. Les consommateurs de ces
    états (re-synchro d'abonnement dans `rebuild_model`, IS-12) ne lisent que `master_enable` et
    `sender_id`/`receiver_id`, donc l'absence de la clé est sans effet de bord."""
    st = {
        "master_enable": False,
        "transport_params": [{"mxl_domain_id": domain_id, "mxl_flow_id": flow_id}],
        "activation": {"mode": None, "requested_time": None, "activation_time": None},
    }
    st["receiver_id" if kind == "sender" else "sender_id"] = None
    return st


def _clone(d):
    return json.loads(json.dumps(d))


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Construction du modèle (appelée par `rebuild_model`)
# ══════════════════════════════════════════════════════════════════════════════════════════════

def build(new_devices, new_sources, new_flows, new_senders, new_receivers,
          recv_state, send_state, cluster_did, version):
    """Ajoute les ressources MXL au modèle en cours de reconstruction.

    Appelée depuis `rebuild_model` APRÈS la passe conteneurs et la passe registre, AVANT le
    commit sous verrou. Best-effort : un conteneur illisible est sauté, jamais fatal — le
    provider IS-04/05 du 2110 ne doit pas tomber parce qu'un plugin a un wiring douteux.

    Renvoie l'ensemble des ids de ressources MXL créées (senders + receivers), pour que
    l'appelant sache lesquelles ne relèvent PAS de la machinerie RTP."""
    from app.database import db_get_containers
    ids = set()
    if _setting_off():
        return ids
    for c in db_get_containers():
        try:
            _build_one(c, new_devices, new_sources, new_flows, new_senders, new_receivers,
                       recv_state, send_state, cluster_did, version, ids)
        except Exception as e:
            log.warning("nmos/mxl: conteneur %s ignoré : %s", c.get("vmid"), e)
    return ids


def _setting_off():
    """Réglage `nmos_mxl` (défaut : activé). Un site qui ne veut pas exposer son bus interne aux
    contrôleurs tiers coupe la surface d'un cran, sans toucher au 2110."""
    from . import _setting
    return str(_setting("nmos_mxl", "1")).strip() in ("0", "false", "off", "no")


def _build_one(c, new_devices, new_sources, new_flows, new_senders, new_receivers,
               recv_state, send_state, cluster_did, version, ids):
    vmid = c["vmid"]
    iu = c.get("instance_uuid") or ("vmid:%s" % vmid)
    hostname = c.get("hostname") or ""
    ports = ports_of(c)
    if not ports["produces"] and not ports["consumes"]:
        return
    _dc = c.get("deploy_config")
    try:
        _dc = json.loads(_dc) if isinstance(_dc, str) else _dc
    except Exception:
        _dc = None
    params = (_dc or {}).get("params") or {}
    domain_id = _domain_id(c.get("node_id"))
    label_base = hostname or ("conteneur %s" % vmid)

    for p in ports["produces"]:
        shm = (p.get("shm") or "").strip()
        if not shm:
            continue                      # port de sortie non nommé : rien à annoncer
        ess = _essence(p)
        slot = p["slot"]
        fmt = p.get("format")
        lbl = p.get("label") or ("%s %s" % (label_base, ess))
        sid = _rid("source", iu, ess, slot)
        fid = _rid("flow",   iu, ess, slot)
        snd = _rid("sender", iu, ess, slot)
        new_sources[sid] = _build_source(sid, cluster_did, vmid, hostname, lbl, ess, fmt, version)
        new_flows[fid] = _build_flow(fid, cluster_did, sid, vmid, hostname, lbl, ess, fmt, version)
        new_senders[snd] = _build_sender(snd, cluster_did, fid, vmid, hostname, lbl, version)
        new_devices[cluster_did]["senders"].append(snd)
        ids.add(snd)
        # Un Sender MXL est ACTIF par construction : le conteneur écrit ce flux tant qu'il tourne.
        # Il n'y a rien à « activer » — d'où master_enable=True et des transport_params RÉELS.
        st = empty_staged("sender", domain_id, flow_uuid(shm))
        st["master_enable"] = True
        prev = send_state.get(snd)
        send_state[snd] = {
            "staged": st, "active": _clone(st), "vmid": vmid, "essence": ess,
            "mxl": True, "shm": shm, "slot": slot,
            # `receiver_id` posé par un contrôleur au patch : c'est une information de ROUTAGE
            # qui lui appartient, on ne l'écrase pas à chaque rebuild.
            **({"receiver_id": prev.get("receiver_id")} if prev else {}),
        }
        if prev and (prev.get("staged") or {}).get("receiver_id"):
            send_state[snd]["staged"]["receiver_id"] = prev["staged"]["receiver_id"]
            send_state[snd]["active"]["receiver_id"] = (prev.get("active") or {}).get("receiver_id")

    for p in ports["consumes"]:
        ess = _essence(p)
        slot = p["slot"]
        rid = _rid("receiver", iu, ess, slot)
        lbl = p.get("label") or ("%s entrée %s %d" % (label_base, ess, slot + 1))
        new_receivers[rid] = _build_receiver(rid, cluster_did, vmid, hostname, lbl, ess, version)
        new_devices[cluster_did]["receivers"].append(rid)
        ids.add(rid)
        # L'état IS-05 d'un Receiver MXL est un CONSTAT du câble réellement posé (params du
        # conteneur), pas une mémoire. `_apply_wire` est le seul à écrire le câblage, et il peut
        # être déclenché hors NMOS (page Câbles, restauration de projet, insertion d'UDC) : une
        # mémoire NMOS divergerait en silence. On relit donc à chaque rebuild.
        shm = wire_of(p, params)
        fuid = flow_uuid(shm) if shm else None
        st = empty_staged("receiver", domain_id, fuid)
        st["master_enable"] = bool(shm)
        st["sender_id"] = _sender_id_for_shm(shm) if shm else None
        recv_state[rid] = {
            "staged": st, "active": _clone(st), "vmid": vmid, "recv_idx": slot,
            "essence": ess, "mxl": True, "shm": shm, "slot": slot,
            "port": {k: p.get(k) for k in ("input_key", "from_list", "shm_field", "label")},
        }


# Index nom de flux → sender_id, reconstruit à chaque rebuild (petit, et évite un balayage
# quadratique des conteneurs à chaque receiver).
_shm_to_sender = {}


def _sender_id_for_shm(shm):
    return _shm_to_sender.get(shm)


def reindex(send_state):
    """Reconstruit l'index nom de flux → sender_id. Appelé par `rebuild_model` après `build`,
    puis les `sender_id` des receivers sont recalés (l'ordre des conteneurs ne garantit pas que
    le producteur ait été vu avant le consommateur)."""
    _shm_to_sender.clear()
    for sid, st in send_state.items():
        if st.get("mxl") and st.get("shm"):
            _shm_to_sender[st["shm"]] = sid


def resync_subscriptions(receivers, recv_state, senders, send_state):
    """Recale `subscription.sender_id` des Receivers MXL et `subscription` des Senders, une fois
    l'index construit. Sans ça, un consommateur déclaré AVANT son producteur dans la liste des
    conteneurs annoncerait un abonnement sans sender_id."""
    abonnes = {}
    for rid, st in recv_state.items():
        if not st.get("mxl"):
            continue
        sid = _sender_id_for_shm(st.get("shm")) if st.get("shm") else None
        for cle in ("staged", "active"):
            if cle in st:
                st[cle]["sender_id"] = sid
        if rid in receivers:
            receivers[rid]["subscription"] = {"sender_id": sid, "active": bool(st.get("shm"))}
        if sid:
            abonnes.setdefault(sid, []).append(rid)
    for sid, st in send_state.items():
        if not st.get("mxl") or sid not in senders:
            continue
        rids = abonnes.get(sid) or []
        # IS-04 ne prévoit qu'UN receiver_id par sender ; un flux MXL peut avoir N lecteurs
        # (c'est tout l'intérêt d'un bus). On annonce `active` dès qu'il y en a au moins un, et
        # `receiver_id` seulement s'il est unique — sinon null, qui est la valeur honnête.
        senders[sid]["subscription"] = {"receiver_id": rids[0] if len(rids) == 1 else None,
                                        "active": bool(rids)}


# ══════════════════════════════════════════════════════════════════════════════════════════════
# IS-05 : /constraints
# ══════════════════════════════════════════════════════════════════════════════════════════════

def _domain_enum():
    """Domaines MXL du cluster (un par nœud enrôlé qui en a un)."""
    out = []
    try:
        from app.database import db_get_nodes, db_node_mxl_domain_id
        for n in db_get_nodes():
            d = db_node_mxl_domain_id(n["id"])
            if d and d not in out:
                out.append(d)
    except Exception as e:
        log.warning("nmos/mxl: énumération des domaines impossible : %s", e)
    return out


def constraints(res_id, send_state, recv_state):
    """`/constraints` IS-05 d'une ressource MXL, ou None si `res_id` n'en est pas une.

    ⚠ `auto` n'y figure JAMAIS, même là où il est accepté en /staged : la BCP l'exclut
    explicitement de l'énumération (« The constraints endpoint does not list `auto` »)."""
    st = send_state.get(res_id) or recv_state.get(res_id)
    if not st or not st.get("mxl"):
        return None
    doms = _domain_enum()
    c = {"mxl_domain_id": {}, "mxl_flow_id": {}}
    if doms:
        c["mxl_domain_id"]["enum"] = doms
    if res_id in send_state:
        # Un Sender écrit LE flux de son port : rien d'autre n'est atteignable.
        fid = (st["active"]["transport_params"][0] or {}).get("mxl_flow_id")
        if fid:
            c["mxl_flow_id"]["enum"] = [fid]
    else:
        # Un Receiver peut lire n'importe quel flux MXL annoncé par le cluster, à essence égale.
        ess = st.get("essence") or "video"
        c["mxl_flow_id"]["enum"] = [
            (s["active"]["transport_params"][0] or {}).get("mxl_flow_id")
            for s in send_state.values()
            if s.get("mxl") and (s.get("essence") or "video") == ess
            and (s["active"]["transport_params"][0] or {}).get("mxl_flow_id")
        ]
    return [c]


# ══════════════════════════════════════════════════════════════════════════════════════════════
# IS-05 : PATCH /staged — poser (ou retirer) un câble
# ══════════════════════════════════════════════════════════════════════════════════════════════

_AUTO = "auto"


def _valid_uuid(v):
    try:
        uuid.UUID(str(v))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def apply_receiver_staged(rid, body, recv_state, send_state):
    """PATCH IS-05 sur un Receiver MXL → pose ou retire un câble via `_apply_wire`.

    Renvoie `(ok, code_http, payload)`. Le payload d'erreur suit le format IS-05
    (`{"code", "error", "debug"}`), celui de succès est l'état staged résultant.

    Trois refus explicites, tous justifiés par le schéma de la BCP :
      - `mxl_flow_id: "auto"` → 400. Le schéma Receiver n'accepte que null ou un UUID ;
        « The literal auto is not used for this parameter. »
      - flux inconnu du cluster → 400 plutôt qu'un câble posé vers le vide.
      - essence discordante (patcher de l'audio sur une entrée vidéo) → 400."""
    st = recv_state.get(rid)
    if not st or not st.get("mxl"):
        return False, 404, {"code": 404, "error": "receiver MXL inconnu", "debug": rid}

    staged = _clone(st["staged"])
    tps = body.get("transport_params")
    if isinstance(tps, list) and tps:
        tp = tps[0] or {}
        if "mxl_domain_id" in tp:
            v = tp["mxl_domain_id"]
            if v is not None and v != _AUTO and not _valid_uuid(v):
                return False, 400, {"code": 400, "error": "mxl_domain_id invalide", "debug": str(v)}
            staged["transport_params"][0]["mxl_domain_id"] = v
        if "mxl_flow_id" in tp:
            v = tp["mxl_flow_id"]
            if v == _AUTO:
                return False, 400, {"code": 400,
                                    "error": "mxl_flow_id n'accepte pas « auto » sur un Receiver "
                                             "(BCP-007-03)", "debug": rid}
            if v is not None and not _valid_uuid(v):
                return False, 400, {"code": 400, "error": "mxl_flow_id invalide", "debug": str(v)}
            staged["transport_params"][0]["mxl_flow_id"] = v
    if "master_enable" in body:
        staged["master_enable"] = bool(body["master_enable"])
    if "sender_id" in body:
        staged["sender_id"] = body["sender_id"]
    if "activation" in body and isinstance(body["activation"], dict):
        staged["activation"].update(body["activation"])

    st["staged"] = staged
    mode = (staged.get("activation") or {}).get("mode")
    if not mode:
        return True, 200, staged          # staging seul : rien n'est appliqué
    if mode != "activate_immediate":
        # `activate_scheduled_*` demanderait un ordonnanceur ; le dire franchement vaut mieux que
        # de l'accepter et de ne rien faire à l'heure dite.
        return False, 501, {"code": 501, "error": "seul activate_immediate est géré", "debug": mode}

    want = (staged["transport_params"][0] or {}).get("mxl_flow_id")
    enable = bool(staged.get("master_enable")) and bool(want)
    ok, code, payload = _appliquer(st, want if enable else None, send_state)
    if not ok:
        return False, code, payload
    st["staged"]["activation"] = {"mode": mode, "requested_time": None,
                                  "activation_time": _now()}
    st["active"] = _clone(st["staged"])
    # ── /active doit porter le CONSTAT, pas l'intention ────────────────────────────────────
    # Deux back-fills, tous deux constatés au banc le 2026-08-31 sur un patch réel :
    #
    # 1. `auto` ne doit PAS survivre dans /active. C'est une INSTRUCTION de résolution, valable
    #    en /staged ; la BCP raisonne en « resolved to a valid value for /active ». Le laisser
    #    tel quel rendrait /active illisible pour un contrôleur qui compare ce qu'il a demandé
    #    à ce qui s'applique. Même règle que le back-fill RTP depuis le transport_file.
    # 2. `sender_id` était laissé à null alors que le flux EST résolu → un abonnement actif sans
    #    sender annoncé, que le contrôleur affiche comme « connecté à rien ».
    _tp = st["active"]["transport_params"][0]
    if _tp.get("mxl_domain_id") == _AUTO or _tp.get("mxl_domain_id") is None:
        _tp["mxl_domain_id"] = _domaine_du_receiver(st)
    st["active"]["sender_id"] = _sender_id_for_shm(st.get("shm")) if st.get("shm") else None
    st["staged"]["activation"] = {"mode": None, "requested_time": None, "activation_time": None}
    return True, 200, st["active"]


def _domaine_du_receiver(st):
    """Domaine MXL RÉEL du conteneur qui sert ce Receiver — ce que `auto` doit devenir dans
    /active. None si le conteneur n'est plus là (le patch a pu être joué sur un orphelin)."""
    try:
        from app.database import db_get_container
        c = db_get_container(st.get("vmid")) or {}
        return _domain_id(c.get("node_id"))
    except Exception as e:
        log.warning("nmos/mxl: domaine du receiver irrésolu : %s", e)
        return None


def _now():
    from . import _tai_version
    return _tai_version()


def _appliquer(st, flow_id_voulu, send_state):
    """Traduit l'intention IS-05 en geste de câblage. `flow_id_voulu=None` → décâblage."""
    to_vmid = st.get("vmid")
    ess = st.get("essence") or "video"
    kind = "data" if ess in ("anc", "data") else ess
    if not to_vmid:
        return False, 500, {"code": 500, "error": "receiver sans conteneur servant", "debug": ""}

    from app.routes.cabling import _apply_wire, _apply_unwire
    from app.vmlocks import verrou_vmid

    if flow_id_voulu is None:
        if not st.get("shm"):
            return True, 200, {}                     # déjà décâblé
        with verrou_vmid(to_vmid, op="nmos-mxl-unwire"):
            ok, _s, _p = _apply_unwire(to_vmid, st["shm"], kind)
        if not ok:
            return False, 500, {"code": 500, "error": "décâblage refusé", "debug": str(_p)}
        st["shm"] = ""
        return True, 200, {}

    # Résolution flux → conteneur producteur. On passe par `send_state` (le modèle qu'on vient
    # de publier) et non par un re-balayage de la DB : ce que le contrôleur a vu fait foi.
    src = None
    for sid, s in send_state.items():
        if not s.get("mxl"):
            continue
        if (s["active"]["transport_params"][0] or {}).get("mxl_flow_id") == flow_id_voulu:
            src = s
            break
    if not src:
        return False, 400, {"code": 400, "error": "mxl_flow_id inconnu du cluster",
                            "debug": str(flow_id_voulu)}
    if (src.get("essence") or "video") != ess:
        return False, 400, {"code": 400,
                            "error": "essence discordante : flux %s sur une entrée %s"
                                     % (src.get("essence"), ess), "debug": str(flow_id_voulu)}

    # ⚠ Garde de FORMAT sur les slots TX du moteur 2110 (docs/reference/TX_LAYOUTS.md, étage 3) :
    # câbler une source dont le format s'écarte du slot provisionné recrée la session, donc
    # `rte_tm_hierarchy_commit` — soit le GEL de toutes les sorties de la carte. La page Câbles
    # refuse ce cas sans porte de sortie ; un PATCH IS-05 doit le refuser AUSSI, sinon la surface
    # NMOS devient un chemin de contournement d'une garde de production (cf. l'anti-patron « un
    # garde conditionné à QUI APPELLE ne protège que celui-là »).
    from app.routes.cabling import _tx_slot_mismatch
    try:
        mm = _tx_slot_mismatch(src.get("vmid"), src.get("shm"), to_vmid, st.get("slot"), kind)
    except Exception as e:
        log.warning("nmos/mxl: garde format TX indisponible (%s) — câblage refusé par prudence", e)
        return False, 500, {"code": 500, "error": "garde de format indisponible", "debug": str(e)}
    if mm:
        axes = ", ".join("%s %s ≠ %s" % (a["axis"], a["source"], a["slot"]) for a in mm["axes"])
        return False, 400, {"code": 400, "error": "écart de format : %s" % axes,
                            "debug": json.dumps(mm.get("axes") or [])}

    with verrou_vmid(to_vmid, op="nmos-mxl-wire"):
        ok, _s, _p = _apply_wire(src.get("vmid"), to_vmid, src.get("shm"), kind,
                                 st.get("slot") if _slotte(st) else None)
    if not ok:
        return False, 500, {"code": 500, "error": "câblage refusé", "debug": str(_p)}
    st["shm"] = src.get("shm")
    return True, 200, {}


def _slotte(st):
    """Le port du consommateur est-il indexé (entrées répétées : mixer, multiview) ? Un port
    unique doit recevoir `to_slot=None`, sinon `_apply_wire` cherche un slot qui n'existe pas."""
    return bool((st.get("port") or {}).get("input_key") or (st.get("port") or {}).get("from_list"))


def apply_sender_staged(sid, body, send_state):
    """PATCH IS-05 sur un Sender MXL.

    Un Sender MXL n'a rien à activer : le conteneur écrit son flux tant qu'il tourne, et
    `mxl_flow_id` est déterminé par le nom du flux, pas par le contrôleur. On accepte donc le
    staging (pour ne pas casser un contrôleur qui stage systématiquement avant de patcher un
    receiver), on refuse tout ce qui prétendrait CHANGER la destination, et on renvoie l'état
    réel. Mentir par un 200 sur une valeur qu'on n'applique pas serait un échec silencieux."""
    st = send_state.get(sid)
    if not st or not st.get("mxl"):
        return False, 404, {"code": 404, "error": "sender MXL inconnu", "debug": sid}
    reel = (st["active"]["transport_params"][0] or {})
    tps = body.get("transport_params")
    if isinstance(tps, list) and tps:
        tp = tps[0] or {}
        for cle in ("mxl_domain_id", "mxl_flow_id"):
            if cle in tp and tp[cle] not in (None, _AUTO, reel.get(cle)):
                return False, 400, {
                    "code": 400,
                    "error": "%s d'un Sender MXL n'est pas déplaçable : le flux est écrit par le "
                             "conteneur (valeur réelle %s)" % (cle, reel.get(cle)),
                    "debug": str(tp[cle])}
    if body.get("master_enable") is False:
        return False, 400, {"code": 400,
                            "error": "un Sender MXL ne se désactive pas depuis IS-05 : arrêter le "
                                     "conteneur producteur", "debug": sid}
    if "receiver_id" in body:
        st["staged"]["receiver_id"] = body["receiver_id"]
        st["active"]["receiver_id"] = body["receiver_id"]
    mode = ((body.get("activation") or {}) if isinstance(body.get("activation"), dict) else {}).get("mode")
    if mode:
        st["active"]["activation"] = {"mode": mode, "requested_time": None, "activation_time": _now()}
    return True, 200, st["active"]
