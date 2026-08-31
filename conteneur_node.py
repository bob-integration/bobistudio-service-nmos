"""Plan 2 — chaque conteneur est un Node NMOS, et l'orchestrateur en est le CONTRÔLEUR.

À ne pas confondre avec `mxl.py`, qui est le **plan 1** : là, l'orchestrateur est un ÉQUIPEMENT
qui publie un Node unique (2110 + MXL sur le même Device de cluster) au contrôleur du client.
Ici, c'est l'inverse : **le conteneur porte l'API**, l'orchestrateur la lit.

    Plan 1 — nous sommes un équipement   → un Node « MXL Orchestrator », public : le client
    Plan 2 — nous sommes un contrôleur   → un Node PAR CONTENEUR, public : nous (puis les tiers)

═══ Le conteneur ne calcule rien ═════════════════════════════════════════════════════════════

L'orchestrateur POUSSE au conteneur un document décrivant ses ressources (`POST :8081/nmos`), et
l'agent le sert découpé sur `/x-nmos/`. Toute la dérivation — manifeste du plugin, identités,
contraintes — reste ici, où elle est déjà écrite et éprouvée. Embarquer une seconde implémentation
dans l'agent, c'est se garantir deux modèles qui divergent, et découvrir l'écart chez un client.

═══ Pourquoi les identifiants DIFFÈRENT de ceux du plan 1 ════════════════════════════════════

Ce n'est pas un choix, c'est une contrainte NMOS : **une ressource IS-04 porte UN `device_id`**.
Le même flux MXL ne peut donc pas porter le même identifiant sous le Device de cluster (plan 1) et
sous le Node de son conteneur (plan 2) — ce serait une ressource revendiquée par deux Devices.

Et c'est cohérent avec la doctrine d'identité du produit : le plan 1 est keyé sur des barreaux
STABLES (`instance_uuid`, et à terme l'emplacement), parce que le routage d'un contrôleur client
doit survivre au recreate ; le Node d'un conteneur, lui, ne connaît que sa propre vie. Les deux
identités répondent à deux questions différentes : « la sortie du multiview de la régie 1 » n'est
pas « la sortie de ce conteneur-ci ».

═══ Surface et sécurité ══════════════════════════════════════════════════════════════════════

Servie sur le `:8081` de l'agent, **déjà authentifié par token et prêt pour le mTLS**. Donc aucune
surface réseau nouvelle, et rien à traiter côté BCP-003 : seul l'orchestrateur peut lire. Ouvrir
cette API à un contrôleur TIERS demanderait un port distinct et une autorisation (IS-10) — c'est
une autre décision, pas un réglage.
"""

import json
import logging

log = logging.getLogger(__name__)

IS04 = "v1.3"
IS05 = "v1.1"


def actif():
    """Réglage `nmos_conteneur_node` — FERMÉ par défaut. Pousser un document à chaque déploiement
    est un effet de bord sur le chemin critique : on ne l'active pas sans le vouloir."""
    from . import _setting
    return str(_setting("nmos_conteneur_node", "0")).strip().lower() in ("1", "true", "on", "yes")


def _rid(kind, instance_uuid, essence="", slot=""):
    """Identifiant de ressource du PLAN 2. Préfixe `n2:` — distinct de celui du plan 1 par
    construction, cf. l'entête (une ressource IS-04 ne peut avoir qu'un `device_id`)."""
    from . import _stable_uuid
    return _stable_uuid("n2:%s:%s:%s:%s" % (kind, instance_uuid, essence, slot))


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Construction du document
# ══════════════════════════════════════════════════════════════════════════════════════════════

def document(conteneur):
    """Document NMOS d'un conteneur, ou None s'il n'a aucun port MXL.

    Réutilise les constructeurs de `mxl.py` : c'est la MÊME dérivation que le plan 1, avec d'autres
    identités et un autre Device. Deux dérivations séparées finiraient par ne plus dire la même
    chose du même flux."""
    from . import mxl, asset_info
    ports = mxl.ports_of(conteneur)
    if not ports["produces"] and not ports["consumes"]:
        return None

    vmid = conteneur["vmid"]
    iu = conteneur.get("instance_uuid") or ("vmid:%s" % vmid)
    hostname = conteneur.get("hostname") or ("conteneur %s" % vmid)
    version = _version()
    domaine = mxl._domain_id(conteneur.get("node_id"))
    a = asset_info()

    nid = _rid("node", iu)
    did = _rid("device", iu)
    doc = {
        "node": {
            "id": nid, "version": version, "label": hostname,
            "description": "Media function Bobi.Studio (%s)" % _type(conteneur),
            "href": None,          # servi sur le :8081 de l'agent, pas sur une URL publique
            "hostname": hostname,
            "tags": {"urn:x-nmos:tag:asset:manufacturer/v1.0": [a["manufacturer"]],
                     "urn:x-nmos:tag:asset:product/v1.0": [a["product"]],
                     "urn:x-mxl:vmid": [str(vmid)]},
            "api": {"versions": [IS04], "endpoints": []},
            "services": [], "caps": {},
            "clocks": [{"name": "clk0", "ref_type": "internal"}],
            # BCP-007-03 : MXL ne sort par aucune NIC. Un Node sans interface est cohérent avec
            # des ressources à `interface_bindings` vide.
            "interfaces": [],
        },
        "devices": [{
            "id": did, "version": version, "label": hostname,
            "description": "Media function %s" % _type(conteneur),
            "tags": {}, "type": "urn:x-nmos:device:generic", "node_id": nid,
            "senders": [], "receivers": [], "controls": _controls(conteneur),
        }],
        "sources": [], "flows": [], "senders": [], "receivers": [],
        "connection": {"senders": {}, "receivers": {}},
    }

    grp_tx = mxl._groupes(ports["produces"], hostname)
    grp_rx = mxl._groupes(ports["consumes"], hostname)
    params = _params(conteneur)

    for p in ports["produces"]:
        shm = (p.get("shm") or "").strip()
        if not shm:
            continue
        ess, slot = mxl._essence(p), p["slot"]
        lbl = p.get("label") or ("%s %s" % (hostname, ess))
        sid = _rid("source", iu, ess, slot)
        fid = _rid("flow", iu, ess, slot)
        snd = _rid("sender", iu, ess, slot)
        doc["sources"].append(mxl._build_source(sid, did, vmid, hostname, lbl, ess,
                                                p.get("format"), version))
        doc["flows"].append(mxl._build_flow(fid, did, sid, vmid, hostname, lbl, ess,
                                            p.get("format"), version))
        s = mxl._build_sender(snd, did, fid, vmid, hostname, lbl, version)
        mxl._poser_grouphint(s, grp_tx[p["key"]])
        doc["senders"].append(s)
        doc["devices"][0]["senders"].append(snd)
        etat = mxl.empty_staged("sender", domaine, mxl.flow_uuid(shm))
        etat["master_enable"] = True
        doc["connection"]["senders"][snd] = {
            "constraints": [{"mxl_domain_id": {"enum": [domaine] if domaine else []},
                             "mxl_flow_id": {"enum": [mxl.flow_uuid(shm)]}}],
            "staged": etat, "active": json.loads(json.dumps(etat)),
        }

    for p in ports["consumes"]:
        ess, slot = mxl._essence(p), p["slot"]
        rid = _rid("receiver", iu, ess, slot)
        lbl = p.get("label") or ("%s entrée %s %d" % (hostname, ess, slot + 1))
        r = mxl._build_receiver(rid, did, vmid, hostname, lbl, ess, version)
        mxl._poser_grouphint(r, grp_rx[p["key"]])
        doc["receivers"].append(r)
        doc["devices"][0]["receivers"].append(rid)
        shm = mxl.wire_of(p, params)
        etat = mxl.empty_staged("receiver", domaine,
                                mxl.flow_uuid(shm) if shm else None)
        etat["master_enable"] = bool(shm)
        doc["connection"]["receivers"][rid] = {
            "constraints": [{"mxl_domain_id": {"enum": [domaine] if domaine else []},
                             "mxl_flow_id": {}}],
            "staged": etat, "active": json.loads(json.dumps(etat)),
        }
    return doc


def _controls(conteneur):
    """Tableau `controls` du Device DU CONTENEUR — c'est par là, et seulement par là, qu'un
    contrôleur découvre comment piloter cet appareil (IS-12 § « IS-04 interactions »).

    ★ Deux hôtes différents, et c'est légitime : `href` est une URL, rien n'impose qu'elle
    pointe le même serveur que le Node.
      · la **Connection API IS-05** du conteneur est servie par son propre agent ;
      · le **contrôle MS-05-02 (IS-12/IS-14)** vit dans l'ORCHESTRATEUR, où le modèle est déjà
        implémenté et porte un bloc par conteneur. Embarquer un serveur IS-12 dans l'agent
        reviendrait à y mettre un protocole à état sur WebSocket, plus `ncp.py` et les modèles
        AMWA, dans chaque image — sans rien apporter qu'on n'ait déjà.
    Un Device qui n'annonce AUCUN contrôle se lit « non pilotable ». Le dire de travers serait
    pire, le taire l'est presque autant."""
    from . import IS05_VERSION
    out = []
    try:
        from app.metrics import get_container_ip
        from app.deploy import agent_url
        ip = get_container_ip(conteneur["vmid"])
        if ip:
            out.append({"href": agent_url(ip, "/x-nmos/connection/%s/" % IS05_VERSION),
                        "type": "urn:x-nmos:control:sr-ctrl/%s" % IS05_VERSION})
    except Exception as e:
        log.debug("nmos/plan2: control IS-05 non annoncé (%s)", e)
    for mod in ("is12", "is14"):
        try:
            m = __import__("services.nmos." + mod, fromlist=[mod])
            if m.actif():
                out.append({"href": m.href(), "type": m.TYPE_CONTROL})
        except Exception as e:
            log.debug("nmos/plan2: control %s non annoncé (%s)", mod, e)
    return out


def _type(c):
    dc = _dc(c)
    return (dc or {}).get("type") or "?"


def _params(c):
    return ((_dc(c) or {}).get("params") or {})


def _dc(c):
    dc = c.get("deploy_config")
    try:
        return json.loads(dc) if isinstance(dc, str) else dc
    except Exception:
        return None


def _version():
    from . import _tai_version
    return _tai_version()


# ══════════════════════════════════════════════════════════════════════════════════════════════
# Pousser / lire — l'orchestrateur en client
# ══════════════════════════════════════════════════════════════════════════════════════════════

def pousser(vmid):
    """Pousse le document au conteneur. True si accepté. Best-effort : ne JAMAIS faire échouer un
    déploiement parce que la surface NMOS n'a pas pu être poussée."""
    if not actif():
        return False
    from app.database import db_get_container
    from app.deploy import agent_headers, agent_session, agent_url
    from app.metrics import get_container_ip
    c = db_get_container(vmid)
    if not c:
        return False
    doc = document(c)
    if doc is None:
        return False
    ip = get_container_ip(vmid)
    if not ip:
        return False
    try:
        r = agent_session().post(agent_url(ip, "/nmos"), json=doc, timeout=5,
                                 headers=agent_headers(vmid))
        if r.status_code != 200:
            log.warning("nmos/plan2: push vers %s refusé (HTTP %s)", vmid, r.status_code)
            return False
        return True
    except Exception as e:
        # Un agent d'ancienne image ne connaît pas /nmos et rend 404 : ce n'est pas une panne,
        # c'est un conteneur qui n'a pas encore été recréé. On n'alerte donc pas.
        log.info("nmos/plan2: push vers %s impossible (%s)", vmid, e)
        return False


def lire(vmid, chemin="/x-nmos/node/%s/self" % IS04):
    """Lit la surface NMOS SERVIE PAR LE CONTENEUR. C'est ici que l'orchestrateur est CLIENT.
    Renvoie (status, payload) ; (None, message) si injoignable."""
    from app.deploy import agent_headers, agent_session, agent_url
    from app.metrics import get_container_ip
    ip = get_container_ip(vmid)
    if not ip:
        return None, "IP introuvable"
    try:
        r = agent_session().get(agent_url(ip, chemin), timeout=5, headers=agent_headers(vmid))
        try:
            return r.status_code, r.json()
        except Exception:
            return r.status_code, r.text[:200]
    except Exception as e:
        return None, str(e)


def comparer(vmid):
    """Confronte ce que le CONTENEUR sert à ce que l'orchestrateur a calculé.

    C'est le go/no-go du plan 2 : si les deux vues divergent, c'est que le conteneur sert un
    document périmé (pas repoussé après un changement) ou que la dérivation a bougé d'un côté
    seulement. Renvoie un dict lisible, jamais une exception."""
    from app.database import db_get_container
    c = db_get_container(vmid)
    attendu = document(c) if c else None
    if attendu is None:
        return {"vmid": vmid, "verdict": "pas de ports MXL"}
    code, servi = lire(vmid, "/x-nmos/node/%s/senders" % IS04)
    if code != 200 or not isinstance(servi, list):
        return {"vmid": vmid, "verdict": "non servi", "code": code, "detail": servi}
    ids_servis = {x.get("id") for x in servi}
    ids_attendus = {x["id"] for x in attendu["senders"]}
    return {
        "vmid": vmid,
        "verdict": "concordant" if ids_servis == ids_attendus else "DIVERGENT",
        "servis": len(ids_servis), "attendus": len(ids_attendus),
        "en_trop": sorted(ids_servis - ids_attendus),
        "manquants": sorted(ids_attendus - ids_servis),
    }
