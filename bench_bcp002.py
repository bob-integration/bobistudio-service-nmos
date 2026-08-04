# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Banc BCP-002-01 / BCP-002-02 — À LANCER À LA MAIN, jamais depuis l'orchestrateur.

    ./venv/bin/python services/nmos/bench_bcp002.py

Travaille sur une COPIE de la base réelle (les grouphints du parc sont la meilleure donnée d'essai
qui soit) : **rien n'est écrit dans la production**, la copie est supprimée à la fin.

Vérifie les deux propriétés qui se cassent en silence :
  - un grouphint est IMMUABLE une fois écrit — un rebuild qui le réécrirait ne lèverait aucune
    erreur, il changerait juste le nommage de production sous les pieds des contrôleurs ;
  - la normalisation des index ne touche PAS au texte de l'exploitant (un préfixe finissant par un
    chiffre n'est pas un index), et elle est idempotente.
Plus la présence et l'unicité des tags d'asset, et la concordance IS-04 ↔ IS-12."""
import os, shutil, sys, tempfile

_RACINE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(_RACINE, "db_bobistudio.db")
TMP = tempfile.mkdtemp(prefix="bcp002-")
DB = os.path.join(TMP, "db_bobistudio.db")
shutil.copy(SRC, DB)
for suf in ("-wal", "-shm"):
    if os.path.exists(SRC + suf):
        shutil.copy(SRC + suf, DB + suf)

sys.path.insert(0, _RACINE)
from app import config
config.DB_PATH = DB
from app import database
database.DB_PATH = DB

from services import nmos
from app.database import db_nmos_resources, db_nmos_resource_get

ECHECS = []
def verifier(c, quoi):
    print(("  ok   " if c else "  ÉCHEC ") + quoi)
    if not c: ECHECS.append(quoi)


def grouphints():
    return {r["id"]: (r.get("group_name"), r.get("role")) for r in db_nmos_resources()}


print("=== base copiée : %d ressources au registre ===" % len(db_nmos_resources()))
avant = grouphints()
ex_avant = sorted({g for g, _ in avant.values() if g})[:6]
print("  groupes avant :", ex_avant)

print("\n=== normalisation des index (rupture unique) ===")
n = nmos.normaliser_grouping_registre()
apres = grouphints()
print("  %d lignes normalisées" % n)
print("  groupes après :", sorted({g for g, _ in apres.values() if g})[:6])
verifier(n > 0, "la normalisation a bien touché des lignes")
verifier(all(g and g[0].startswith("2110") for g in apres.values()),
         "les noms de groupe restent bâtis sur la même base")
import re
verifier(all(re.search(r"\d\d$", g) or not re.search(r"\d$", g) for g, _ in apres.values()),
         "plus aucun index de groupe à un seul chiffre")
verifier(all(re.search(r"\d\d$", r) or not re.search(r"\d$", r) for _, r in apres.values()),
         "plus aucun index de rôle à un seul chiffre")

print("\n=== idempotence ===")
verifier(nmos.normaliser_grouping_registre() == 0, "un second passage ne change rien")

print("\n=== immuabilité : un rebuild ne doit PLUS réécrire les grouphints ===")
fige = grouphints()
# On simule ce que faisait le rebuild : réécrire avec la valeur RECALCULÉE (non paddée).
from app.database import db_nmos_resource_upsert
rid = next(iter(fige))
r0 = db_nmos_resource_get(rid)
db_nmos_resource_upsert(rid, r0["kind"], r0["essence"], r0["label"],
                        "PREFIXE-CHANGE 1", "role-usurpe",
                        r0.get("transport") or {}, r0.get("bind_instance_uuid"), r0.get("bind_slot"))
r1 = db_nmos_resource_get(rid)
verifier((r1["group_name"], r1["role"]) == fige[rid],
         "un upsert avec d'autres valeurs NE change PAS le grouping figé (%r)" % (fige[rid],))
verifier(r1["label"] == r0["label"], "le libellé, lui, reste rafraîchissable")

print("\n=== une ressource NEUVE prend bien le grouping fourni ===")
from app.database import db_nmos_resource_create
neuf = db_nmos_resource_create("sender", "video", "Banc", "BANC 07", "video")
rn = db_nmos_resource_get(neuf)
verifier((rn["group_name"], rn["role"]) == ("BANC 07", "video"),
         "première écriture : le grouping fourni est retenu")

print("\n=== garde-fou : un préfixe d'exploitant finissant par un chiffre n'est PAS réécrit ===")
solo = db_nmos_resource_create("sender", "video", "Solo", "REGIE 1", "video")
n2 = nmos.normaliser_grouping_registre()
rs = db_nmos_resource_get(solo)
verifier(rs["group_name"] == "REGIE 1",
         "« REGIE 1 » (radical sans famille) reste intact — c'est le texte de l'exploitant")

print("\n=== BCP-002-02 : tags d'asset ===")
node = nmos._build_node_resource("0:0")
dev = nmos._build_cluster_device_resource("d", "0:0")
T = nmos
for nom, res, fonction in (("Node", node, False), ("Device", dev, True)):
    t = res.get("tags") or {}
    for tag in (T.TAG_MANUFACTURER, T.TAG_PRODUCT, T.TAG_INSTANCE_ID):
        verifier(isinstance(t.get(tag), list) and len(t[tag]) == 1,
                 "%s : %s présent avec EXACTEMENT une valeur" % (nom, tag.split(':')[-1]))
    if fonction:
        verifier(isinstance(t.get(T.TAG_FUNCTION), list) and len(t[T.TAG_FUNCTION]) >= 1,
                 "Device : function présent (au moins une valeur)")
    else:
        verifier(T.TAG_FUNCTION not in t, "Node : pas de tag function (réservé au Device)")
a = nmos.asset_info()
print("  asset =", a)
verifier(a["instance_id"] == nmos._get_node_id(),
         "instance-id = l'UUID du Node persiste (unique par installation)")
verifier(all(a[k] for k in ("manufacturer", "product", "instance_id", "function")),
         "aucune valeur d'asset vide")

print("\n=== IS-12 et IS-04 racontent le MÊME appareil ===")
from services.nmos import is12, modele
app = modele._construire_appareil()
dm = app.device_manager._vals
verifier(dm["manufacturer"]["name"] == a["manufacturer"], "fabricant identique des deux côtés")
verifier(dm["product"]["name"] == a["product"], "produit identique des deux côtés")
verifier(dm["serialNumber"] == a["instance_id"], "identifiant d'instance identique des deux côtés")

print("\n=== unicité groupe:rôle préservée après normalisation ===")
import collections
paires = [("%s:%s" % (g, r)) for g, r in apres.values() if g]
d = [(p, n) for p, n in collections.Counter(paires).items() if n > 1]
verifier(not d, "aucun couple groupe:rôle dupliqué (MUST du registre) : %r" % (d[:3],))

print("\n" + "=" * 60)
print("ÉCHECS : %d" % len(ECHECS))
for e in ECHECS: print("  - " + e)
shutil.rmtree(TMP, ignore_errors=True)
sys.exit(1 if ECHECS else 0)
