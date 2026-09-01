# NMOS — service Bobi.Studio

*[English version](README.en.md)*

Implémentation des spécifications **AMWA NMOS** pour [Bobi.Studio](https://github.com/bob-integration/bobistudio),
un orchestrateur broadcast bâti sur le bus ST 2110 / MXL.

Le service s'enregistre auprès du registre de l'installation, publie les conteneurs de
production en Nodes / Devices / Senders / Receivers, expose leurs paramètres au contrôle
MS-05-02, et remonte la santé des flux — dans les deux sens : ce que nous produisons comme
ce qu'un appareil tiers déclare.

---

## Ce qui est implémenté

| Spécification | Ce qu'elle apporte | Où |
|---|---|---|
| **IS-04** | Enregistrement et découverte : Node, Devices, Senders, Receivers, plus la découverte DNS-SD du registre de l'installation d'accueil | `registre.py`, `decouverte.py`, `conteneur_node.py` |
| **IS-05** | Connexion : activation d'un Receiver sur un Sender, SDP, activation immédiate ou programmée | `registre.py` |
| **IS-07** | Événements et tally, dans les deux sens — nous publions les nôtres, et nous consommons ceux d'un appareil tiers | `is07.py`, `is07_client.py`, `is07_entrant.py` |
| **IS-09** | System Parameters : les constantes globales de l'installation | `is09.py` |
| **IS-12** | Contrôle MS-05-02 sur WebSocket (port dédié 5010) | `is12.py`, `ncp.py`, `modele.py` |
| **IS-14** | Le même modèle en REST, avec sauvegarde et restauration `bulkProperties` | `is14.py` |

Plus les **BCP** : `002-01` (grouping, figé au registre), `002-02` (identité d'asset),
`003-02` (autorisation), `004-01` (capacités de récepteur), `008-01` et `008-02`
(moniteurs de santé des Receivers et Senders), `007-03` (exposition du bus MXL en IS-04/05).

**Le modèle de contrôle est partagé.** `modele.py` porte le modèle vivant MS-05-02, avec un
compteur de références ; IS-12 et IS-14 n'en sont que deux transports. Une propriété écrite
par l'un est immédiatement lue par l'autre — c'est le point qu'une implémentation qui
duplique le modèle rate, et ça ne se voit qu'en croisant les deux protocoles.

---

## Ce qu'il faut savoir avant de le lire

**Les UUID sont dérivés, pas tirés au sort.** L'identité d'un flux vient de son nom d'hôte
(`uuid5`), et celle d'un conteneur de son `instance_uuid`, qui survit à une recréation. Un
contrôleur externe qui s'est abonné ne perd donc pas sa cible quand le conteneur est refait.

**Le grouping BCP-002-01 est immuable.** Il est figé au moment de l'enregistrement et ne
bouge plus : un `group_hint` qui change sous un contrôleur abonné vaut une perte de routage
silencieuse.

**BCP-008 va dans les deux sens.** `monitors.py` publie la santé de NOS Receivers et Senders ;
`supervision_tiers.py` fait l'inverse — il raccorde les statuts BCP-008 d'un appareil tiers
à nos propres alertes, pour qu'une panne chez le voisin se voie sur le même écran.

Les modèles AMWA vendorisés vivent dans `nc_models/` : **ne pas les éditer**, ils sont
repris tels quels de la spécification.

---

## Bancs

Quatre programmes de mesure, à lancer à la main contre une instance en marche — ils ne
tournent pas en intégration continue, et c'est voulu : ils ont besoin d'un vrai registre et
de vrais conteneurs.

```bash
python3 is12_bench.py          # le transport WebSocket et le modèle de contrôle
python3 is14_bench.py          # le transport REST et bulkProperties
python3 bench_telemetrie.py    # les moniteurs BCP-008
python3 bench_bcp002.py        # grouping et identité d'asset
```

`client_is12.py` et `client_ncp.py` sont des clients autonomes : ils servent à interroger un
appareil TIERS, pas le nôtre — utile pour vérifier ce qu'un équipement annonce réellement,
plutôt que ce que sa documentation prétend.

---

## Installation

Ce dépôt est un **sous-module** de Bobi.Studio, monté dans `services/nmos/`. Le service est
découvert au démarrage et enregistre son blueprint tout seul ; il se configure dans
**Réglages → NMOS** (activation, adresse du registre, domaine, port IS-12).

Il ne s'utilise pas en dehors de l'orchestrateur : il lit sa configuration et l'état des
conteneurs dans sa base.

---

## Licence

GPL-3.0-or-later — voir [LICENSE](LICENSE). Copyright © 2026 BOBI SAS, France.

Les modèles AMWA sous `nc_models/` sont publiés par l'AMWA sous leurs propres termes.
