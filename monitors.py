# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""AMWA BCP-008-01 / BCP-008-02 — supervision des Receivers et Senders.

`NcReceiverMonitor` et `NcSenderMonitor` sont des objets MS-05-02 (cf. `ncp.py`) publiés sur
IS-12 (cf. `is12.py`). Ce module fait le travail de fond : traduire ce que le moteur 2110_io
rapporte réellement en statuts normalisés, avec les règles de temporisation de la BCP.

★ LES STATUTS SE COMPARENT À L'INTENTION, JAMAIS À UN IDÉAL. C'est écrit dans la BCP elle-même
(« Inactive » est un état NEUTRE, pas une panne) et c'est la règle du projet. Concrètement :
  - un receiver sans abonnement IS-05 actif est `Inactive` — surtout pas `Unhealthy` ;
  - un slot TX en mire de repli n'est pas en panne : il fait ce qu'on lui a demandé ;
  - les drapeaux de contenu (image figée, noir, silence) ne dégradent le `streamStatus` QUE si
    l'exploitant les a ARMÉS sur cette source (`io2110_flows.alarmes_par_slot`). Une mire figée
    volontairement n'est pas un défaut ; la même image figée sur une caméra en direct l'est.

Ce que nous NE mesurons pas, et que nous ne prétendons donc pas mesurer :
  - la perte et le retard PAQUET côté RX. La BCP prévoit ce cas : `GetLostPacketCounters` et
    `GetLatePacketCounters` doivent alors être implémentées et renvoyer une collection VIDE.
    Renvoyer des zéros serait un mensonge (« zéro perte mesurée » ≠ « rien n'est mesuré »).
  - la santé PAR JAMBE d'un flux redondant 2022-7. Tant qu'on ne sait pas dire quelle jambe est
    tombée, `connectionStatus` ne prend jamais la valeur `PartiallyHealthy` « récupération de
    perte en cours » : on ne peut pas la distinguer d'une réception nominale.
"""
import logging
import time

from . import ncp

log = logging.getLogger(__name__)

# ─── Valeurs d'énumération (modèle de supervision AMWA) ──────────────────────────────────────
# NcConnectionStatus / NcStreamStatus / NcTransmissionStatus / NcEssenceStatus / NcOverallStatus
INACTIVE          = 0
HEALTHY           = 1
PARTIALLY_HEALTHY = 2
UNHEALTHY         = 3
# NcSynchronizationStatus : 0 = NotUsed (neutre), puis mêmes rangs.
NOT_USED          = 0
# NcLinkStatus : pas d'état neutre.
ALL_UP    = 1
SOME_DOWN = 2
ALL_DOWN  = 3

DELAI_RAPPORT_DEFAUT = 3          # secondes — valeur imposée par la BCP à la construction

# Rang de santé d'une valeur de statut, tous domaines confondus. Les états neutres (Inactive,
# NotUsed) valent 0 : ils ne dégradent rien et ne comptent dans aucun compteur de transition.
_RANG = {INACTIVE: 0, HEALTHY: 1, PARTIALLY_HEALTHY: 2, UNHEALTHY: 3}
_RANG_LIEN = {ALL_UP: 1, SOME_DOWN: 2, ALL_DOWN: 3}

# Libellés des drapeaux de présence signal du moteur (mêmes termes que les alertes du projet).
_LIBELLE_SIGNAL = {
    "black": "image noire", "frozen": "image figée", "silence": "silence audio",
    "clip": "saturation audio", "gamut": "image hors gamut",
    "tx_late": "trames en retard à l'émission",
}


class _Domaine:
    """Un statut de domaine : sa valeur publiée, son message, son compteur de transitions.

    Porte à lui seul la règle de temporisation de la BCP (§ status reporting delay) :
      - toute dégradation est publiée IMMÉDIATEMENT ;
      - toute amélioration doit se MAINTENIR pendant `statusReportingDelay` avant d'être publiée ;
      - le passage à l'état neutre (désactivation) est immédiat et sans délai ;
      - le compteur s'incrémente à chaque dégradation entre deux états NON neutres.

    La fenêtre d'instabilité qui suit une activation est gérée par l'appelant (il pousse
    `Healthy` pendant toute sa durée), pas ici : c'est une décision qui concerne tous les
    domaines d'un même monitor à la fois."""

    def __init__(self, moniteur, nom_statut, nom_message, nom_compteur, neutre, rangs):
        self.mon = moniteur
        self.nom_statut = nom_statut
        self.nom_message = nom_message
        self.nom_compteur = nom_compteur
        self.neutre = neutre
        self.rangs = rangs
        self._candidat = None
        self._depuis = 0.0

    @property
    def valeur(self):
        return self.mon._vals.get(self.nom_statut)

    @property
    def message(self):
        return self.mon._vals.get(self.nom_message)

    def rang(self, v):
        return self.rangs.get(v, 0)

    def _publier(self, valeur, message, degradation):
        if degradation:
            nouveau_msg = message
        else:
            # Retour vers un état plus sain : la BCP RECOMMANDE de conserver la cause précédente
            # préfixée de « Previously: » quand on n'a rien de neuf à dire. Sans ça, l'exploitant
            # qui regarde après coup ne voit qu'un voyant vert et aucune trace de l'incident.
            ancien = self.message
            if message is None and ancien and not ancien.startswith("Previously: "):
                nouveau_msg = "Previously: " + ancien
            else:
                nouveau_msg = message if message is not None else ancien
        self.mon.poser(self.nom_statut, valeur)
        self.mon.poser(self.nom_message, nouveau_msg)

    def appliquer(self, cible, message, maintenant, delai):
        if cible is None:                       # domaine non observable ce tour : on ne touche à rien
            return
        courant = self.valeur
        if cible == courant:
            self._candidat = None
            if message is not None and message != self.message:
                self.mon.poser(self.nom_message, message)
            return
        neutre_impliquee = (cible == self.neutre or courant == self.neutre)
        degradation = self.rang(cible) > self.rang(courant)
        if cible == self.neutre or degradation or delai <= 0:
            self._candidat = None
            if degradation and not neutre_impliquee:
                self.mon.poser(self.nom_compteur, (self.mon._vals.get(self.nom_compteur) or 0) + 1)
            self._publier(cible, message, degradation)
            return
        # Amélioration : elle doit tenir `delai` secondes avant d'être publiée.
        if self._candidat != cible:
            self._candidat, self._depuis = cible, maintenant
        elif maintenant - self._depuis >= delai:
            self._candidat = None
            self._publier(cible, message, False)

    def reinitialiser(self):
        self.mon.poser(self.nom_compteur, 0)
        self.mon.poser(self.nom_message, None)


class _Moniteur(ncp.NcWorker):
    """Partie commune aux deux monitors : NcStatusMonitor (3p1…3p3) + liens + synchronisation."""

    class_id = [1, 2, 2]

    def __init__(self, appareil, oid, role, owner, resource_id, resource_type,
                 label, contexte, description=None):
        touchpoints = [{"contextNamespace": "x-nmos",
                        "resource": {"resourceType": resource_type, "id": resource_id}}]
        super().__init__(appareil, oid, role, owner, user_label=label,
                         touchpoints=touchpoints, description=description)
        self.resource_id = resource_id
        self.ctx = contexte
        self._vals.update({
            "overallStatus": INACTIVE,
            "overallStatusMessage": None,
            "statusReportingDelay": DELAI_RAPPORT_DEFAUT,
            "linkStatus": ALL_UP,
            "linkStatusMessage": None,
            "linkStatusTransitionCounter": 0,
            "externalSynchronizationStatus": NOT_USED,
            "externalSynchronizationStatusMessage": None,
            "externalSynchronizationStatusTransitionCounter": 0,
            "synchronizationSourceId": None,
            "autoResetCountersAndMessages": True,
        })
        self.lien = _Domaine(self, "linkStatus", "linkStatusMessage",
                             "linkStatusTransitionCounter", None, _RANG_LIEN)
        self.sync = _Domaine(self, "externalSynchronizationStatus",
                             "externalSynchronizationStatusMessage",
                             "externalSynchronizationStatusTransitionCounter", NOT_USED, _RANG)
        self._domaines = [self.lien, self.sync]
        self._actif_precedent = False
        self._active_depuis = 0.0
        self._source_sync_precedente = None
        self._sync_change_jusqua = 0.0

    # ─── Propriétés ──────────────────────────────────────────────────────────────────────────
    def _ecrire(self, nom, valeur):
        if nom == "statusReportingDelay":
            if not isinstance(valeur, int) or isinstance(valeur, bool) or valeur < 0:
                return ncp.erreur(ncp.PARAMETER_ERROR,
                                  "statusReportingDelay doit être un entier de secondes ≥ 0")
        if nom == "autoResetCountersAndMessages" and not isinstance(valeur, bool):
            return ncp.erreur(ncp.PARAMETER_ERROR, "autoResetCountersAndMessages est un booléen")
        return super()._ecrire(nom, valeur)

    @property
    def delai(self):
        return int(self._vals.get("statusReportingDelay") or 0)

    # ─── Compteurs et messages ───────────────────────────────────────────────────────────────
    def _m_reset(self, args):
        self.reinitialiser()
        return ncp.ok()

    def reinitialiser(self):
        for d in self._domaines:
            d.reinitialiser()
        self.poser("overallStatusMessage", None)
        self._raz_compteurs_specifiques()

    def _raz_compteurs_specifiques(self):
        """Repères de remise à zéro des compteurs matériels (surchargé côté sender)."""

    # ─── Échantillonnage ─────────────────────────────────────────────────────────────────────
    def echantillonner(self, maintenant):
        actif = bool(self.ctx.actif())
        if actif and not self._actif_precedent:
            # Activation : les compteurs et messages repartent de zéro si l'exploitant l'a demandé
            # (défaut BCP), et la fenêtre d'instabilité s'ouvre.
            self._active_depuis = maintenant
            if self._vals.get("autoResetCountersAndMessages"):
                self.reinitialiser()
        self._actif_precedent = actif
        # Pendant la fenêtre qui suit l'activation, la BCP impose de publier Healthy et de RETENIR
        # tout état moins sain : un receiver met un instant à se verrouiller, crier pendant ce
        # temps-là ne renseigne personne.
        indulgence = actif and (maintenant - self._active_depuis) < self.delai
        releve = self.ctx.releve()
        self._echantillonner_domaines(maintenant, actif, indulgence, releve)
        self._maj_overall(actif)

    def _echantillonner_domaines(self, maintenant, actif, indulgence, releve):
        self._appliquer(self.lien, maintenant, indulgence, ALL_UP, *self._lien(releve))
        self._appliquer(self.sync, maintenant, indulgence, HEALTHY, *self._sync(maintenant, releve))

    def _appliquer(self, domaine, maintenant, indulgence, sain, cible, message):
        if indulgence and cible is not None and domaine.rang(cible) > domaine.rang(sain):
            cible, message = sain, None
        domaine.appliquer(cible, message, maintenant, self.delai)

    # ─── Domaine « liens physiques » ─────────────────────────────────────────────────────────
    def _lien(self, releve):
        liens = releve.get("liens")
        if not liens:
            # NcLinkStatus n'a pas d'état « inconnu ». Plutôt que d'inventer une panne, on publie
            # AllUp en DISANT dans le message que rien n'a été observé — l'exploitant voit la
            # nuance, un contrôleur qui n'affiche que la pastille ne voit pas d'alarme fantôme.
            return ALL_UP, "état des liens non publié par le moteur"
        bas, haut = liens.get("down") or [], liens.get("up") or []
        if not bas:
            return ALL_UP, None
        detail = ", ".join(str(i) for i in bas) + (" sont" if len(bas) > 1 else " est") + " hors lien"
        return (ALL_DOWN if not haut else SOME_DOWN), detail

    # ─── Domaine « synchronisation externe » ─────────────────────────────────────────────────
    def _sync(self, maintenant, releve):
        ptp = releve.get("ptp")
        if ptp is None:
            self.poser("synchronizationSourceId", None)
            return NOT_USED, "synchronisation externe non observée sur ce nœud"
        source = ptp.get("gm_id") or None
        if source and ptp.get("iface"):
            source = "{} on {}".format(source, ptp["iface"])
        if source != self._source_sync_precedente:
            # Changement de grandmaster : la BCP impose un passage TEMPORAIRE par PartiallyHealthy,
            # même quand le nouveau verrou est parfait — un changement de référence de temps est un
            # événement, pas un détail. La PERTE du grandmaster (source → None) n'est pas un
            # changement de source : c'est une panne, et la retarder de trois secondes derrière un
            # « partiellement sain » serait exactement l'inverse de ce qu'on veut.
            if self._source_sync_precedente is not None and source is not None:
                self._sync_change_jusqua = maintenant + max(self.delai, 1)
                self.poser("externalSynchronizationStatusMessage",
                           "Source change from: {}".format(self._source_sync_precedente))
            self._source_sync_precedente = source
        self.poser("synchronizationSourceId", source)
        if maintenant < self._sync_change_jusqua:
            return PARTIALLY_HEALTHY, "Source change from: {}".format(self._source_sync_precedente)
        if ptp.get("locked") or ptp.get("synced"):
            return HEALTHY, None
        if not ptp.get("running"):
            return UNHEALTHY, "aucun client PTP actif sur le nœud"
        if not source:
            return UNHEALTHY, "aucun grandmaster annoncé — horloge en roue libre"
        return UNHEALTHY, "déverrouillé du grandmaster {}".format(source)

    # ─── Statut global ───────────────────────────────────────────────────────────────────────
    def _domaines_overall(self):
        return self._domaines

    def _maj_overall(self, actif):
        if not actif:
            self.poser("overallStatus", INACTIVE)
            return
        pire, causes = HEALTHY, []
        for d in self._domaines_overall():
            r = d.rang(d.valeur)
            if r > _RANG[pire]:
                pire = {1: HEALTHY, 2: PARTIALLY_HEALTHY, 3: UNHEALTHY}[r]
            if r > 1 and d.message:
                causes.append(d.message)
        self.poser("overallStatus", pire)
        if causes:
            self.poser("overallStatusMessage", " ; ".join(dict.fromkeys(causes)))
        elif pire == HEALTHY:
            ancien = self._vals.get("overallStatusMessage")
            if ancien and not ancien.startswith("Previously: "):
                self.poser("overallStatusMessage", "Previously: " + ancien)


class MoniteurReceiver(_Moniteur):
    """AMWA BCP-008-01 §NcReceiverMonitor."""

    class_id = [1, 2, 2, 1]

    def __init__(self, appareil, oid, role, owner, receiver_id, label, contexte):
        super().__init__(appareil, oid, role, owner, receiver_id, "receiver", label, contexte,
                         description="Supervision du Receiver %s" % receiver_id)
        self._vals.update({
            "connectionStatus": INACTIVE, "connectionStatusMessage": None,
            "connectionStatusTransitionCounter": 0,
            "streamStatus": INACTIVE, "streamStatusMessage": None,
            "streamStatusTransitionCounter": 0,
        })
        self.connexion = _Domaine(self, "connectionStatus", "connectionStatusMessage",
                                  "connectionStatusTransitionCounter", INACTIVE, _RANG)
        self.stream = _Domaine(self, "streamStatus", "streamStatusMessage",
                               "streamStatusTransitionCounter", INACTIVE, _RANG)
        self._domaines = [self.lien, self.connexion, self.sync, self.stream]

    def _enregistrer_methodes(self):
        super()._enregistrer_methodes()
        self._methodes.update({(4, 1): self._m_lost_packets,
                               (4, 2): self._m_late_packets,
                               (4, 3): self._m_reset})

    def _m_lost_packets(self, args):
        # Collection VIDE, comme la BCP l'exige d'un appareil qui ne sait pas compter les paquets
        # perdus. Le moteur travaille en zéro-copie sur des TRAMES : il constate qu'une trame
        # n'est pas arrivée, jamais combien de paquets lui manquaient.
        return ncp.ok([], avec_valeur=True)

    def _m_late_packets(self, args):
        return ncp.ok([], avec_valeur=True)

    def _echantillonner_domaines(self, maintenant, actif, indulgence, releve):
        super()._echantillonner_domaines(maintenant, actif, indulgence, releve)
        self._appliquer(self.connexion, maintenant, indulgence, HEALTHY,
                        *self._connexion(actif, releve))
        self._appliquer(self.stream, maintenant, indulgence, HEALTHY, *self._stream(actif, releve))

    def _connexion(self, actif, releve):
        if not actif:
            return INACTIVE, None
        flux = releve.get("flux")
        if flux is None:
            return UNHEALTHY, "état du moteur 2110 indisponible"
        mode = flux.get("mode") or "idle"
        if mode == "error":
            return UNHEALTHY, "création de session échouée ({})".format(
                flux.get("error") or "transport")
        if mode in ("idle", "", "off", "none"):
            return UNHEALTHY, "abonné mais aucune session sur le moteur"
        if flux.get("stalled"):
            return UNHEALTHY, "aucun paquet reçu — source absente ou réseau"
        return HEALTHY, None

    def _stream(self, actif, releve):
        if not actif:
            return INACTIVE, None
        flux = releve.get("flux")
        if flux is None:
            return UNHEALTHY, "état du moteur 2110 indisponible"
        mode = flux.get("mode") or "idle"
        if mode == "error" or mode in ("idle", "", "off", "none") or flux.get("stalled"):
            return UNHEALTHY, "aucun flux décodable"
        anomalies = _anomalies_armees(releve)
        if anomalies:
            return PARTIALLY_HEALTHY, ", ".join(anomalies)
        return HEALTHY, None


class MoniteurSender(_Moniteur):
    """AMWA BCP-008-02 §NcSenderMonitor."""

    class_id = [1, 2, 2, 2]

    def __init__(self, appareil, oid, role, owner, sender_id, label, contexte):
        super().__init__(appareil, oid, role, owner, sender_id, "sender", label, contexte,
                         description="Supervision du Sender %s" % sender_id)
        self._vals.update({
            "transmissionStatus": INACTIVE, "transmissionStatusMessage": None,
            "transmissionStatusTransitionCounter": 0,
            "essenceStatus": INACTIVE, "essenceStatusMessage": None,
            "essenceStatusTransitionCounter": 0,
        })
        self.transmission = _Domaine(self, "transmissionStatus", "transmissionStatusMessage",
                                     "transmissionStatusTransitionCounter", INACTIVE, _RANG)
        self.essence = _Domaine(self, "essenceStatus", "essenceStatusMessage",
                                "essenceStatusTransitionCounter", INACTIVE, _RANG)
        self._domaines = [self.lien, self.transmission, self.sync, self.essence]
        # Les compteurs du moteur sont CUMULATIFS et ne redémarrent qu'avec lui : on mémorise le
        # repère de la dernière remise à zéro et on publie la différence.
        self._repere = {"late": 0, "repeats": 0}

    def _enregistrer_methodes(self):
        super()._enregistrer_methodes()
        self._methodes.update({(4, 1): self._m_transmission_errors,
                               (4, 2): self._m_reset})

    def _raz_compteurs_specifiques(self):
        flux = self.ctx.flux() or {}
        for k in ("late", "repeats"):
            self._repere[k] = int(flux.get(k) or 0)

    def _m_transmission_errors(self, args):
        flux = self.ctx.flux()
        if flux is None:
            return ncp.ok([], avec_valeur=True)
        compteurs = []
        if flux.get("late") is not None:
            compteurs.append({
                "name": "late frames",
                "value": max(0, int(flux["late"]) - self._repere["late"]),
                "description": "Trames remises trop tard à la couche transport (epoch manqué)",
            })
        if flux.get("repeats") is not None:
            compteurs.append({
                "name": "repeated frames",
                "value": max(0, int(flux["repeats"]) - self._repere["repeats"]),
                "description": "Trames rejouées faute de nouvelle image de la source",
            })
        return ncp.ok(compteurs, avec_valeur=True)

    def _echantillonner_domaines(self, maintenant, actif, indulgence, releve):
        super()._echantillonner_domaines(maintenant, actif, indulgence, releve)
        self._appliquer(self.transmission, maintenant, indulgence, HEALTHY,
                        *self._transmission(actif, releve))
        self._appliquer(self.essence, maintenant, indulgence, HEALTHY, *self._essence(actif, releve))

    def _transmission(self, actif, releve):
        if not actif:
            return INACTIVE, None
        flux = releve.get("flux")
        if flux is None:
            return UNHEALTHY, "état du moteur 2110 indisponible"
        if flux.get("fps") is None:
            # Le moteur ne publie de cadence que pour les sorties VIDÉO ; un flux 2110-30 ou -40
            # n'a aucune métrique d'émission. Dire « sain » sans le préciser laisserait croire que
            # quelque chose a été vérifié.
            return HEALTHY, "aucune métrique d'émission publiée pour ce flux"
        if flux.get("stalled"):
            return UNHEALTHY, "activé mais n'émet aucun flux"
        if (flux.get("signal") or {}).get("tx_late"):
            return PARTIALLY_HEALTHY, "trames remises en retard à la couche transport"
        return HEALTHY, None

    def _essence(self, actif, releve):
        """Santé de ce qu'on DONNE à émettre — distinct de la transmission elle-même.

        Le moteur TX est délibérément indépendant de son producteur : il continue d'émettre une
        cadence parfaite en rejouant la dernière image quand la source décroche. C'est la bonne
        conception (le fil ne doit jamais s'arrêter), mais ça rend l'incident INVISIBLE sur le
        seul `transmissionStatus`. C'est précisément ce que `essenceStatus` sert à dire."""
        if not actif:
            return INACTIVE, None
        flux = releve.get("flux")
        if flux is None:
            return UNHEALTHY, "état du moteur 2110 indisponible"
        if not flux.get("inputs_latency_ms"):
            # Aucune source câblée : le moteur sert sa sortie de repli (mire ou noir). Ce n'est pas
            # une panne — c'est configuré — mais ce n'est pas non plus le programme.
            return PARTIALLY_HEALTHY, "aucune source câblée — sortie de repli du moteur"
        fps_source = flux.get("fps_source")
        if fps_source is not None and float(fps_source) < 1.0 and float(flux.get("fps") or 0) >= 1.0:
            return UNHEALTHY, "la source n'avance plus — seules des trames rejouées sont émises"
        nominal = float(flux.get("fps_nominal") or 0)
        if fps_source is not None and nominal > 0 and float(fps_source) < nominal * 0.9:
            return PARTIALLY_HEALTHY, ("source à {:.1f} img/s pour {:.0f} attendues — "
                                       "trames rejouées".format(float(fps_source), nominal))
        anomalies = _anomalies_armees(releve)
        if anomalies:
            return PARTIALLY_HEALTHY, ", ".join(anomalies)
        return HEALTHY, None


def _anomalies_armees(releve):
    """Libellés des drapeaux de présence signal ACTIFS *et* armés par l'exploitant sur ce slot.

    `tx_late` en est exclu : il est déjà porté par `transmissionStatus`, le compter deux fois
    ferait passer un seul incident pour deux."""
    signal = (releve.get("flux") or {}).get("signal") or {}
    armes = (releve.get("alarmes") or {}).get("drapeaux") or {}
    return [_LIBELLE_SIGNAL.get(k, k) for k in sorted(signal)
            if k != "tx_late" and signal.get(k) and armes.get(k)]


class Contexte:
    """Tout ce qu'on sait dire d'un flux, rassemblé pour les monitors.

    N'ouvre AUCUNE connexion : la boucle de surveillance a déjà relevé le moteur au dernier tick,
    et les résultats sont dans les caches de `app.metrics`. Un monitor qui re-pollerait :8080 pour
    son compte multiplierait la charge par le nombre de flux (25 RX + 25 TX par nœud)."""

    def __init__(self, sens, f_cible, f_actif):
        self.sens = sens
        # Résolus À CHAQUE relevé, jamais figés à la construction : une ressource NMOS survit au
        # remplacement du conteneur qui la sert (c'est tout l'intérêt de l'identité d'instance).
        # Un monitor qui aurait capturé le vmid à sa création superviserait un conteneur mort.
        self._f_cible = f_cible
        self._f_actif = f_actif

    def cible(self):
        try:
            return self._f_cible() or {}
        except Exception as e:
            log.debug("BCP-008 : cible du monitor irrésolue (%s)", e)
            return {}

    def actif(self):
        try:
            return bool(self._f_actif())
        except Exception as e:
            log.debug("BCP-008 : état d'activation illisible (%s)", e)
            return False

    def flux(self):
        from app import metrics
        c = self.cible()
        if not c:
            return None
        return metrics.etat_flux(c["vmid"], self.sens, c["essence"], c["idx"], c.get("sub_idx"))

    def releve(self):
        from app import metrics
        c = self.cible()
        if not c:
            return {"flux": None, "liens": None, "ptp": None, "alarmes": None}
        return {
            "flux": self.flux(),
            "liens": metrics.etat_liens(c["vmid"]),
            "ptp": _ptp_du_conteneur(c["vmid"]),
            # Les drapeaux ne sont réglés que sur le flux VIDÉO d'un slot ; l'audio et l'ANC qui
            # lui sont rattachés en héritent (cf. io2110_flows.alarmes_par_slot).
            "alarmes": metrics.alarmes_slot(c["vmid"], self.sens, c["idx"], "video"),
        }


# Un nœud porte des DIZAINES de flux (25 RX + 25 TX par moteur) et chacun a son monitor. Résoudre
# le nœud puis lire son état PTP par monitor et par tick, c'est autant d'ouvertures SQLite à la
# seconde pour une réponse identique. On mémorise donc par conteneur, avec un TTL court : l'état
# PTP est échantillonné bien plus lentement que ça côté node_health, rien ne se perd.
_PTP_TTL_S = 2.0
_ptp_cache = {}     # vmid → (instant monotone, snapshot|None)


def _ptp_du_conteneur(vmid):
    hit = _ptp_cache.get(vmid)
    now = time.monotonic()
    if hit is not None and now - hit[0] < _PTP_TTL_S:
        return hit[1]
    val = None
    try:
        from app import node_health
        from app.database import db_get_container
        c = db_get_container(vmid) or {}
        nid = c.get("node_id")
        if nid is not None:
            snap = (node_health.latest().get("nodes") or {}).get(str(nid)) or {}
            ptp = snap.get("ptp")
            val = dict(ptp) if isinstance(ptp, dict) else None
    except Exception as e:
        log.debug("BCP-008 : état PTP illisible pour vmid %s (%s)", vmid, e)
        val = None
    _ptp_cache[vmid] = (now, val)
    return val
