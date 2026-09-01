# NMOS — a Bobi.Studio service

*[Version française](README.md)*

An implementation of the **AMWA NMOS** specifications for
[Bobi.Studio](https://github.com/bob-integration/bobistudio), a broadcast orchestrator built
on the ST 2110 / MXL bus.

The service registers with the facility's registry, publishes production containers as
Nodes / Devices / Senders / Receivers, exposes their parameters to MS-05-02 control, and
reports stream health — in both directions: what we produce, and what a third-party device
declares about itself.

---

## What is implemented

| Specification | What it brings | Where |
|---|---|---|
| **IS-04** | Registration and discovery: Node, Devices, Senders, Receivers, plus DNS-SD discovery of the host facility's registry | `registre.py`, `decouverte.py`, `conteneur_node.py` |
| **IS-05** | Connection: activating a Receiver onto a Sender, SDP, immediate or scheduled activation | `registre.py` |
| **IS-07** | Events and tally, both ways — we publish ours, and we consume a third party's | `is07.py`, `is07_client.py`, `is07_entrant.py` |
| **IS-09** | System Parameters: the facility's global constants | `is09.py` |
| **IS-12** | MS-05-02 control over WebSocket (dedicated port 5010) | `is12.py`, `ncp.py`, `modele.py` |
| **IS-14** | The same model over REST, with `bulkProperties` backup and restore | `is14.py` |

Plus the **BCPs**: `002-01` (grouping, frozen at registration), `002-02` (asset identity),
`003-02` (authorization), `004-01` (receiver capabilities), `008-01` and `008-02` (Receiver
and Sender health monitors), `007-03` (exposing the MXL bus through IS-04/05).

**The control model is shared.** `modele.py` holds the live MS-05-02 model with a reference
count; IS-12 and IS-14 are only two transports onto it. A property written through one is
immediately readable through the other — the point an implementation that duplicates the
model misses, and it only shows when you cross the two protocols.

---

## What to know before reading it

**UUIDs are derived, not drawn at random.** A flow's identity comes from its hostname
(`uuid5`), a container's from its `instance_uuid`, which survives a re-creation. An external
controller that subscribed therefore does not lose its target when the container is rebuilt.

**BCP-002-01 grouping is immutable.** It is frozen at registration and never moves: a
`group_hint` that changes under a subscribed controller is a silent loss of routing.

**BCP-008 works both ways.** `monitors.py` publishes the health of *our* Receivers and
Senders; `supervision_tiers.py` does the reverse — it wires a third-party device's BCP-008
statuses into our own alerts, so a neighbour's failure shows up on the same screen.

The vendored AMWA models live in `nc_models/`: **do not edit them**, they are taken from the
specification as-is.

---

## Benches

Four measurement programs, run by hand against a live instance — they do not run in
continuous integration, deliberately: they need a real registry and real containers.

```bash
python3 is12_bench.py          # the WebSocket transport and the control model
python3 is14_bench.py          # the REST transport and bulkProperties
python3 bench_telemetrie.py    # the BCP-008 monitors
python3 bench_bcp002.py        # grouping and asset identity
```

`client_is12.py` and `client_ncp.py` are standalone clients: they query a **third-party**
device, not ours — useful to check what a piece of equipment actually announces, rather than
what its documentation claims.

---

## Installing

This repository is a **submodule** of Bobi.Studio, mounted at `services/nmos/`. The service
is discovered at start-up and registers its own blueprint; configure it under
**Settings → NMOS** (enable, registry address, domain, IS-12 port).

It is not usable outside the orchestrator: it reads its configuration and the state of
containers from its database.

---

## Licence

GPL-3.0-or-later — see [LICENSE](LICENSE). Copyright © 2026 BOBI SAS, France.

The AMWA models under `nc_models/` are published by AMWA under their own terms.
