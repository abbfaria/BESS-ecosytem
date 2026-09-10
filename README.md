# BESS Edge–Cloud Ecosystem

This repository is the practical implementation component of a Master's
thesis, **"Creation of a System of Interaction between a Microcontroller and
a Cloud Management Platform."** It is an academic project, not a production
product.

The thesis studies real-time electricity price arbitrage for a grid-connected
solar PV system with a LiFePO4 battery energy storage system (BESS), using
an edge device (a Raspberry Pi in the physical design; simulated here by the
`edge/` service) to control a hybrid inverter and a cloud platform to
aggregate telemetry, run a day-ahead charge/discharge optimizer against
Ukrainian Day-Ahead Market (DAM) prices, and expose monitoring/control
interfaces.

This repo is a self-contained software testbed for that architecture: a
simulated edge node and a full cloud stack (broker, database, dashboards,
API), talking to each other over authenticated MQTT, deployable to two
VMs for end-to-end experiments and the thesis's test cases.

## Architecture

```
        mTLS :8883 (MQTT)
  edge/  ───────────────────────►  cloud/
  ├─ BESS/PV sensor emulator        ├─ mosquitto  (MQTT broker, mTLS)
  ├─ SQLite store + offline buffer  ├─ cloud-api  (FastAPI: MQTT subscriber
  ├─ MILP/greedy optimizer          │              + InfluxDB writer + REST)
  │  (PuLP/CBC vs. DAM prices)      ├─ influxdb   (time-series storage)
  ├─ MQTT client (mTLS, replay)     └─ grafana    (dashboards)
  └─ local REST API (:8000)
```

Edge publishes telemetry every few seconds and buffers locally (SQLite) if
the link drops, replaying on reconnect. Cloud stores it in InfluxDB, serves
it over a REST API, and renders it in Grafana. Commands and daily
charge/discharge schedules flow the other way, edge → cloud → edge.

## Repository layout

| Path | What |
|---|---|
| `edge/` | Edge service: sensor emulator, MQTT client, SQLite buffer, optimizer, local API. Deploys to the edge node. |
| `cloud/` | Cloud stack: mosquitto config/ACLs, cloud-api (FastAPI), Grafana provisioning/dashboards, nginx. Deploys to the cloud node. |
| `shared/` | MQTT topic names and message schemas shared by both sides. |
| `tests/` | Unit test suite (mocked I/O — no live broker/DB required). |
| `.github/workflows/` | CI/CD: tests on every push/PR; on push to `main`, self-hosted runners on each VM redeploy that VM's stack. |

## Running it

Each side needs its own `.env` (not committed — see `edge/docker-compose.yml`
and `cloud/docker-compose.yml` for the variables each expects) and mTLS
certificates generated with `edge/scripts/generate_certs.sh`.

```bash
# Certificates (once, produces certs/ca.crt + certs/{server,edge,cloud-api}.{crt,key})
edge/scripts/generate_certs.sh bess-edge-01

# Cloud node
cd cloud && docker compose up -d --build

# Edge node
cd edge && docker compose up -d --build
```

## Tests

```bash
python -m unittest discover -s tests -v
```

## CI/CD

Every push runs the unit test suite on a GitHub-hosted runner. A push to
`main` additionally redeploys both VMs via self-hosted runners registered
on each one: the workflow syncs the relevant `edge/`/`cloud/` subtree into
the live deployment directory (never touching `.env`, `certs/`, or data
volumes) and runs `docker compose up -d --build`, gated on the tests
passing first.
