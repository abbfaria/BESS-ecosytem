# BESS Edge–Cloud Ecosystem (MVP)

Practical component of the Master's thesis **"Creation of a System of
Interaction between a Microcontroller and a Cloud Management Platform."**
A minimal proof of concept: a simulated edge node (Raspberry Pi in the
physical design) controls a 20 kWh LiFePO4 battery next to a 10 kWp PV
array and sends telemetry over mTLS MQTT to a cloud stack that stores it
in InfluxDB and shows it in Grafana.

## Architecture

```
        mTLS :8883 (MQTT)
  edge/  ──────────────────►  cloud/
  └─ main.py: state_at(now)   ├─ mosquitto  (broker, mTLS + ACL)
     every 10 s → MQTT        ├─ cloud-api  (MQTT → InfluxDB, seeds history)
                              ├─ influxdb
                              └─ grafana    (dashboard "BESS – Огляд системи")
```

The whole business logic lives in one file, `edge/src/bess_model.py`:

* `prices(day)` — deterministic day-ahead market (DAM) price curve by UTC hour.
* `plan_day()` — greedy dispatch: grid-charge in the 6 cheapest hours,
  discharge/sell in the 6 most expensive ones.
  * SoC always stays within **30–90 %**.
  * **BESS revenue** = energy sold to the grid × DAM price − cost of scheduled
    `GRID_CHARGE`. House load and PV are not included.
  * Each day opens with a small sale from the energy kept overnight (60 % SoC),
    and grid charging is only allowed while it keeps the day's running total
    above 1 UAH, so **today's and yesterday's revenue are always > 0**.
* `state_at(t)` — full telemetry snapshot at any moment.

The edge publishes `state_at(now)`. cloud-api copies the same file into its
image at build time (`COPY edge/src/bess_model.py`), so the backfill and the
live data are identical and join without jumps. When cloud-api starts it
clears the bucket. Then, every 5 minutes, it writes:

| Measurement          | Range                          | Step   |
|----------------------|--------------------------------|--------|
| `telemetry`          | yesterday 00:00 UTC → now      | 5 min  |
| `telemetry_forecast` | now → end of tomorrow          | 15 min |
| `market_price`       | yesterday … tomorrow, hourly   | 1 h    |

Live edge telemetry goes to `telemetry` as it arrives. Days are UTC days.

## Repository layout

| Path | What |
|---|---|
| `edge/src/bess_model.py` | Prices, dispatch plan, telemetry model (shared with cloud). |
| `edge/src/main.py` | Edge loop: publish telemetry over MQTT. |
| `cloud/api/src/main.py` | MQTT subscriber, InfluxDB writer, seeding, `/health`, `/api/v1/latest`. |
| `cloud/grafana/` | Datasource and dashboard provisioning. |
| `cloud/broker/` | Mosquitto config and ACL. |
| `tests/` | Invariant tests for the model (stdlib only). |
| `ops/` | Host-level systemd units and the deploy script for the two VMs. |

## Running it

```bash
# Certificates, once (writes certs/ at the repo root)
edge/scripts/generate_certs.sh bess-edge-01
mkdir -p cloud/certs edge/certs
cp certs/ca.crt certs/server.crt certs/server.key cloud/certs/
cp certs/ca.crt certs/edge.crt certs/edge.key edge/certs/
chmod 644 cloud/certs/server.key edge/certs/edge.key  # containers run as non-root

# Cloud node (Grafana: http://localhost:3000, admin/changeme)
cd cloud && docker compose up -d --build

# Edge node (set MQTT_HOST to the cloud node's name/IP, e.g. in edge/.env)
cd edge && docker compose up -d --build
```

To run both on one machine, start the edge against the host's broker:

```bash
docker build -t bess-edge edge
docker run -d --name bess-edge-local --network host \
  -v "$PWD/edge/certs:/certs:ro" -e MQTT_HOST=localhost bess-edge
```

Check what landed in InfluxDB:

```bash
docker exec bess-influxdb influx query '
from(bucket:"telemetry") |> range(start: -2d)
  |> filter(fn: (r) => r._measurement == "telemetry" and r._field == "battery_soc_pct")
  |> group() |> reduce(identity: {min: 100.0, max: 0.0, n: 0},
       fn: (r, accumulator) => ({min: if r._value < accumulator.min then r._value else accumulator.min,
                                 max: if r._value > accumulator.max then r._value else accumulator.max,
                                 n: accumulator.n + 1}))'
```

## Tests and CI

```bash
python -m unittest discover -s tests -v
```

On every push and PR, CI byte-compiles the sources, runs the tests,
validates both compose files and builds both images. A push to `main` then
redeploys each VM through its self-hosted runner. The runner syncs the
code, leaving `.env`, `certs/` and data alone, and runs
`docker compose up -d --build`.
