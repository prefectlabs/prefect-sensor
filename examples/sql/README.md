# SQL sensor example

This stack runs PostgreSQL, [`cgen`](https://github.com/bdalpe/cgen), and a
locally built `prefect-sensor` container. `cgen` inserts a sample order every
second, and the SQL sensor emits each new row to Prefect Cloud as
`sensor.sql.row.detected`.

## Prerequisites

- Docker with Compose v2
- A Prefect Cloud workspace API URL and API key

Copy the environment template and replace both placeholder values:

```bash
cp .env.example .env
```

The workspace API URL has the form
`https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>`.
Keep `.env` local because it contains your Prefect API key. The PostgreSQL
credentials in the Compose and configuration files are for this local example
only; PostgreSQL is not exposed outside the Compose network.

## Run the example

From this directory, start PostgreSQL and `cgen`, then build the sensor from the
repository checkout:

```bash
docker compose up --build
```

Both `cgen` and the sensor wait for PostgreSQL to become healthy. `cgen` inserts
one of the configured sample orders every second. The sensor polls the `orders`
table every five seconds and tracks its generated `id` column as a high-water
mark. In Prefect Cloud, filter the event feed for `sensor.sql.row.detected`.
Each event payload contains the row's `id`, `order_id`, `status`, and `amount`.

Inspect the generated rows from another terminal:

```bash
docker compose exec postgres \
  psql -U sensor -d sensor -c "SELECT * FROM orders ORDER BY id DESC LIMIT 10;"
```

Stop the services with `Ctrl-C`. To remove the containers, generated database
rows, and saved sensor high-water mark:

```bash
docker compose down -v
```

The `postgres-data` volume retains generated rows across normal restarts, while
the `sensor-state` volume retains the last emitted `id`. Consequently, restarting
the stack does not replay previously emitted rows. Removing both volumes resets
the database and sensor state; PostgreSQL recreates the `orders` table from
`init.sql` on the next start.
