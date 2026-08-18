# Kafka sensor example

This stack runs a single-node Redpanda broker, Redpanda Console, `cgen`, and a
locally built `prefect-sensor` container. `cgen` publishes a sample order every
second and the sensor emits each Kafka record to Prefect Cloud as
`sensor.kafka.message.received`.

## Prerequisites

- Docker with Compose v2
- A Prefect Cloud workspace API URL and API key

Copy the environment template and replace both placeholder values:

```bash
cp .env.example .env
```

The workspace API URL has the form
`https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>`.
Keep `.env` local; it contains your Prefect API key.

## Run the example

From this directory, start the stack and build the sensor from the repository
checkout:

```bash
docker compose up --build
```

Open [Redpanda Console](http://localhost:8080) and inspect the
`prefect-sensor-demo` topic to see the source records. In Prefect Cloud, filter
the event feed for `sensor.kafka.message.received`. Event payloads include the
topic, partition, offset, decoded key, and decoded value.

Stop the services with `Ctrl-C`. To remove the containers, broker data, and
saved sensor offsets:

```bash
docker compose down -v
```

The Compose file requires both Prefect variables and exits with a configuration
error when either is missing. The first run uses `auto_offset_reset: earliest`;
later runs resume from the consumer group's broker commit, with the named
`sensor-state` volume retained as a local fallback snapshot.
