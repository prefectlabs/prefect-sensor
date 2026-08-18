---
title: Docker
---

# Docker

A pre-built container image is published at `ghcr.io/prefectlabs/prefect-sensor`, built from the [`Dockerfile`](https://github.com/prefectlabs/prefect-sensor/blob/main/Dockerfile) in the repo root. The image's `ENTRYPOINT` is the `prefect-sensor` CLI, so any arguments after the image name are passed straight through.

## Image tags

| Tag | Use it for |
| --- | --- |
| `:latest` | Most recent release — convenient for trying things out. |
| `:<version>` (e.g. `:0.1.0`) | Pin to a specific release in production. |
| `:main` | Built from the `main` branch — unstable; only use it knowingly. |

Pull a specific tag explicitly:

```bash
docker pull ghcr.io/prefectlabs/prefect-sensor:0.1.0
```

## Run with a config file

Mount your `sensor.yaml` into the container and tell `prefect-sensor` where to find it. The convention used throughout this guide is to mount the config at `/config/sensor.yaml`:

```bash
docker run --rm \
  -v $(pwd)/sensor.yaml:/config/sensor.yaml \
  ghcr.io/prefectlabs/prefect-sensor \
  start --config /config/sensor.yaml
```

To inspect what a config would start without running it, swap `start` for `list`:

```bash
docker run --rm \
  -v $(pwd)/sensor.yaml:/config/sensor.yaml \
  ghcr.io/prefectlabs/prefect-sensor \
  list --config /config/sensor.yaml
```

## Environment variables

The YAML's `env()` interpolation reads from the container's environment, so any secret referenced in your config needs to be present at runtime. Pass them with `-e` (the bare form forwards the value from your shell):

```bash
docker run --rm \
  -v $(pwd)/sensor.yaml:/config/sensor.yaml \
  -e SFTP_PASSWORD \
  -e PREFECT_API_KEY \
  -e PREFECT_API_URL \
  ghcr.io/prefectlabs/prefect-sensor \
  start --config /config/sensor.yaml
```

`PREFECT_API_KEY` and `PREFECT_API_URL` are required to route observations into Prefect Cloud (or a self-hosted Prefect server). The sensor calls `prefect.events.emit_event`, which uses these variables to authenticate and pick the API endpoint.

For larger sets of variables, use `--env-file`:

```bash
docker run --rm --env-file .env \
  -v $(pwd)/sensor.yaml:/config/sensor.yaml \
  ghcr.io/prefectlabs/prefect-sensor \
  start --config /config/sensor.yaml
```

## docker-compose

To run the sensor as a long-lived service:

```yaml
services:
  prefect-sensor:
    image: ghcr.io/prefectlabs/prefect-sensor:latest
    command: start --config /config/sensor.yaml
    volumes:
      - ./sensor.yaml:/config/sensor.yaml:ro
    environment:
      PREFECT_API_KEY: ${PREFECT_API_KEY}
      PREFECT_API_URL: ${PREFECT_API_URL}
      SFTP_PASSWORD: ${SFTP_PASSWORD}
    restart: unless-stopped
```

For an end-to-end SFTP example, use
[`examples/sftp/`](https://github.com/prefectlabs/prefect-sensor/tree/main/examples/sftp).
Its Compose stack runs an OpenSSH/SFTP server and a locally built sensor image
connected to Prefect Cloud. `cgen` continuously writes sample data into its
mounted `upload/` directory so appeared and changed events are produced
automatically; removing a file demonstrates the removed event.

For an end-to-end local filesystem example, use
[`examples/filesystem/`](https://github.com/prefectlabs/prefect-sensor/tree/main/examples/filesystem).
Its Compose stack bind-mounts the same host directory into `cgen` and the sensor
containers. Writes from `cgen`, as well as files created, renamed, or removed on
the host, produce native filesystem events without an intermediate service.

For an end-to-end Kafka example, use
[`examples/kafka/`](https://github.com/prefectlabs/prefect-sensor/tree/main/examples/kafka).
Its Compose stack runs Redpanda, Redpanda Console, `cgen` as the Kafka producer,
and a locally built sensor image connected to Prefect Cloud.

## Building locally

When iterating on a custom sensor or change that isn't published yet, build the image from a checkout:

```bash
docker build -t prefect-sensor:dev .
docker run --rm prefect-sensor:dev --help
```

The local build follows the same `uv sync --frozen` pattern as the published image, so dependency resolution is reproducible from `uv.lock`.
