# File system sensor example

This stack runs [`cgen`](https://github.com/bdalpe/cgen) and a locally built
`prefect-sensor` container. Both containers mount the host's `files/` directory
at `/data`, so the sensor receives native filesystem notifications when `cgen`
or a host process changes that directory.

## Prerequisites

- Docker with Compose v2
- A Prefect Cloud workspace API URL and API key

Copy the environment template and replace the Prefect placeholder values:

```bash
cp .env.example .env
```

The workspace API URL has the form
`https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>`.
Keep `.env` local because it contains your Prefect API key.

## Run the example

From this directory, build the sensor from the repository checkout and start
the stack:

```bash
docker compose up --build
```

The sensor container starts before `cgen`. Once running, `cgen` appends a sample
report every second to `files/cgen-events.log`. In Prefect Cloud, filter the
event feed for `sensor.filesystem.file.created` and
`sensor.filesystem.file.modified`. The event payload's path is `/data/cgen-events.log`,
which is the path inside the sensor container; the same file is available on
the host as `files/cgen-events.log`.

The shared directory also makes it easy to demonstrate move and delete events
without disturbing the generated log:

```bash
printf 'new report\n' > files/report.txt
mv files/report.txt files/report.done
rm files/report.done
```

These operations emit `sensor.filesystem.file.created`,
`sensor.filesystem.file.modified`, `sensor.filesystem.file.moved`, and
`sensor.filesystem.file.deleted`. Watchdog can emit more than one modified event
for a single write because operating systems report filesystem operations at a
lower level than application writes.

Stop the services with `Ctrl-C`. To remove the containers and saved sensor
state:

```bash
docker compose down -v
```

The `sensor-state` volume retains the sensor's high-water mark across normal
restarts. If files change while the sensor is stopped, it emits catch-up
`sensor.filesystem.file.created` events with `payload.catchup: true` for files
newer than that mark. Removing the volume resets this state. The bind-mounted
`files/cgen-events.log` remains on the host after Compose stops; remove it
manually to reset the generated data.
