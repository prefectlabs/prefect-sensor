# SFTP sensor example

This stack runs an OpenSSH/SFTP server, [`cgen`](https://github.com/bdalpe/cgen),
and a locally built `prefect-sensor` container. `cgen` writes generated report
events to the server's `/upload` directory, and the sensor emits file lifecycle
events to Prefect Cloud.

## Prerequisites

- Docker with Compose v2
- A Prefect Cloud workspace API URL and API key

Copy the environment template and replace the Prefect placeholder values:

```bash
cp .env.example .env
```

The workspace API URL has the form
`https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>`.
The included SFTP username and password are for this local example only. Keep
`.env` local because it contains your Prefect API key.

## Run the example

From this directory, start the SFTP server and `cgen`, then build the sensor
from the repository checkout:

```bash
docker compose up --build
```

The sensor waits for the SFTP port to accept connections before it starts.
`cgen` writes a sample report every second to `upload/cgen-events.log`, so the
sensor first emits `sensor.sftp.file.appeared` and then emits
`sensor.sftp.file.changed` as the file grows. The included `upload/test2.txt`
also produces an `appeared` event on the first run.

The host's `upload/` directory is shared by `cgen` and the SFTP server at
`/upload`. To demonstrate a removal independently of the generated file, add a
temporary file, wait for the next sensor poll, then remove it:

```bash
printf 'new report\n' > upload/report.txt
# Wait at least five seconds for file.appeared.
rm upload/report.txt
```

Wait at least five more seconds for `sensor.sftp.file.removed`. Together with
the continuously growing `cgen-events.log`, this demonstrates
`sensor.sftp.file.appeared`, `sensor.sftp.file.changed`, and
`sensor.sftp.file.removed` in Prefect Cloud.

Stop the services with `Ctrl-C`. To remove the containers and saved sensor
state:

```bash
docker compose down -v
```

The `sensor-state` volume retains the latest observed modification time across
normal restarts, preventing existing files from being emitted as newly appeared
again. Removing the volume resets that state. The generated
`upload/cgen-events.log` file is bind-mounted from the host and remains after
Compose stops; remove it manually if you want to reset the generated data.
