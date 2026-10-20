# Docker compose for Autosubmit and Postgres

The Docker compose file for Autosubmit and Postgres is used
to run Autosubmit and Postgres in a containerised environment.

This setup provides a local development environment for
Autosubmit using Docker Compose. It runs two services:

- Autosubmit: Builds the development image, installs Autosubmit in
  editable mode from the local source code, and initialises its required
  directories and database tables.
- PostgreSQL: Runs PostgreSQL 16 and provides the database used by Autosubmit.
  The database is persisted in a Docker volume.

## Running the Environment

Start the environment using the provided `run.sh` script:

```bash
$ run.sh
```

The script builds the Autosubmit image and starts both services in the
foreground. Press Ctrl+C to stop the foreground process.

The Docker Compose configuration mounts the local source code into `/app`,
allowing changes to the source code to be tested without rebuilding the image.
The configuration file is mounted at `/etc/autosubmitrc`, while experiment
data, logs, and metadata are stored in local directories.

PostgreSQL is also exposed on `localhost:5432`, allowing connections from
database clients running on the host. You can configure this in your database
client (e.g., PyCharm databases).

> [!NOTE]
> If you have another service already listening on port 5432, you can change
> the port in the `docker-compose.yml` file. You can also $ POSTGRES_HOST_PORT=5433 ./run.sh

## Testing Autosubmit

You can start `bash` inside the `autosubmit Docker Compose service.

```bash
$ docker compose exec autosubmit /bin/bash
```

And to verify Autosubmit was installed correctly, you can issue a `describe` command.
It should report that there are no experiments, without issuing any errors.

```bash
(base) autosubmit@a8728f5ed60b:/app$ autosubmit describe
Autosubmit v4.2.0

Command traceability:
  Command: /opt/conda/bin/autosubmit describe
  Host: a8728f5ed60b
  User: autosubmit
  PID: 77
  Python: 3.10.20
  Platform: Linux-7.0.0-38-generic-x86_64-with-glibc2.36
  Timezone: UTC (UTC+00:00)
  Working directory: /app

[WARNING] No experiments found for expid=* and user *
```

## Testing Postgres

You can start the `psql` client to connect to the Postgres database in the `postgres`
Docker Compose service.

```bash
$ docker compose  exec postgres psql -U autosubmit -d autosubmit
psql (16.15 (Debian 16.15-1.pgdg13+2))
Type "help" for help.

autosubmit=# 
```

```bash
autosubmit=# \conninfo
You are connected to database "autosubmit" as user "autosubmit" via socket in "/var/run/postgresql" at port "5432".
autosubmit=# \dt
            List of relations
 Schema |    Name    | Type  |   Owner    
--------+------------+-------+------------
 public | db_version | table | autosubmit
 public | details    | table | autosubmit
 public | experiment | table | autosubmit
(3 rows)

autosubmit=# \dn
      List of schemas
  Name  |       Owner       
--------+-------------------
 public | pg_database_owner
(1 row)

autosubmit=# SELECT version();
                                                       version                                                        
----------------------------------------------------------------------------------------------------------------------
 PostgreSQL 16.15 (Debian 16.15-1.pgdg13+2) on x86_64-pc-linux-gnu, compiled by gcc (Debian 14.2.0-19) 14.2.0, 64-bit
(1 row)
```

## Other

Other useful commands:

```bash
# Stop and remove volumes
$ docker compose down --volumes
$
$ # Check the Autosubmit installation and version
$ docker compose exec autosubmit autosubmit --version
$
$ # Start a shell session within the Autosubmit container
$ docker compose exec autosubmit /bin/bash
$
$ # Start a psql session within the PostgreSQL container
$ docker compose exec postgres psql -U autosubmit -d autosubmit
```
