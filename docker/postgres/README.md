# Docker compose for Autosubmit and Postgres

The Docker compose file for Autosubmit and Postgres is used
to run Autosubmit and Postgres in a containerised environment.

TODO: More info...

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
