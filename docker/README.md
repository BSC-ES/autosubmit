## Prerequisites

This image has been tested on Ubuntu Linux on x86 architecture.
It requires Docker v24.10 or greater, and it is recommended to
give the container at least 1G of memory (although it may run fine
with less, depending on your workflows).

Disk space should be about 1.4G (also depends on your workflow tasks). 

You will need valid SSH keys to use within the Docker Compose
cluster, or if you want to use SSH with Docker. The `linuxserver.io`
image, used for the computing nodes, provides a keygen utility.

```bash
# Here you can choose 'rsa', for example.
$ docker run --rm -it --entrypoint /keygen.sh linuxserver/openssh-server
```

Copy the generated private key into `id_rsa` and the public key
into `id_rsa.pub`. Also copy the public key into `authorized_keys`.

## Build

```bash
$ export AUTOSUBMIT_VERSION=4.1.12
$ docker build \
  --build-arg AUTOSUBMIT_VERSION=${AUTOSUBMIT_VERSION} \
  -t ${USER}/autosubmit:${AUTOSUBMIT_VERSION}-bullseye-slim \
  -t ${USER}/autosubmit:latest \
  .
```

## Run

This image is useful for testing, or getting started with Autosubmit.
For a production installation, please consult the Autosubmit documentation.

The examples in this section show different ways to use this image.
For running experiments, SSH keys will have to be configured so that
the container running this image can SSH locally, and also to a remote
server (if there are remote jobs).

If you would like to persist logs, database, experiments, and
other metadata, you can bind volumes for:

- `/app/autosubmit/database` -- where the `autosubmit.db` will stay
- `/app/autosubmit/experiments` -- where the experiments, logs, and metadata will stay

There is also a `docker-compose.yml` file that builds a complete
client and server cluster for Autosubmit. The `autosubmit` node
can be used to submit jobs to the other nodes (as it is normally
configured with VM's submitting jobs to remote Slurm, PBS, etc.).

```bash
$ docker run --rm ${USER}/autosubmit:latest \
  autosubmit --version
```

### Use volumes

Create the directories to hold DB and experiments somewhere.

```bash
$ mkdir -pv /tmp/autosubmit/{database,experiments}
```

Create an external DB, for example:

```bash
$ docker run --rm \
  -v /tmp/autosubmit/database:/app/autosubmit/database \
  -v /tmp/autosubmit/experiments:/app/autosubmit/experiments \
  ${USER}/autosubmit:latest \
  autosubmit install
```

Create a dummy experiment:

```bash
$ docker run --rm \
  -v /tmp/autosubmit/database:/app/autosubmit/database \
  -v /tmp/autosubmit/experiments:/app/autosubmit/experiments \
  ${USER}/autosubmit:latest \
  autosubmit expid -H local -d test --dummy
```

Confirm any container created with the image can access the experiments:

```bash
$ docker run --rm \
  -v /tmp/autosubmit/database:/app/autosubmit/database \
  -v /tmp/autosubmit/experiments:/app/autosubmit/experiments \
  ${USER}/autosubmit:latest \
  autosubmit describe
```

To delete an experiment (use `-ti` if you do not pass `-f`):

```bash
$ docker run --rm \
  -v /tmp/autosubmit/database:/app/autosubmit/database \
  -v /tmp/autosubmit/experiments:/app/autosubmit/experiments \
  ${USER}/autosubmit:latest \
  autosubmit delete -f a000
```

### Run experiments

In order to run experiments, Autosubmit will need valid
SSH configuration so that it can submit the jobs to local
or remote hosts (always via `ssh`).

To start with SSH (see prerequisites note about SSH keys):

```bash
$ docker run --rm \
  -ti \
  -p 2222:22 \
  -e DISPLAY=$DISPLAY \
  -v $(pwd -P)/id_rsa:/home/autosubmit/.ssh/id_rsa \
  -v $(pwd -P)/id_rsa.pub:/home/autosubmit/.ssh/id_rsa.pub \
  -v $(pwd -P)/authorized_keys:/home/autosubmit/.ssh/authorized_keys \
  -v /tmp/.X11-unix/:/tmp/.X11-unix/ \
  ${USER}/autosubmit:latest /bin/bash
```

Once inside your container, you can run commands such as:

```bash
$ autosubmit expid -H local -d test --dummy
Autosubmit is running with 4.0.84
The new experiment "a000" has been registered.
Generating folder structure...
Experiment folder: /app/autosubmit//experiments/a000
Generating config files...
Experiment a000 created
$ autosubmit create a000
$ autosubmit run a000
```

It is important to know that this container uses `tini` as the init process
so that Autosubmit jobs do not become defunct (zombie) processes
inside the container.

Once you stop the container, all your jobs and logs will be
deleted if you have not used volumes.

### Docker Compose Example

You **must** have these three files created before running `docker compose`,
`id_rsa.pub`, `id_rsa`, and `authorized_keys`. See prerequisites.

Remember to set the correct permissions too, e.g.

```bash
$ chmod 600 id_rsa*
$ chmod 600 authorized_keys
```

Build the images:

```bash
$ docker compose build
```

Start the cluster:

```bash
$ docker compose up
```

`docker compose` supports `-d/--detach` to start in the background.
To stop the cluster, you can press `CTRL + C` (or your platform equivalent),
or run `docker compose stop`.

You can also increase the number of computing nodes, as in this example
where two are created.

```bash
$ docker compose up --force-recreate --build --scale computing_node=2
```

List the ports:

```bash
$ docker compose ps
NAME                           IMAGE                                       COMMAND                  SERVICE             CREATED             STATUS              PORTS
autosubmit                     dockerfiles-autosubmit                      "sudo /usr/sbin/sshd…"   autosubmit          33 minutes ago      Up 32 minutes       0.0.0.0:32783->22/tcp, :::32783->22/tcp
dockerfiles-computing_node-1   lscr.io/linuxserver/openssh-server:latest   "/init"                  computing_node      33 minutes ago      Up 32 minutes       2222/tcp, 0.0.0.0:32782->22/tcp, :::32782->22/tcp
dockerfiles-computing_node-2   lscr.io/linuxserver/openssh-server:latest   "/init"                  computing_node      33 minutes ago      Up 32 minutes       2222/tcp, 0.0.0.0:32781->22/tcp, :::32781->22/tcp
```

You can connect to the Autosubmit VM via Docker or SSH. The example below
uses Docker. Autosubmit will be loaded automatically via the Micromamba
Conda environment:

```bash
# Docker Compose container names may change, use docker ps
# to confirm yours is called autosubmit.
$ docker exec -ti autosubmit /bin/bash
```

For SSH, you have to use the randomly generated local TCP port that
is bound to the port `22` inside the container. Looking at the example above,
you could use:

```bash
$ ssh -i id_rsa -p 32791 autosubmit@localhost
Warning: Permanently added '[localhost]:32791' (ED25519) to the list of known hosts.
X11 forwarding request failed on channel 0
Linux b35e552f22bc 5.15.0-78-generic #85-Ubuntu SMP Fri Jul 7 15:25:09 UTC 2023 x86_64

The programs included with the Debian GNU/Linux system are free software;
the exact distribution terms for each program are described in the
individual files in /usr/share/doc/*/copyright.

Debian GNU/Linux comes with ABSOLUTELY NO WARRANTY, to the extent
permitted by applicable law.
Last login: Sun Aug  6 09:45:28 2023 from 172.21.0.1
```

From the Autosubmit container, you should be able to connect to the
computing nodes.

```bash
$ ssh autosubmit@dockerfiles-computing_node-1
Welcome to OpenSSH Server
5a29566b9914:~$ 
logout
Connection to dockerfiles-computing_node-1 closed.
$ ssh autosubmit@dockerfiles-computing_node-2
Welcome to OpenSSH Server
cff117cd9a67:~$ 
logout
Connection to dockerfiles-computing_node-2 closed.
```
