# Postgres multiple schemas performance tests

We used the scripts in this directory to test the performance of
Postgres managing multiple schemas. Ref: https://github.com/BSC-ES/autosubmit/issues/3429

This was tested on this VM:

```bash
$ cat /proc/version
Linux version 5.14.0-611.49.1.el9_7.x86_64 (mockbuild@iad1-prod-build001.bld.equ.rockylinux.org) (gcc (GCC) 11.5.0 20240719 (Red Hat 11.5.0-11), GNU ld version 2.35.2-67.el9_7.1) #1 SMP PREEMPT_DYNAMIC Thu Apr 23 13:13:41 UTC 2026
$ free -h
               total        used        free      shared  buff/cache   available
Mem:            15Gi       1.4Gi        10Gi       316Mi       4.0Gi        14Gi
Swap:             0B          0B          0B
$ cat /proc/cpuinfo 
processor	: 0
vendor_id	: GenuineIntel
cpu family	: 6
model		: 85
model name	: Intel Xeon Processor (Skylake, IBRS)
stepping	: 4
microcode	: 0x1
cpu MHz		: 2095.078
cache size	: 16384 KB
physical id	: 0
siblings	: 1
core id		: 0
cpu cores	: 1
apicid		: 0
initial apicid	: 0
fpu		: yes
fpu_exception	: yes
cpuid level	: 13
wp		: yes
flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ss syscall nx pdpe1gb rdtscp lm constant_tsc rep_good nopl xtopology cpuid tsc_known_freq pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm abm 3dnowprefetch cpuid_fault pti ssbd ibrs ibpb stibp fsgsbase tsc_adjust bmi1 hle avx2 smep bmi2 erms invpcid rtm mpx avx512f avx512dq rdseed adx smap clflushopt clwb avx512cd avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves arat umip pku ospke md_clear arch_capabilities
bugs		: cpu_meltdown spectre_v1 spectre_v2 spec_store_bypass l1tf mds swapgs taa mmio_stale_data retbleed gds bhi ibpb_no_ret spectre_v2_user its
bogomips	: 4190.15
clflush size	: 64
cache_alignment	: 64
address sizes	: 46 bits physical, 48 bits virtual
power management:

processor	: 1
vendor_id	: GenuineIntel
cpu family	: 6
model		: 85
model name	: Intel Xeon Processor (Skylake, IBRS)
stepping	: 4
microcode	: 0x1
cpu MHz		: 2095.078
cache size	: 16384 KB
physical id	: 1
siblings	: 1
core id		: 0
cpu cores	: 1
apicid		: 1
initial apicid	: 1
fpu		: yes
fpu_exception	: yes
cpuid level	: 13
wp		: yes
flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ss syscall nx pdpe1gb rdtscp lm constant_tsc rep_good nopl xtopology cpuid tsc_known_freq pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm abm 3dnowprefetch cpuid_fault pti ssbd ibrs ibpb stibp fsgsbase tsc_adjust bmi1 hle avx2 smep bmi2 erms invpcid rtm mpx avx512f avx512dq rdseed adx smap clflushopt clwb avx512cd avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves arat umip pku ospke md_clear arch_capabilities
bugs		: cpu_meltdown spectre_v1 spectre_v2 spec_store_bypass l1tf mds swapgs taa mmio_stale_data retbleed gds bhi ibpb_no_ret spectre_v2_user its
bogomips	: 4190.15
clflush size	: 64
cache_alignment	: 64
address sizes	: 46 bits physical, 48 bits virtual
power management:

processor	: 2
vendor_id	: GenuineIntel
cpu family	: 6
model		: 85
model name	: Intel Xeon Processor (Skylake, IBRS)
stepping	: 4
microcode	: 0x1
cpu MHz		: 2095.078
cache size	: 16384 KB
physical id	: 2
siblings	: 1
core id		: 0
cpu cores	: 1
apicid		: 2
initial apicid	: 2
fpu		: yes
fpu_exception	: yes
cpuid level	: 13
wp		: yes
flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ss syscall nx pdpe1gb rdtscp lm constant_tsc rep_good nopl xtopology cpuid tsc_known_freq pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm abm 3dnowprefetch cpuid_fault pti ssbd ibrs ibpb stibp fsgsbase tsc_adjust bmi1 hle avx2 smep bmi2 erms invpcid rtm mpx avx512f avx512dq rdseed adx smap clflushopt clwb avx512cd avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves arat umip pku ospke md_clear arch_capabilities
bugs		: cpu_meltdown spectre_v1 spectre_v2 spec_store_bypass l1tf mds swapgs taa mmio_stale_data retbleed gds bhi ibpb_no_ret spectre_v2_user its
bogomips	: 4190.15
clflush size	: 64
cache_alignment	: 64
address sizes	: 46 bits physical, 48 bits virtual
power management:

processor	: 3
vendor_id	: GenuineIntel
cpu family	: 6
model		: 85
model name	: Intel Xeon Processor (Skylake, IBRS)
stepping	: 4
microcode	: 0x1
cpu MHz		: 2095.078
cache size	: 16384 KB
physical id	: 3
siblings	: 1
core id		: 0
cpu cores	: 1
apicid		: 3
initial apicid	: 3
fpu		: yes
fpu_exception	: yes
cpuid level	: 13
wp		: yes
flags		: fpu vme de pse tsc msr pae mce cx8 apic sep mtrr pge mca cmov pat pse36 clflush mmx fxsr sse sse2 ss syscall nx pdpe1gb rdtscp lm constant_tsc rep_good nopl xtopology cpuid tsc_known_freq pni pclmulqdq ssse3 fma cx16 pcid sse4_1 sse4_2 x2apic movbe popcnt tsc_deadline_timer aes xsave avx f16c rdrand hypervisor lahf_lm abm 3dnowprefetch cpuid_fault pti ssbd ibrs ibpb stibp fsgsbase tsc_adjust bmi1 hle avx2 smep bmi2 erms invpcid rtm mpx avx512f avx512dq rdseed adx smap clflushopt clwb avx512cd avx512bw avx512vl xsaveopt xsavec xgetbv1 xsaves arat umip pku ospke md_clear arch_capabilities
bugs		: cpu_meltdown spectre_v1 spectre_v2 spec_store_bypass l1tf mds swapgs taa mmio_stale_data retbleed gds bhi ibpb_no_ret spectre_v2_user its
bogomips	: 4190.15
clflush size	: 64
cache_alignment	: 64
address sizes	: 46 bits physical, 48 bits virtual
power management:
$ df -h
Filesystem      Size  Used Avail Use% Mounted on
devtmpfs        4.0M     0  4.0M   0% /dev
tmpfs           7.7G  1.1M  7.7G   1% /dev/shm
tmpfs           3.1G  270M  2.9G   9% /run
/dev/vda5       999G   35G  965G   4% /
/dev/vda2       936M  382M  555M  41% /boot
/dev/vda1        99M  7.1M   92M   8% /boot/efi
overlay         999G   35G  965G   4% /var/lib/docker/overlay2/6234e39c921ac00ab042d38740e8795a0d978a3a25471b52fa9870d90b3f2ffd/merged
overlay         999G   35G  965G   4% /var/lib/docker/overlay2/13b45846113e8722cc0dc78ec7790e0c4da465cfed6422f0104f95bf53b13f68/merged
overlay         999G   35G  965G   4% /var/lib/docker/overlay2/93003744150c46c0d17ec037d1a04940db70f54294f13f7df2c029808ef807a1/merged
tmpfs           1.6G     0  1.6G   0% /run/user/1002
```

## Setup

### Setting up PostgreSQL 18.6

```bash
$ dnf module list postgresql
Last metadata expiration check: 0:02:13 ago on Wed Nov 18 09:28:42 2026.
Rocky Linux 9 - AppStream
Name                                                            Stream                                                      Profiles                                                               Summary                                                                                
postgresql                                                      15                                                          client, server [d]                                                     PostgreSQL server and client module                                                    
postgresql                                                      16                                                          client, server [d]                                                     PostgreSQL server and client module                                                    
postgresql                                                      18                                                          client, server [d]                                                     PostgreSQL server and client module                                                    

Hint: [d]efault, [e]nabled, [x]disabled, [i]nstalled
dnf module enable postgresql:18
Last metadata expiration check: 1:14:07 ago on Wed Nov 18 08:18:42 2026.
Dependencies resolved.
===========================================================================================================================================
 Package                          Architecture                    Version                           Repository                        Size
===========================================================================================================================================
Enabling module streams:
 postgresql                                                       18                                                                      

Transaction Summary
===========================================================================================================================================

Is this ok [y/N]: y
Complete!
dnf install postgresql-server postgresql-contrib
Last metadata expiration check: 1:14:17 ago on Wed Nov 18 08:18:42 2026.
Dependencies resolved.
===========================================================================================================================================
 Package                              Architecture        Version                                             Repository              Size
===========================================================================================================================================
Installing:
 postgresql-contrib                   x86_64              18.6-1.module+el9.8.0+40321+a908ab5d                appstream              1.0 M
 postgresql-server                    x86_64              18.6-1.module+el9.8.0+40321+a908ab5d                appstream              7.5 M
Upgrading:
 openssl                              x86_64              1:3.5.9-1.el9_9                                     baseos                 1.4 M
 openssl-libs                         x86_64              1:3.5.9-1.el9_9                                     baseos                 2.3 M
Installing dependencies:
 libxslt                              x86_64              1.1.34-16.el9                                       appstream              240 k
 openssl-fips-provider                x86_64              1:3.5.9-1.el9_9                                     baseos                 816 k
 postgresql                           x86_64              18.6-1.module+el9.8.0+40321+a908ab5d                appstream              2.0 M
 postgresql-private-libs              x86_64              18.6-1.module+el9.8.0+40321+a908ab5d                appstream              158 k
 uuid                                 x86_64              1.6.2-55.el9                                        appstream               56 k

Transaction Summary
===========================================================================================================================================
Install  7 Packages
Upgrade  2 Packages

Total download size: 15 M
Is this ok [y/N]: y
Downloading Packages:
(1/9): libxslt-1.1.34-16.el9.x86_64.rpm                                                                    1.5 MB/s | 240 kB     00:00    
(2/9): openssl-fips-provider-3.5.9-1.el9_9.x86_64.rpm                                                      2.9 MB/s | 816 kB     00:00    
(3/9): postgresql-contrib-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64.rpm                                  4.7 MB/s | 1.0 MB     00:00    
(4/9): postgresql-private-libs-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64.rpm                             1.5 MB/s | 158 kB     00:00    
(5/9): uuid-1.6.2-55.el9.x86_64.rpm                                                                        562 kB/s |  56 kB     00:00    
(6/9): postgresql-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64.rpm                                          3.2 MB/s | 2.0 MB     00:00    
(7/9): openssl-3.5.9-1.el9_9.x86_64.rpm                                                                    3.2 MB/s | 1.4 MB     00:00    
(8/9): openssl-libs-3.5.9-1.el9_9.x86_64.rpm                                                               4.8 MB/s | 2.3 MB     00:00    
(9/9): postgresql-server-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64.rpm                                   7.6 MB/s | 7.5 MB     00:00    
-------------------------------------------------------------------------------------------------------------------------------------------
Total                                                                                                      6.2 MB/s |  15 MB     00:02     
Running transaction check
Transaction check succeeded.
Running transaction test
Transaction test succeeded.
Running transaction
  Preparing        :                                                                                                                   1/1 
  Upgrading        : openssl-libs-1:3.5.9-1.el9_9.x86_64                                                                              1/11 
  Installing       : openssl-fips-provider-1:3.5.9-1.el9_9.x86_64                                                                     2/11 
  Installing       : postgresql-private-libs-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64                                              3/11 
  Installing       : postgresql-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64                                                           4/11 
  Installing       : uuid-1.6.2-55.el9.x86_64                                                                                         5/11 
  Installing       : libxslt-1.1.34-16.el9.x86_64                                                                                     6/11 
  Installing       : postgresql-contrib-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64                                                   7/11 
  Running scriptlet: postgresql-server-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64                                                    8/11 
  Installing       : postgresql-server-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64                                                    8/11 
  Running scriptlet: postgresql-server-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64                                                    8/11 
  Upgrading        : openssl-1:3.5.9-1.el9_9.x86_64                                                                                   9/11 
  Cleanup          : openssl-1:3.2.2-6.el9_5.1.x86_64                                                                                10/11 
  Cleanup          : openssl-libs-1:3.2.2-6.el9_5.1.x86_64                                                                           11/11 
  Running scriptlet: openssl-libs-1:3.2.2-6.el9_5.1.x86_64                                                                           11/11 
  Verifying        : openssl-fips-provider-1:3.5.9-1.el9_9.x86_64                                                                     1/11 
  Verifying        : libxslt-1.1.34-16.el9.x86_64                                                                                     2/11 
  Verifying        : postgresql-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64                                                           3/11 
  Verifying        : postgresql-contrib-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64                                                   4/11 
  Verifying        : postgresql-private-libs-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64                                              5/11 
  Verifying        : postgresql-server-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64                                                    6/11 
  Verifying        : uuid-1.6.2-55.el9.x86_64                                                                                         7/11 
  Verifying        : openssl-1:3.5.9-1.el9_9.x86_64                                                                                   8/11 
  Verifying        : openssl-1:3.2.2-6.el9_5.1.x86_64                                                                                 9/11 
  Verifying        : openssl-libs-1:3.5.9-1.el9_9.x86_64                                                                             10/11 
  Verifying        : openssl-libs-1:3.2.2-6.el9_5.1.x86_64                                                                           11/11 

Upgraded:
  openssl-1:3.5.9-1.el9_9.x86_64                                    openssl-libs-1:3.5.9-1.el9_9.x86_64                                   
Installed:
  libxslt-1.1.34-16.el9.x86_64                                           openssl-fips-provider-1:3.5.9-1.el9_9.x86_64                     
  postgresql-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64                 postgresql-contrib-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64   
  postgresql-private-libs-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64    postgresql-server-18.6-1.module+el9.8.0+40321+a908ab5d.x86_64    
  uuid-1.6.2-55.el9.x86_64                                              

Complete!
$ psql --version
psql (PostgreSQL) 18.6
```

Create the user for the tests:

```bash
$ CREATE USER autosubmit WITH PASSWORD 'autosubmit';
$ ALTER USER autosubmit CREATEDB;
$ CREATE DATABASE autosubmit OWNER autosubmit;
```

### Installing other packages

These packages were installed to assist with monitoring and system
administration.

```bash
$ dnf install sysstat
$ dnf install time
$ dnf install bc
$ dnf update -y
$ dnf list available 'python3*'
$ dnf install python3.12 python3.12-pip python3.12-devel
$ dnf install postgresql-devel gcc gcc-c++ make
$ dnf install -y gcc gcc-c++ make
```

### Installing Autosubmit

```bash
$ python3.12 -m venv venv
$ source venv/bin/activate
$ git clone -o upstream https://github.com/BSC-ES/autosubmit.git
$ cd autosubmit
$ pip install -e .[postgres]
```

The base tests (T0) were created with the commit `58d578d405f84085140a3555f19637993c3afbab`,
from the 4.2.0 development (`master`).

## Reset data

The script `reset_postgres.sh` resets the Postgres configuration and data
directories back to their original state. This script is used between tests.

```bash
$ CONFIRM_RESET=YES sudo ./scripts/reset_postgres.sh 
================================================
PostgreSQL cluster reset
================================================
Service: postgresql
Data directory: /var/lib/pgsql/data

[1/7] Stopping PostgreSQL...
PostgreSQL stopped.

[2/7] Removing existing cluster...
Existing cluster removed.

[3/7] Initialising fresh PostgreSQL cluster...
The files belonging to this database system will be owned by user "postgres".
This user must also own the server process.

The database cluster will be initialized with locale "C.UTF-8".
The default database encoding has accordingly been set to "UTF8".
The default text search configuration will be set to "english".

Data page checksums are enabled.

fixing permissions on existing directory /var/lib/pgsql/data ... ok
creating subdirectories ... ok
selecting dynamic shared memory implementation ... posix
selecting default "max_connections" ... 100
selecting default "shared_buffers" ... 128MB
selecting default time zone ... UTC
creating configuration files ... ok
running bootstrap script ... ok
performing post-bootstrap initialization ... ok
syncing data to disk ... ok

initdb: warning: enabling "trust" authentication for local connections
initdb: hint: You can change this by editing pg_hba.conf or using the option -A, or --auth-local and --auth-host, the next time you run initdb.

Success. You can now start the database server using:

    /bin/pg_ctl -D /var/lib/pgsql/data -l logfile start


[4/7] Starting PostgreSQL...
Waiting for PostgreSQL...
PostgreSQL is ready.

[5/7] Creating Autosubmit PostgreSQL user...
CREATE ROLE
ALTER ROLE
Autosubmit user created.

[6/7] Creating Autosubmit database...
Autosubmit database created.

[7/7] Verifying installation...
                                                    version                                                    
---------------------------------------------------------------------------------------------------------------
 PostgreSQL 18.6 on x86_64-redhat-linux-gnu, compiled by gcc (GCC) 11.5.0 20240719 (Red Hat 11.5.0-14), 64-bit
(1 row)


PostgreSQL roles:
                              List of roles
 Role name  |                         Attributes                         
------------+------------------------------------------------------------
 autosubmit | Create DB
 postgres   | Superuser, Create role, Create DB, Replication, Bypass RLS


Databases:
                                                   List of databases
    Name    |   Owner    | Encoding | Locale Provider | Collate |  Ctype  | Locale | ICU Rules |   Access privileges   
------------+------------+----------+-----------------+---------+---------+--------+-----------+-----------------------
 autosubmit | autosubmit | UTF8     | libc            | C.UTF-8 | C.UTF-8 |        |           | 
 postgres   | postgres   | UTF8     | libc            | C.UTF-8 | C.UTF-8 |        |           | 
 template0  | postgres   | UTF8     | libc            | C.UTF-8 | C.UTF-8 |        |           | =c/postgres          +
            |            |          |                 |         |         |        |           | postgres=CTc/postgres
 template1  | postgres   | UTF8     | libc            | C.UTF-8 | C.UTF-8 |        |           | =c/postgres          +
            |            |          |                 |         |         |        |           | postgres=CTc/postgres
(4 rows)


Autosubmit database connection test:
 current_user | current_database 
--------------+------------------
 autosubmit   | autosubmit
(1 row)


Initial disk consumption:
44M	/var/lib/pgsql/data

================================================
PostgreSQL reset completed successfully.
================================================
```

## Run your tests

### Baseline tests (T0-PG, T0-AS)

The baseline tests is performed on the Postgres machine. It is used
just to take a “snapshot” of the virtual machine state before the tests
are executed.

**T0-PG** is the baseline Postgres test. It is executed with a fresh
PostgreSQL 18.6, no Autosubmit databases.

**T0-AS** is the baseline Autosubmit test. It is executed after T0-PG,
without any Autosubmit experiments or schemas, and after `autosubmit install`
has been executed.

We first collect the baseline without Autosubmit.

```bash
$ sudo DBNAME=postgres ./scripts/collect_metrics.sh T0-PG
================================================
Autosubmit PostgreSQL benchmark
================================================
Label: T0-PG
Database: postgres
Timestamp: 20261123_144937
Output: /root/scripts/../results/T0-PG
Collecting: database_sizes
Collecting: catalog_sizes
Collecting: catalog_counts
Collecting: relation_counts
Collecting: schema_counts
Collecting: system_table_statistics
Collecting: user_table_statistics
Collecting: database_statistics
Collecting: io_statistics
Collecting: connections
Collecting: postgresql_settings

================================================
Metrics collection completed.
Results: /root/scripts/../results/T0-PG
================================================
```

Now, we configure Autosubmit to use the local PostgreSQL installation.

```bash
cat <<'EOF' > /root/.autosubmitrc
[local]
path = /root/autosubmit/experiments/

[globallogs]
path = /root/autosubmit/logs

[structures]
path = /root/autosubmit/metadata/structures

[historicdb]
path = /root/autosubmit/metadata/data

[historiclog]
path = /root/autosubmit/metadata/logs

[database]
backend = postgres
connection_url = postgresql://autosubmit:autosubmit@localhost:5432/autosubmit
path = /root/autosubmit/database
EOF
```

```bash
$ autosubmit install
Autosubmit v4.2.0

Command traceability:
  Command: /root/venv/bin/autosubmit install
  Host: bsc-es-services-1.novalocal
  User: root
  PID: 824168
  Python: 3.12.16
  Platform: Linux-5.14.0-611.49.1.el9_7.x86_64-x86_64-with-glibc2.34
  Timezone: UTC (UTC+00:00)
  Working directory: /root

Autosubmit directory structure has been initialized successfully:
  /root/autosubmit/experiments
  /root/autosubmit/logs
  /root/autosubmit/metadata/structures
  /root/autosubmit/metadata/data
  /root/autosubmit/metadata/logs
  /root/autosubmit/database

Creating autosubmit Postgres database...
Autosubmit database created successfully
```

And now run a new test to create a new snapshot.

```bash
$ sudo DBNAME=autosubmit ./scripts/collect_metrics.sh T0-AS
================================================
Autosubmit PostgreSQL benchmark
================================================
Label: T0-AS
Database: autosubmit
Timestamp: 20261123_145721
Output: /root/scripts/../results/T0-AS
Collecting: database_sizes
Collecting: catalog_sizes
Collecting: catalog_counts
Collecting: relation_counts
Collecting: schema_counts
Collecting: system_table_statistics
Collecting: user_table_statistics
Collecting: database_statistics
Collecting: io_statistics
Collecting: connections
Collecting: postgresql_settings

================================================
Metrics collection completed.
Results: /root/scripts/../results/T0-AS
================================================
```

### Test with 1 experiment (T1)

First, we create a new experiment.

```bash
$ autosubmit expid -H MARENOSTRUM5 -d "postgres schemas"
```

That will create the default experiment, with the dummy platform. Modify
the jobs to include a wrapper.

```yaml
WRAPPERS:
  W1: 
    TYPE: "horizontal"
    JOBS_IN_WRAPPER:
      - SIM
```

Add the minimum necessary parameters for wrappers to the platform.

```yaml
PLATFORMS:
  MARENOSTRUM5:
    # ...
    PROCESSORS_PER_NODE: 112
    MAX_PROCESSORS: 112
```

Run `create -cw`, `inspect -cw`, verify everything looks correct, and run
`autosubmit run`. Wait for the experiment to complete, and for the remote jobs
to complete.

Now, extract T1 metrics.

```bash
$ sudo DBNAME=autosubmit ./scripts/collect_metrics.sh T1
```

### Test with 10 experiments (T2)

Copy the `a000` experiment 9 more times, to have 10 experiments.

```bash
$ ./scripts/clone_experiments.sh a000 10
```

Then, extract T2 metrics.

```bash
$ sudo DBNAME=autosubmit ./scripts/collect_metrics.sh T2
```

### Test with 100 experiments (T3)

```bash
$ ./scripts/clone_experiments.sh a000 100
$ sudo DBNAME=autosubmit ./scripts/collect_metrics.sh T3
```

### Test with 1000 experiments (T4)

```bash
$ ./scripts/clone_experiments.sh a000 1000
$ sudo DBNAME=autosubmit ./scripts/collect_metrics.sh T4
```

### Test with 5000 experiments (T5)

```bash
$ ./scripts/clone_experiments.sh a000 5000
$ sudo DBNAME=autosubmit ./scripts/collect_metrics.sh T5
```

### Test with 10000 experiments (T6)

```bash
$ ./scripts/clone_experiments.sh a000 10000
$ sudo DBNAME=autosubmit ./scripts/collect_metrics.sh T6
```

## Results

Run `summarise.sh` to get a quick summary Markdown with plots.

```bash
./scripts/summarise.sh 
================================================
Autosubmit PostgreSQL benchmark
Results summarisation
================================================

Project:  /home/kinow/Development/python/workspace/autosubmit/scripts/postgres_schemas_perf
Results:  /home/kinow/Development/python/workspace/autosubmit/scripts/postgres_schemas_perf/results
Output:   /home/kinow/Development/python/workspace/autosubmit/scripts/postgres_schemas_perf/results/summary

Reading benchmark results...
  Processing T0-PG (0 experiments)
  Processing T0-AS (0 experiments)
  Processing T1 (1 experiments)
  Processing T2 (10 experiments)
  Processing T3 (100 experiments)
  Processing T4 (1,000 experiments)
  Processing T5 (5,000 experiments)
  Skipping T6: directory not found

Writing consolidated CSV files...
Generating plots...
Generating Markdown report...

================================================
Summarisation completed
================================================

Tests processed: T0-PG, T0-AS, T1, T2, T3, T4, T5

Tables:  /home/kinow/Development/python/workspace/autosubmit/scripts/postgres_schemas_perf/results/summary/tables
Plots:   /home/kinow/Development/python/workspace/autosubmit/scripts/postgres_schemas_perf/results/summary/plots
Report:  /home/kinow/Development/python/workspace/autosubmit/scripts/postgres_schemas_perf/results/summary/report/summary.md

================================================
Done.
================================================
```

