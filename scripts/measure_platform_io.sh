#!/usr/bin/env bash
#
# Measure the process, file and network I/O Autosubmit performs during a run.
#
#   ./scripts/measure_platform_io.sh <expid>
#   ./scripts/measure_platform_io.sh --summarise <trace-file>
#
# Requires autosubmit on PATH, an experiment already created, and either
# bpftrace (needs CAP_BPF or root) or strace. Results are written under
# <LOCAL_ROOT_DIR>/<expid>/tmp/measure_<timestamp>/. See
# docs/source/devguide/platforms/io_measurement.rst for interpretation.

set -euo pipefail

summarise() {
    local trace="$1" expid="${2:-unknown}" ts="${3:-unknown}"

    [ -r "$trace" ] || { echo "Cannot read trace file: $trace" >&2; return 1; }

    local size
    size="$(du -h "$trace" | cut -f1)"

    awk -v expid="$expid" -v ts="$ts" -v trace="$trace" -v size="$size" '
        function comma(n,   s, r) {
            s = sprintf("%d", n)
            while (length(s) > 3) {
                r = "," substr(s, length(s) - 2) r
                s = substr(s, 1, length(s) - 3)
            }
            return s r
        }
        function bytes(b) {
            if (b >= 1073741824) return sprintf("~%.1f GB", b / 1073741824)
            if (b >= 1022976)    return sprintf("~%.0f MB", b / 1048576)
            if (b >= 1024)       return sprintf("~%.0f KB", b / 1024)
            return sprintf("%d B", b)
        }
        function base(p,   n, a) {
            n = split(p, a, "/")
            return a[n]
        }
        function row(metric, count) {
            printf "  %-60s %s\n", metric, count
        }
        function rule() {
            printf "  %s\n", \
                "--------------------------------------------------------------------------------"
        }
        {
            syscalls++
            line = $0
            sub(/^[0-9]+ +/, "", line)

            if ((i = index(line, "execve(\"")) > 0) {
                p = substr(line, i + 8)
                p = substr(p, 1, index(p, "\"") - 1)
                execs[base(p)]++
                exec_total++
                next
            }

            if (index(line, "openat(") == 1) {
                i = index(line, ", \"")
                if (i > 0) {
                    p = substr(line, i + 3)
                    p = substr(p, 1, index(p, "\"") - 1)
                    opens++
                    b = base(p)
                    if (b ~ /\.db$/) { dbopens[b]++; db_total++ }
                    if (b ~ /_STAT_[0-9]+$/) seen_stat[b] = 1
                }
                next
            }

            if (index(line, "sin_port=htons(22)") > 0) { ssh_conn++; next }

            if (line ~ / = [0-9]+$/) {
                n = line
                sub(/.* = /, "", n)
                if (index(line, "read(") == 1)  { bytes_read += n;    next }
                if (index(line, "write(") == 1) { bytes_written += n; next }
            }
        }
        END {
            jobs = 0
            for (s in seen_stat) jobs++

            print "===================================================================="
            print "Autosubmit platform I/O measurement"
            print "===================================================================="
            printf "Experiment : %s\n", expid
            printf "Timestamp  : %s\n", ts
            printf "Trace file : %s (%s)\n", trace, size
            print ""
            printf "  %-60s %s\n", "Metric", "Count"
            rule()

            row("SSH connections opened", \
                ssh_conn > 0 ? comma(ssh_conn) : "0 (no SSH platform in use)")

            row("Subprocess launches (total)", \
                comma(exec_total) " across all binaries")

            je_desc = ""
            je_min = -1
            split("bash timeout printenv", jb, " ")
            for (k = 1; k <= 3; k++) {
                c = execs[jb[k]] + 0
                je_desc = je_desc (je_desc == "" ? "" : ", ") jb[k] " " comma(c)
                if (je_min < 0 || c < je_min) je_min = c
            }
            if (jobs > 0 && je_min > 0)
                je_desc = je_desc sprintf(" (~%d per job)", int(je_min / jobs + 0.5))
            row("Job-execution invocations (bash + timeout + printenv)", je_desc)

            row("Job-status check subprocesses (ps + grep)", \
                "ps " comma(execs["ps"] + 0) ", grep " comma(execs["grep"] + 0))

            row("Completion-marker sweeps (find)", comma(execs["find"] + 0))

            db_desc = comma(db_total)
            if (db_total > 0) {
                parts = ""
                for (d in dbopens)
                    parts = parts (parts == "" ? "" : " + ") comma(dbopens[d]) " " d
                db_desc = db_desc " (" parts ")"
            }
            row("Opens of local SQLite DBs", db_desc)

            row("Bytes written to disk", bytes(bytes_written))
            row("Bytes read from disk (includes subprocess-startup overhead)", \
                bytes(bytes_read))
            row("Total syscalls", comma(syscalls))
            rule()

            if (jobs > 0) printf "\n  Jobs detected in trace: %d\n", jobs
        }
    ' "$trace"
}

if [ $# -eq 2 ] && [ "$1" = "--summarise" ]; then
    summarise "$2" "(existing trace)" "$(date +%Y%m%d_%H%M%S)"
    exit 0
fi

if [ $# -ne 1 ]; then
    cat >&2 <<'USAGE'
Usage:
  measure_platform_io.sh <expid>              run and summarise
  measure_platform_io.sh --summarise <file>   re-summarise an existing trace
USAGE
    exit 2
fi

EXPID="$1"
TS="$(date +%Y%m%d_%H%M%S)"

LOCAL_ROOT_DIR="$(python3 -c 'from autosubmit.config.basicconfig import BasicConfig; BasicConfig.read(); print(BasicConfig.LOCAL_ROOT_DIR)' 2>/dev/null || true)"
[ -n "$LOCAL_ROOT_DIR" ] || LOCAL_ROOT_DIR="${HOME}/autosubmit"

EXPID_DIR="${LOCAL_ROOT_DIR}/${EXPID}"
if [ ! -d "$EXPID_DIR" ]; then
    echo "Experiment directory not found: ${EXPID_DIR}" >&2
    echo "Create the experiment first (autosubmit expid ... && autosubmit create ${EXPID})." >&2
    exit 4
fi

OUTDIR="${EXPID_DIR}/tmp/measure_${TS}"
mkdir -p "$OUTDIR"

TRACE="${OUTDIR}/strace.log"
STDOUT_LOG="${OUTDIR}/autosubmit.stdout"
SUMMARY="${OUTDIR}/summary.txt"

if command -v bpftrace >/dev/null 2>&1 && bpftrace -e 'BEGIN { exit(); }' >/dev/null 2>&1; then
    TRACER="bpftrace"
elif command -v strace >/dev/null 2>&1; then
    TRACER="strace"
else
    echo "Neither bpftrace nor strace is available." >&2
    echo "Install one (Debian/Ubuntu: apt install bpftrace | apt install strace)." >&2
    exit 3
fi

echo "Tracer     : ${TRACER}"
echo "Experiment : ${EXPID}"
echo "Output     : ${OUTDIR}/"
echo

if [ "$TRACER" = "bpftrace" ]; then
    BPF_OUT="${OUTDIR}/bpftrace.out"
    bpftrace -o "${BPF_OUT}" -e '
        tracepoint:syscalls:sys_enter_execve { @execs[str(args->filename)] = count(); }
        tracepoint:syscalls:sys_enter_connect { @connects[comm] = count(); }
        tracepoint:syscalls:sys_enter_openat { @opens[comm] = count(); }
        tracepoint:syscalls:sys_exit_read  /args->ret > 0/ { @bytes_read[comm] = sum(args->ret); }
        tracepoint:syscalls:sys_exit_write /args->ret > 0/ { @bytes_written[comm] = sum(args->ret); }
    ' &
    BPF_PID=$!
    sleep 1

    autosubmit run "${EXPID}" > "${STDOUT_LOG}" 2>&1 || true

    kill -SIGINT "${BPF_PID}" 2>/dev/null || true
    wait "${BPF_PID}" 2>/dev/null || true

    {
        echo "===================================================================="
        echo "Autosubmit platform I/O measurement"
        echo "===================================================================="
        echo "Experiment : ${EXPID}"
        echo "Timestamp  : ${TS}"
        echo "Tracer     : bpftrace"
        echo
        cat "${BPF_OUT}"
    } | tee "${SUMMARY}"
else
    strace -f -y -yy -s 128 \
        -e trace=openat,connect,read,write,execve \
        -o "${TRACE}" \
        autosubmit run "${EXPID}" > "${STDOUT_LOG}" 2>&1 || true

    summarise "${TRACE}" "${EXPID}" "${TS}" | tee "${SUMMARY}"
fi

echo
echo "Summary         : ${SUMMARY}"
echo "Workflow output : ${STDOUT_LOG}"