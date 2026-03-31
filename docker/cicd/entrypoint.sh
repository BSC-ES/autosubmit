#!/bin/bash
set -e

USER_ID=$(id -u)
GROUP_ID=$(id -g)

if [ "$USER_ID" != "0" ]; then
    if ! getent passwd "$USER_ID" >/dev/null; then
        DYNAMIC_HOME="${HOME:-/tmp/userhome}"
        mkdir -p "$DYNAMIC_HOME"
        echo "dynamic:x:${USER_ID}:${GROUP_ID}:Dynamic User:${DYNAMIC_HOME}:/bin/bash" >> /etc/passwd
        echo "dynamic:x:${GROUP_ID}:" >> /etc/group
        export HOME="$DYNAMIC_HOME"
    fi

    export USER=dynamic
    export LOGNAME=dynamic

    # Fix: Ensure .local/bin exists and is in the PATH
    mkdir -p "$HOME/.local/bin"
    export PATH="$HOME/.local/bin:$PATH"

    # THE MAGIC FIX:
    # Symlink the 'autosubmit' executable to a global path.
    # Since we are not root, we can't write to /usr/local/bin directly
    # UNLESS we prepared it in the Dockerfile. (See step 2 below)
    ln -sf "$HOME/.local/bin/autosubmit" /usr/local/bin/autosubmit || true

    export PYTEST_ADDOPTS="-o cache_dir=/tmp/.pytest_cache"
    export MPLCONFIGDIR=/tmp/matplotlib-$(date +%s)
    mkdir -p "$MPLCONFIGDIR"
fi

exec tini -s -g -- "$@"