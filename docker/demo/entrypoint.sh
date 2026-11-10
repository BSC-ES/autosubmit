#!/bin/bash

# Prepare Autosubmit
autosubmit configure $( [ -n "$AS_PG_CONN_URL" ] && echo "--database-backend=postgres --database-conn-url=$AS_PG_CONN_URL" )
autosubmit install

# Execute /load_ssh_private_key.sh
/load_ssh_private_key.sh

# Run jupyter lab as daemon and assign token if env variable exists
jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.base_url=/jupyterlab $( [ -n "$JUPYTER_TOKEN" ] && echo "--NotebookApp.token=$JUPYTER_TOKEN" ) &

# Run the command passed by docker run
/apps/autosubmit-api/bin/autosubmit_api start -b 0.0.0.0:8000
