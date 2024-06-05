#!/bin/bash

# Execute /load_ssh_private_key.sh
/load_ssh_private_key.sh

# Run jupyter lab as daemon and assign token if env variable exists
if [ -n "$JUPYTER_TOKEN" ]; then
    jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.base_url=/jupyterlab --NotebookApp.token=$JUPYTER_TOKEN &
else
    jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.base_url=/jupyterlab &
fi

# Run the command passed by docker run
autosubmit_api start -b 0.0.0.0:8000
