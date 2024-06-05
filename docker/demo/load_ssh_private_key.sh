#!/usr/bin/env bash

SSH_PATH=$HOME/.ssh
mkdir -p $SSH_PATH

# If VAULT_ADDR is not set, then we are not running in a container
if [ -z "${VAULT_ADDR}" ]
then
    # VAULT_ADDR is not set
    ssh-keygen -b 4096 -t rsa -f "${SSH_PATH}/id_rsa" -q -N ""
    SSH_PRIVATE_KEY=$(base64 "${SSH_PATH}/id_rsa" -w 0)
    SSH_PUBLIC_KEY=$(base64 "${SSH_PATH}/id_rsa.pub" -w 0)

else
    # Original script, assumes VAULT_ADDR is set
    echo "begin script"
    whoami
    set -euo pipefail

    SSH_PRIVATE_KEY=$(vault kv get -field=SSH_PRIVATE_KEY "${VAULT_MOUNT}/${VAULT_TOP_DIR}/autosubmit" || echo "")
    if [ -z "$SSH_PRIVATE_KEY" ]
    then
        ssh-keygen -b 4096 -t rsa -f "${SSH_PATH}/id_rsa" -q -N ""
        SSH_PRIVATE_KEY=$(base64 "${SSH_PATH}/id_rsa" -w 0)
        SSH_PUBLIC_KEY=$(base64 "${SSH_PATH}/id_rsa.pub" -w 0)
        vault kv put "${VAULT_MOUNT}/${VAULT_TOP_DIR}/autosubmit" SSH_PRIVATE_KEY="${SSH_PRIVATE_KEY}" SSH_PUBLIC_KEY="${SSH_PUBLIC_KEY}"
    else
        SSH_PUBLIC_KEY=$(vault kv get -field=SSH_PUBLIC_KEY "${VAULT_MOUNT}/${VAULT_TOP_DIR}/autosubmit" || echo "")
        echo $SSH_PRIVATE_KEY | base64 -d > "${SSH_PATH}/id_rsa"
        echo $SSH_PUBLIC_KEY | base64 -d > "${SSH_PATH}/id_rsa.pub"
    fi
fi

chmod 600  "${SSH_PATH}/id_rsa"