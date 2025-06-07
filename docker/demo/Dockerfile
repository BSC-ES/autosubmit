FROM inseefrlab/onyxia-jupyter-python:py3.10.12

# Avoid interactive stuff
ENV DEBIAN_FRONTEND=noninteractive
RUN  sudo ln -fs /usr/share/zoneinfo/America/New_York /etc/localtime &&\
     sudo apt update -y -q &&\
     sudo apt install wget curl python3 python3-tk python3-dev graphviz git subversion sqlite3 gpg lsb-release rsync -y -q &&\
     sudo apt install build-essential libssl-dev libffi-dev -y -q &&\
     sudo rm -rf /var/lib/apt/lists/*

# Install Autosubmit
ARG AUTOSUBMIT_VERSION=4.1.14
ARG AUTOSUBMIT_GIT_REF
ARG AUTOSUBMIT_API_VERSION=4.1.1
ARG AUTOSUBMIT_API_GIT_REF

RUN pip3 install --upgrade pip &&\
    pip3 install jupyterlab \
    $( [ -n "${AUTOSUBMIT_GIT_REF}" ] && echo "git+https://github.com/BSC-ES/autosubmit.git@${AUTOSUBMIT_GIT_REF}" || echo "autosubmit==${AUTOSUBMIT_VERSION}" )


WORKDIR /apps

# Create virtual environment for the Autosubmit API
RUN python3 -m venv /apps/autosubmit-api &&\
    /apps/autosubmit-api/bin/pip install --upgrade pip &&\
    /apps/autosubmit-api/bin/pip install \
    $( [ -n "${AUTOSUBMIT_API_GIT_REF}" ] && echo "git+https://github.com/BSC-ES/autosubmit-api.git@${AUTOSUBMIT_API_GIT_REF}" || echo "autosubmit-api==${AUTOSUBMIT_API_VERSION}" )

# Set the environment variables
ENV PROTECTION_LEVEL=NONE
ENV JUPYTER_TOKEN=""
ENV AS_PG_CONN_URL=""

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN sudo chmod +x /entrypoint.sh

COPY load_ssh_private_key.sh /load_ssh_private_key.sh
RUN sudo chmod +x /load_ssh_private_key.sh

EXPOSE 8888
EXPOSE 8000

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]
