FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    gcc \
    git \
    curl \
    graphviz \
    libnss-wrapper \
    libpq-dev \
    python3.9 python3.9-venv python3.9-dev \
    python3.10 python3.10-venv python3.10-dev \
    python3.11 python3.11-venv python3.11-dev \
    python3.12 python3.12-venv python3.12-dev \
    r-base \
    rsync \
    tini \
    xvfb \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.9 1 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 2 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 3 \
    && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 4

RUN curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py \
    && python3.9 /tmp/get-pip.py \
    && python3.10 /tmp/get-pip.py \
    && python3.11 /tmp/get-pip.py \
    && python3.12 /tmp/get-pip.py \
    && python3.9 -m pip install --upgrade pip setuptools wheel \
    && python3.10 -m pip install --upgrade pip setuptools wheel \
    && python3.11 -m pip install --upgrade pip setuptools wheel \
    && python3.12 -m pip install --upgrade pip setuptools wheel \
    && rm /tmp/get-pip.py

RUN update-alternatives --set python3 /usr/bin/python3.9

RUN chmod 666 /etc/passwd /etc/group
RUN mkdir -p /tmp/userhome && chmod 1777 /tmp /tmp/userhome
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /workspace
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD [ "bash" ]
