FROM mambaorg/micromamba:1-bookworm-slim AS micromamba

# The micromamba image is referenced in a multi-stage build.
# Note that around 90% of the content of this image is actually
# for micromamba users, permissions, dependencies, and SSH.
# The Autosubmit portion is rather small, being mainly install
# Autosubmit with pip, and run `autosubmit configure` and then
# `autosubmit install`. It would be possible to create another
# image autosubmit-base, for example, with the users, permissions,
# dependencies, and SSH. Which would tremendously reduce this.

FROM debian:bookworm-slim

ARG AUTOSUBMIT_ROOT_DIR=/app/autosubmit/

USER root

# micromamba docs, https://micromamba-docker.readthedocs.io/en/latest/advanced_usage.html#adding-micromamba-to-an-existing-docker-image

# if your image defaults to a non-root user, then you may want to make the
# next 3 ARG commands match the values in your image. You can get the values
# by running: docker run --rm -it my/image id -a
ARG MAMBA_USER=mamba
ARG MAMBA_USER_ID=1000
ARG MAMBA_USER_GID=1000
ENV MAMBA_USER=$MAMBA_USER
ENV MAMBA_ROOT_PREFIX="/opt/conda"
ENV MAMBA_EXE="/bin/micromamba"

COPY --from=micromamba "$MAMBA_EXE" "$MAMBA_EXE"
COPY --from=micromamba /usr/local/bin/_activate_current_env.sh /usr/local/bin/_activate_current_env.sh
COPY --from=micromamba /usr/local/bin/_dockerfile_shell.sh /usr/local/bin/_dockerfile_shell.sh
COPY --from=micromamba /usr/local/bin/_entrypoint.sh /usr/local/bin/_entrypoint.sh
COPY --from=micromamba /usr/local/bin/_dockerfile_initialize_user_accounts.sh /usr/local/bin/_dockerfile_initialize_user_accounts.sh
COPY --from=micromamba /usr/local/bin/_dockerfile_setup_root_prefix.sh /usr/local/bin/_dockerfile_setup_root_prefix.sh

RUN /usr/local/bin/_dockerfile_initialize_user_accounts.sh && \
    /usr/local/bin/_dockerfile_setup_root_prefix.sh

USER $MAMBA_USER

SHELL ["/usr/local/bin/_dockerfile_shell.sh"]

# ENTRYPOINT ["/usr/local/bin/_entrypoint.sh"]
# Optional: if you want to customize the ENTRYPOINT and have a conda
# environment activated, then do this:
# ENTRYPOINT ["/usr/local/bin/_entrypoint.sh", "my_entrypoint_program"]

# end micromamba

# micromamba docs, https://micromamba-docker.readthedocs.io/en/latest/advanced_usage.html#changing-the-user-id-or-name

ARG NEW_MAMBA_USER=autosubmit
ARG NEW_MAMBA_USER_ID=1000
ARG NEW_MAMBA_USER_GID=1000
USER root

RUN if grep -q '^ID=alpine$' /etc/os-release; then \
      # alpine does not have usermod/groupmod
      apk add --no-cache --virtual temp-packages shadow=4.13-r0; \
    fi && \
    usermod "--login=${NEW_MAMBA_USER}" "--home=/home/${NEW_MAMBA_USER}" \
      --move-home "-u ${NEW_MAMBA_USER_ID}" "${MAMBA_USER}" && \
    groupmod "--new-name=${NEW_MAMBA_USER}" \
      "-g ${NEW_MAMBA_USER_GID}" "${MAMBA_USER}" && \
    if grep -q '^ID=alpine$' /etc/os-release; then \
      # remove the packages that were only needed for usermod/groupmod
      apk del temp-packages; \
    fi && \
    # Update the expected value of MAMBA_USER for the
    # _entrypoint.sh consistency check.
    echo "${NEW_MAMBA_USER}" > "/etc/arg_mamba_user" && \
    :
ENV MAMBA_USER=$NEW_MAMBA_USER
USER $MAMBA_USER

# end micromamba

WORKDIR "${AUTOSUBMIT_ROOT_DIR}"

ENV ENV_NAME=base
ENV PATH="$MAMBA_ROOT_PREFIX/bin:/usr/bin:/usr/local/bin:$PATH"

# The directories the container will use for Autosubmit files.
USER root
RUN mkdir -pv "${AUTOSUBMIT_ROOT_DIR}/logs" && \
    mkdir -pv "${AUTOSUBMIT_ROOT_DIR}/metadata" && \
    chown -R "${MAMBA_USER}:${MAMBA_USER}" "${AUTOSUBMIT_ROOT_DIR}"

# update-ca-certificates is for: libmamba No CA certificates found on system.
# openssh-server is for: Autosubmit platforms.
# git is for: Autosubmit Git projects.
# bash, curl, dialog, graphviz, python3-tk, sqlite3 are for: Autosubmit dependencies listed at readthedocs.
# less, vim, iputils-ping, net-tools are for: convenience.
# TODO: add something like xpdf=3.04+git20210103-3 if GUI/X is needed.
RUN apt update && \
    apt install -y \
      gcc \
      bash \
      ca-certificates \
      curl \
      desktop-file-utils \
      dialog \
      graphviz \
      iputils-ping \
      less \
      net-tools \
      openssh-server \
      rsync \
      python3-tk \
      sqlite3 \
      subversion \
      sudo \
      tini \
      vim \
      xdg-utils && \
    update-ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER $MAMBA_USER

# Install and configure Autosubmit.

# Install Python.
RUN micromamba install --yes --name base --channel conda-forge \
      git=2.41.0 \
      python=3.9.16 && \
    micromamba clean --all --yes && \
    /usr/local/bin/_activate_current_env.sh

# Install Autosubmit.
# If GIT_REF is set, it will install from the specified branch/tag/commit.
# Otherwise, if AUTOSUBMIT_VERSION is set, it will install the specified version from pypi.
# Otherwise, it will install the latest version from pypi.
ARG GIT_REF
ARG AUTOSUBMIT_VERSION
RUN if [ -n "${GIT_REF}" ] ; then \
      pip install git+https://github.com/BSC-ES/autosubmit.git@${GIT_REF}; \
    elif [ -n "${AUTOSUBMIT_VERSION}" ] ; then \
      pip install autosubmit==${AUTOSUBMIT_VERSION}; \
    else \
      pip install autosubmit; \
    fi

# Configure Autosubmit.
RUN autosubmit configure \
      -db "${AUTOSUBMIT_ROOT_DIR}/database/" \
      -dbf autosubmit.db \
      -lr "${AUTOSUBMIT_ROOT_DIR}/experiments/"

# Copy the default config file /home/${MAMBA_USER}/.autosubmitrc to ${AUTOSUBMIT_ROOT_DIR}/autosubmitrc
RUN cp "/home/${MAMBA_USER}/.autosubmitrc" "${AUTOSUBMIT_ROOT_DIR}/autosubmitrc" && \
    chown -R "${MAMBA_USER}:${MAMBA_USER}" "${AUTOSUBMIT_ROOT_DIR}"

# Set AUTOSUBMIT_CONFIGURATION to the path of the Autosubmit configuration file.
ENV AUTOSUBMIT_CONFIGURATION="${AUTOSUBMIT_ROOT_DIR}/autosubmitrc"

# Set AUTOSUBMIT_CONFIGURATION to the path of the Autosubmit configuration file.
ENV AUTOSUBMIT_CONFIGURATION="${AUTOSUBMIT_ROOT_DIR}/autosubmitrc"

# SSH (for Autosubmit local platform.)
USER root

RUN chown -R "${MAMBA_USER}:${MAMBA_USER}" "/home/${MAMBA_USER}/" && \
    chmod 0755 "/home/${MAMBA_USER}/" && \
    mkdir -pv /var/run/sshd && \
    mkdir -pv "/home/$MAMBA_USER/.ssh" && \
    chmod 0700 "/home/$MAMBA_USER/.ssh" && \
    touch "/home/$MAMBA_USER/.ssh/authorized_keys" && \
    chmod 600 "/home/$MAMBA_USER/.ssh/authorized_keys" && \
    chown -R "${MAMBA_USER}:${MAMBA_USER}" "/home/$MAMBA_USER/.ssh" && \
    usermod -a -G sudo "${MAMBA_USER}" && \
    sed -i "s/^%sudo.*$/%sudo ALL=(ALL:ALL) NOPASSWD:ALL/g" /etc/sudoers && \
    sed -i 's/#\?\(PasswordAuthentication\s*\).*$/\1 no/g' /etc/ssh/sshd_config && \
    sed -i 's/#\?\(SyslogFacility.*\)$/\1/g' /etc/ssh/sshd_config && \
    sed -i 's/#\?\(LogLevel\).*$/\1 VERBOSE/g' /etc/ssh/sshd_config && \
    sed -i 's/#\?.*StrictHostKeyChecking.*$/StrictHostKeyChecking accept-new/g' /etc/ssh/ssh_config && \
    env | egrep -v "^(HOME=|USER=|MAIL=|LC_ALL=|LS_COLORS=|LANG=|HOSTNAME=|PWD=|TERM=|SHLVL=|LANGUAGE=|_=)" >> /etc/environment

# TODO: For PDF and GUI/X support, we could use something similar to:
#
# COPY <<EOF /usr/share/applications/defaults.list
# application/pdf=xpdf.desktop
# EOF
#
# RUN update-mime-database /usr/share/mime

# entrypoint

COPY <<EOF /usr/local/bin/entrypoint.sh
#!/bin/bash
export USER=$(whoami)
sudo service ssh restart
exec "\$@"
EOF

RUN chmod a+x /usr/local/bin/entrypoint.sh

USER $MAMBA_USER

EXPOSE 22

LABEL maintainer="Autosubmit team"

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
CMD ["autosubmit"]
