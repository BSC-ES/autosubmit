FROM ghcr.io/linuxserver/openssh-server:latest

RUN \
  echo "**** install runtime packages ****" && \
  apk add --no-cache --upgrade \
    google-authenticator \
    xauth \
    xclock \
    xorg-server && \
  rm -rf \
    /tmp/* \
    $HOME/.cache

ADD rootfs/ /
