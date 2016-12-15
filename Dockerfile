FROM metabrainz/consul-template-base

ARG DEBIAN_FRONTEND=noninteractive

ARG BUILD_DEPS=" \
    build-essential \
    cabal-install \
    ghc \
    zlib1g-dev"

ARG RUN_DEPS=" \
    # Fixes "getProtocolByName: does not exist (no such protocol name: tcp)"
    netbase"

RUN useradd --create-home --shell /bin/bash caa

ARG CAA_ADMIN_ROOT=/home/caa/caa-admin
WORKDIR $CAA_ADMIN_ROOT

COPY caa-admin.cabal cabal.config LICENSE Main.hs ./

RUN chown -R caa:caa $CAA_ADMIN_ROOT && \
    apt-get update && \
    apt-get install \
        --no-install-suggests \
        --no-install-recommends \
        -y \
        $BUILD_DEPS \
        $RUN_DEPS && \
    cabal update && \
    cabal install && \
    apt-get purge --auto-remove -y $BUILD_DEPS

COPY docker/consul-template.conf /etc/
COPY docker/devel.cfg.ctmpl ./
COPY docker/start_caa_admin.sh /usr/local/bin/
