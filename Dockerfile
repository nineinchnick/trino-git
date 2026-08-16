ARG TRINO_VERSION
FROM trinodb/trino-core:$TRINO_VERSION AS plugin

ARG VERSION

COPY target/trino-git-$VERSION.zip /tmp/trino-git.zip
RUN mkdir /tmp/trino-git && \
    cd /tmp/trino-git && \
    jar --extract --file /tmp/trino-git.zip

FROM trinodb/trino-core:$TRINO_VERSION

ARG VERSION

COPY --chown=trino:trino --from=plugin /tmp/trino-git/trino-git-$VERSION/ /usr/lib/trino/plugin/git/
COPY --chown=trino:trino catalog/git.properties /etc/trino/catalog/git.properties
