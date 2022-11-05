ARG TRINO_VERSION
FROM nineinchnick/trino-core:$TRINO_VERSION

ARG VERSION

ADD target/trino-git-$VERSION/ /usr/lib/trino/plugin/git/
ADD catalog/git.properties /etc/trino/catalog/git.properties
