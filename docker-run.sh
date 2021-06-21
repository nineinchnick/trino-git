#!/usr/bin/env bash

set -euo pipefail

VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)

connectors=("$@")

if [ ${#connectors} == 0 ]; then
    connectors=(git)
fi

opts=()
for c in "${connectors[@]}"; do
    opts+=(-e "${c^^}"_TOKEN -v "$(pwd)/catalog/$c.properties:/etc/trino/catalog/$c.properties")
done

docker run \
    --name trino-git \
    -d \
    -p8080:8080 \
    -v $(mktemp -d):/etc/trino/catalog \
    "${opts[@]}" \
    nineinchnick/trino-git:"$VERSION"
