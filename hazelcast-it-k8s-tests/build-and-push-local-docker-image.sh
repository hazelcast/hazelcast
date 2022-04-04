#!/usr/bin/env bash

set -e

function findScriptDir() {
  CURRENT=$PWD

  DIR=$(dirname "$0")
  cd "$DIR" || exit
  TARGET_FILE=$(basename "$0")

  # Iterate down a (possible) chain of symlinks
  while [ -L "$TARGET_FILE" ]
  do
      TARGET_FILE=$(readlink "$TARGET_FILE")
      DIR=$(dirname "$TARGET_FILE")
      cd "$DIR" || exit
      TARGET_FILE=$(basename "$TARGET_FILE")
  done

  SCRIPT_DIR=$(pwd -P)
  # Restore current directory
  cd "$CURRENT" || exit
}

findScriptDir

cd "$SCRIPT_DIR/.."

if [[ ! -d "$SCRIPT_DIR/hazelcast-docker" ]]; then
git clone https://github.com/hazelcast/hazelcast-docker.git $SCRIPT_DIR/hazelcast-docker
fi

if [ -z "$DOCKER_REPO" ]; then
      echo "DOCKER_REPO variable not set"
      exit 1
fi

export DOCKER_TAG=${DOCKER_TAG:-"$(whoami)-$(git branch --show-current)"}

echo "Using docker tag $DOCKER_TAG"

echo "Building distribution package"
./mvnw clean install -Dnot-quick -Pquick

echo "Creating local docker image"
find distribution/target -name "hazelcast-*.zip" -not -iname '*-slim.zip' -exec cp -f '{}' "$SCRIPT_DIR/hazelcast-docker/hazelcast-oss/hazelcast-distribution.zip" \;
docker buildx build "$SCRIPT_DIR/hazelcast-docker/hazelcast-oss" --platform=linux/amd64 --tag "${DOCKER_REPO}/hazelcast:${DOCKER_TAG}"

echo "Pushing the hazelcast:${DOCKER_TAG} image to docker repository"
docker push "${DOCKER_REPO}/hazelcast:${DOCKER_TAG}"
