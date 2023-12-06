#!/bin/bash

set -euo pipefail ${RUNNER_DEBUG:+-x}
export GH_DEBUG=${RUNNER_DEBUG:+1}

get_milestone() {
  local repo=$1
  local pr=$2
  gh pr view --repo $repo $pr --json "milestone" -q '.milestone.title'
}

if [ $# -ne 4 ]; then
  echo "Usage: $0 <SOURCE_REPO> <SOURCE_PR> <TARGET_REPO> <TARGET_PR>"
  exit 1
fi

SOURCE_REPO=$1
SOURCE_PR=$2
TARGET_REPO=$3
TARGET_PR=$4

SOURCE_MILESTONE=$(get_milestone $SOURCE_REPO $SOURCE_PR)
TARGET_MILESTONE=$(get_milestone $TARGET_REPO $TARGET_PR)


if [ "$SOURCE_MILESTONE" != "$TARGET_MILESTONE" ]; then
  gh pr edit --repo $TARGET_REPO $TARGET_PR --milestone "$SOURCE_MILESTONE"
  echo "Milestone set to $SOURCE_MILESTONE in $TARGET_REPO#$TARGET_PR PR"
else
  echo "No milestone changes detected. Skipping."
fi
