#!/bin/bash

set -euo pipefail ${RUNNER_DEBUG:+-x}
export GH_DEBUG=${RUNNER_DEBUG:+1}

get_labels() {
  local repo=$1
  local pr=$2
  gh pr view --repo $repo $pr --json labels --jq '.labels[].name' | sort
}

join_with_comma() {
  tr '\n' ',' | sed 's/,$//'
}

if [ $# -ne 4 ]; then
  echo "Usage: $0 <SOURCE_REPO> <SOURCE_PR> <TARGET_REPO> <TARGET_PR>"
  exit 1
fi

SOURCE_REPO=$1
SOURCE_PR=$2
TARGET_REPO=$3
TARGET_PR=$4

# Get the labels from the source and target pull requests
SOURCE_LABELS=$(get_labels $SOURCE_REPO $SOURCE_PR)
TARGET_LABELS=$(get_labels $TARGET_REPO $TARGET_PR)

LABELS_TO_ADD=$(comm -23 <(echo "$SOURCE_LABELS") <(echo "$TARGET_LABELS") | join_with_comma)
LABELS_TO_REMOVE=$(comm -13 <(echo "$SOURCE_LABELS") <(echo "$TARGET_LABELS") | join_with_comma)

if [ -n "$LABELS_TO_ADD" ] || [ -n "$LABELS_TO_REMOVE" ]; then
  echo "Adding labels: $LABELS_TO_ADD"
  echo "Removing labels: $LABELS_TO_REMOVE"
  gh pr edit --repo $TARGET_REPO $TARGET_PR --add-label "$LABELS_TO_ADD" --remove-label "$LABELS_TO_REMOVE"
  echo "Labels synchronized from PR $SOURCE_PR to PR $TARGET_PR"
else
  echo "No label changes detected. Skipping."
fi
