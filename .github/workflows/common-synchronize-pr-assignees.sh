#!/bin/bash

set -euo pipefail ${RUNNER_DEBUG:+-x}
export GH_DEBUG=${RUNNER_DEBUG:+1}

get_assignees() {
  local repo=$1
  local pr=$2
  gh pr view --repo $repo $pr --json assignees --jq '.assignees[].login' | sort
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

# Get the assignees from the source and target pull requests
SOURCE_ASSIGNEES=$(get_assignees $SOURCE_REPO $SOURCE_PR)
TARGET_ASSIGNEES=$(get_assignees $TARGET_REPO $TARGET_PR)

ASSIGNEES_TO_ADD=$(comm -23 <(echo "$SOURCE_ASSIGNEES") <(echo "$TARGET_ASSIGNEES") | join_with_comma)
ASSIGNEES_TO_REMOVE=$(comm -13 <(echo "$SOURCE_ASSIGNEES") <(echo "$TARGET_ASSIGNEES") | join_with_comma)

if [ -n "$ASSIGNEES_TO_ADD" ] || [ -n "$ASSIGNEES_TO_REMOVE" ]; then
  echo "Adding assignees: $ASSIGNEES_TO_ADD"
  echo "Removing assignees: $ASSIGNEES_TO_REMOVE"
  gh pr edit --repo $TARGET_REPO $TARGET_PR --add-assignee "$ASSIGNEES_TO_ADD" --remove-assignee "$ASSIGNEES_TO_REMOVE"
  echo "Assignees synchronized from PR $SOURCE_PR to PR $TARGET_PR"
else
  echo "No assignee changes detected. Skipping."
fi
