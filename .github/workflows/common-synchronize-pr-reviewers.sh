#!/bin/bash

set -euo pipefail ${RUNNER_DEBUG:+-x}
export GH_DEBUG=${RUNNER_DEBUG:+1}

get_reviewers() {
  local repo=$1
  local pr=$2
  gh pr view --repo $repo $pr --json reviewRequests --jq '.reviewRequests[].login' | sort
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

# Get the reviewers from the source and target pull requests
SOURCE_REVIEWERS=$(get_reviewers $SOURCE_REPO $SOURCE_PR)
TARGET_REVIEWERS=$(get_reviewers $TARGET_REPO $TARGET_PR)

REVIEWERS_TO_ADD=$(comm -23 <(echo "$SOURCE_REVIEWERS") <(echo "$TARGET_REVIEWERS") | join_with_comma)
REVIEWERS_TO_REMOVE=$(comm -13 <(echo "$SOURCE_REVIEWERS") <(echo "$TARGET_REVIEWERS") | join_with_comma)

if [ -n "$REVIEWERS_TO_ADD" ] || [ -n "$REVIEWERS_TO_REMOVE" ]; then
  echo "Adding reviewers: $REVIEWERS_TO_ADD"
  echo "Removing reviewers: $REVIEWERS_TO_REMOVE"
  gh pr edit --repo $TARGET_REPO $TARGET_PR --add-reviewer "$REVIEWERS_TO_ADD" --remove-reviewer "$REVIEWERS_TO_REMOVE"
  echo "Reviewers synchronized from PR $SOURCE_PR to PR $TARGET_PR"
else
  echo "No reviewer changes detected. Skipping."
fi
