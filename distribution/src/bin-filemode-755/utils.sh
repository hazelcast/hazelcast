#!/usr/bin/env bash

prepare_java_opts_with_masking() {
  mask_keys=""
  mask_array=()
  FILTERED_JAVA_OPTS=()
  DISPLAY_OPTS=()

  for opt in "${JAVA_OPTS_ARRAY[@]}"; do
    if [[ "${opt}" == -DmaskOpts=* ]]; then
      mask_keys="${opt#-DmaskOpts=}"
      IFS=',' read -r -a mask_array <<< "$mask_keys"
    else
      FILTERED_JAVA_OPTS+=("${opt}")
    fi
  done

  DISPLAY_OPTS=()
  for opt in "${FILTERED_JAVA_OPTS[@]}"; do
    masked=false
    for key in "${mask_array[@]}"; do
      if [[ "${opt}" == -D"${key}"=* ]]; then
        DISPLAY_OPTS+=("-D${key}=****")
        masked=true
        break
      fi
    done
    if ! $masked; then
      DISPLAY_OPTS+=("${opt}")
    fi
  done
}