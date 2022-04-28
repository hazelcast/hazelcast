#!/bin/bash

set -e

EXPECTED_SIZE=$1

verify_hazelcast_cluster_size() {
    EXPECTED_SIZE=$1
    for i in `seq 1 10`; do
        local MEMBER_COUNT=$( curl -sS http://127.0.0.1:5701/hazelcast/health/cluster-size )

        if [ "$MEMBER_COUNT" == "$EXPECTED_SIZE" ] ; then
            echo "Hazelcast cluster size equal to ${EXPECTED_SIZE}"
            return 0
        else
            echo "Hazelcast cluster size NOT equal to ${EXPECTED_SIZE}!. Waiting.."
            sleep 10
        fi
    done
    return 1
}

echo "Checking Hazelcast cluster size"
verify_hazelcast_cluster_size $EXPECTED_SIZE
