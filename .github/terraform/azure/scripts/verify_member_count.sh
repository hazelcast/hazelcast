#!/bin/bash

set -e

EXPECTED_SIZE=$1

verify_hazelcast_cluster_size() {
    return 1
}

echo "Checking Hazelcast cluster size"
verify_hazelcast_cluster_size $EXPECTED_SIZE
