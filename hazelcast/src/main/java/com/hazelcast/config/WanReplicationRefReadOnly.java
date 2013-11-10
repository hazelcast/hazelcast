package com.hazelcast.config;

/**
 * @ali 10/11/13
 */
public class WanReplicationRefReadOnly extends WanReplicationRef {

    public WanReplicationRefReadOnly(WanReplicationRef ref) {
        this.name = ref.name;
        this.mergePolicy = ref.mergePolicy;
    }

    public WanReplicationRef setName(String name) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    public WanReplicationRef setMergePolicy(String mergePolicy) {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
