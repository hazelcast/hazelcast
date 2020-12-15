package com.hazelcast.map.impl.recordstore;

public enum ExpiryReason {
        TTL,
        IDLENESS,
        NOT_EXPIRED
    }