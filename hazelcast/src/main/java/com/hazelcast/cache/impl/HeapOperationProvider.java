package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheOperationProvider;
import com.hazelcast.cache.impl.operation.CacheGetOperation;
import com.hazelcast.cache.impl.operation.CachePutOperation;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;

import javax.cache.expiry.ExpiryPolicy;

/**
 * TODO add a proper JavaDoc
 */
public class HeapOperationProvider implements CacheOperationProvider {

    private final String nameWithPrefix;

    public HeapOperationProvider(String nameWithPrefix) {
        this.nameWithPrefix = nameWithPrefix;
    }

    @Override
    public Operation createPutOperation(Data key, Data value, ExpiryPolicy policy, boolean get) {
        return new CachePutOperation(nameWithPrefix, key, value, policy, get);
    }

    @Override
    public Operation createGetOperation(Data key, ExpiryPolicy policy) {
        return new CacheGetOperation(nameWithPrefix, key, policy);
    }
}
