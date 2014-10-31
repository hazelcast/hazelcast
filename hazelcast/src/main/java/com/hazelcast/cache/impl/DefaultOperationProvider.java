package com.hazelcast.cache.impl;

import com.hazelcast.cache.impl.operation.CacheClearOperationFactory;
import com.hazelcast.cache.impl.operation.CacheContainsKeyOperation;
import com.hazelcast.cache.impl.operation.CacheEntryProcessorOperation;
import com.hazelcast.cache.impl.operation.CacheGetAllOperationFactory;
import com.hazelcast.cache.impl.operation.CacheGetAndRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheGetAndReplaceOperation;
import com.hazelcast.cache.impl.operation.CacheGetOperation;
import com.hazelcast.cache.impl.operation.CacheKeyIteratorOperation;
import com.hazelcast.cache.impl.operation.CacheLoadAllOperationFactory;
import com.hazelcast.cache.impl.operation.CachePutIfAbsentOperation;
import com.hazelcast.cache.impl.operation.CachePutOperation;
import com.hazelcast.cache.impl.operation.CacheRemoveAllOperationFactory;
import com.hazelcast.cache.impl.operation.CacheRemoveOperation;
import com.hazelcast.cache.impl.operation.CacheReplaceOperation;
import com.hazelcast.cache.impl.operation.CacheSizeOperationFactory;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.Set;

/**
 * Provide operations other then {@link InMemoryFormat#NATIVE}
 */
public class DefaultOperationProvider implements CacheOperationProvider {

    private final String nameWithPrefix;

    public DefaultOperationProvider(String nameWithPrefix) {
        this.nameWithPrefix = nameWithPrefix;
    }

    @Override
    public Operation createPutOperation(Data key, Data value, ExpiryPolicy policy, boolean get, int completionId) {
        return new CachePutOperation(nameWithPrefix, key, value, policy, get, completionId);
    }

    @Override
    public Operation createGetOperation(Data key, ExpiryPolicy policy) {
        return new CacheGetOperation(nameWithPrefix, key, policy);
    }

    @Override
    public Operation createContainsKeyOperation(Data key) {
        return new CacheContainsKeyOperation(nameWithPrefix, key);
    }

    @Override
    public Operation createPutIfAbsentOperation(Data key, Data value, ExpiryPolicy policy, int completionId) {
        return new CachePutIfAbsentOperation(nameWithPrefix, key, value, policy, completionId);
    }

    @Override
    public Operation createRemoveOperation(Data key, Data oldValue, int completionId) {
        return new CacheRemoveOperation(nameWithPrefix, key, oldValue, completionId);
    }

    @Override
    public Operation createGetAndRemoveOperation(Data key, int completionId) {
        return new CacheGetAndRemoveOperation(nameWithPrefix, key, completionId);
    }

    @Override
    public Operation createReplaceOperation(Data key, Data oldValue, Data newValue, ExpiryPolicy policy, int completionId) {
        return new CacheReplaceOperation(nameWithPrefix, key, oldValue, newValue, policy, completionId);
    }

    @Override
    public Operation createGetAndReplaceOperation(Data key, Data value, ExpiryPolicy policy, int completionId) {
        return new CacheGetAndReplaceOperation(nameWithPrefix, key, value, policy, completionId);
    }

    @Override
    public Operation createEntryProcessorOperation(Data key, Integer completionId, EntryProcessor
            entryProcessor, Object... arguments) {
        return new CacheEntryProcessorOperation(nameWithPrefix, key, completionId, entryProcessor, arguments);
    }

    @Override
    public Operation createKeyIteratorOperation(int lastTableIndex, int fetchSize) {
        return new CacheKeyIteratorOperation(nameWithPrefix, lastTableIndex, fetchSize);
    }

    @Override
    public OperationFactory createGetAllOperationFactory(Set<Data> keySet, ExpiryPolicy policy) {
        return new CacheGetAllOperationFactory(nameWithPrefix, keySet, policy);
    }

    @Override
    public OperationFactory createLoadAllOperationFactory(Set<Data> keySet, boolean replaceExistingValues) {
        return new CacheLoadAllOperationFactory(nameWithPrefix, keySet, replaceExistingValues);
    }

    @Override
    public OperationFactory createClearOperationFactory(Integer completionId) {
        return new CacheClearOperationFactory(nameWithPrefix, completionId);
    }

    @Override
    public OperationFactory createRemoveAllOperationFactory(Set<Data> keySet, Integer completionId) {
        return new CacheRemoveAllOperationFactory(nameWithPrefix, keySet, completionId);
    }

    @Override
    public OperationFactory createSizeOperationFactory() {
        return new CacheSizeOperationFactory(nameWithPrefix);
    }
}
