package com.hazelcast.cache.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.Set;

/**
 * Provide InMemoryFormat specific operations for cache
 */
public interface CacheOperationProvider {

    Operation createPutOperation(Data key, Data value, ExpiryPolicy policy, boolean get, int completionId);

    Operation createGetOperation(Data key, ExpiryPolicy policy);

    Operation createContainsKeyOperation(Data key);

    Operation createPutIfAbsentOperation(Data key, Data value, ExpiryPolicy policy, int completionId);

    Operation createRemoveOperation(Data key, Data value, int completionId);

    Operation createGetAndRemoveOperation(Data key, int completionId);

    Operation createReplaceOperation(Data key, Data oldValue, Data newValue, ExpiryPolicy policy, int completionId);

    Operation createGetAndReplaceOperation(Data key, Data value, ExpiryPolicy policy, int completionId);

    Operation createEntryProcessorOperation(Data key, Integer completionId, EntryProcessor entryProcessor, Object... args);

    Operation createKeyIteratorOperation(int lastTableIndex, int fetchSize);

    OperationFactory createGetAllOperationFactory(Set<Data> keySet, ExpiryPolicy policy);

    OperationFactory createLoadAllOperationFactory(Set<Data> keySet, boolean replaceExistingValues);

    OperationFactory createClearOperationFactory(Integer completionId);

    OperationFactory createRemoveAllOperationFactory(Set<Data> keySet, Integer completionId);

    OperationFactory createSizeOperationFactory();
}
