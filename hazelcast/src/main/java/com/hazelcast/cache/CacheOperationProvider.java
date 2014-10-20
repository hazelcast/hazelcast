package com.hazelcast.cache;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.Set;

/**
 * TODO add a proper JavaDoc
 */
public interface CacheOperationProvider {

    Operation createPutOperation(Data key, Data value, ExpiryPolicy policy, boolean get);

    Operation createGetOperation(Data key, ExpiryPolicy policy);

    Operation createContainsKeyOperation(Data key);

    Operation createPutIfAbsentOperation(Data key, Data value, ExpiryPolicy policy);

    Operation createRemoveOperation(Data key, Data value);

    Operation createGetAndRemoveOperation(Data key);

    Operation createReplaceOperation(Data key, Data oldValue, Data newValue, ExpiryPolicy policy);

    Operation createGetAndReplaceOperation(Data key, Data value, ExpiryPolicy policy);

    Operation createEntryProcessorOperation(Data key, Integer completionId, EntryProcessor entryProcessor, Object... args);

    Operation createKeyIteratorOperation(int lastTableIndex, int fetchSize);

    Operation createDestroyOperation();

    OperationFactory createGetAllOperationFactory(Set<Data> keySet, ExpiryPolicy policy);

    OperationFactory createLoadAllOperationFactory(Set<Data> keySet, boolean replaceExistingValues);

    OperationFactory createClearOperationFactory(Set<Data> keySet, boolean isRemoveAll, Integer completionId);

    OperationFactory createSizeOperationFactory();
}
