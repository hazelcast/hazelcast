/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cache.impl;

import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.CacheMergeTypes;

import javax.cache.expiry.ExpiryPolicy;
import javax.cache.processor.EntryProcessor;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provide InMemoryFormat specific operations for cache
 */
public interface CacheOperationProvider {

    Operation createPutOperation(Data key, Data value, ExpiryPolicy policy, boolean get, int completionId);

    Operation createPutAllOperation(List<Map.Entry<Data, Data>> entries, ExpiryPolicy policy, int completionId);

    Operation createGetOperation(Data key, ExpiryPolicy policy);

    Operation createContainsKeyOperation(Data key);

    Operation createPutIfAbsentOperation(Data key, Data value, ExpiryPolicy policy, int completionId);

    Operation createRemoveOperation(Data key, Data value, int completionId);

    Operation createGetAndRemoveOperation(Data key, int completionId);

    Operation createReplaceOperation(Data key, Data oldValue, Data newValue, ExpiryPolicy policy, int completionId);

    Operation createGetAndReplaceOperation(Data key, Data value, ExpiryPolicy policy, int completionId);

    Operation createEntryProcessorOperation(Data key, Integer completionId, EntryProcessor entryProcessor, Object... args);

    /**
     * Creates an operation for fetching a segment of a keys from a single
     * partition.
     *
     * @see CacheProxy#iterator(int, int, boolean)
     */
    Operation createFetchKeysOperation(IterationPointer[] pointers, int fetchSize);

    /**
     * Creates an operation for fetching a segment of a entries from a single
     * partition.
     *
     * @see CacheProxy#iterator(int, int, boolean)
     */
    Operation createFetchEntriesOperation(IterationPointer[] pointers, int fetchSize);

    Operation createMergeOperation(String name, List<CacheMergeTypes<Object, Object>> mergingEntries,
                                   SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> policy);

    OperationFactory createMergeOperationFactory(String name, int[] partitions,
                                                 List<CacheMergeTypes<Object, Object>>[] mergingEntries,
                                                 SplitBrainMergePolicy<Object, CacheMergeTypes<Object, Object>, Object> policy);

    Operation createSetExpiryPolicyOperation(List<Data> keys, Data expiryPolicy);

    OperationFactory createGetAllOperationFactory(Set<Data> keySet, ExpiryPolicy policy);

    OperationFactory createLoadAllOperationFactory(Set<Data> keySet, boolean replaceExistingValues);

    OperationFactory createClearOperationFactory();

    OperationFactory createRemoveAllOperationFactory(Set<Data> keySet, Integer completionId);

    OperationFactory createSizeOperationFactory();

}
