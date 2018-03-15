/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dictionary.impl;

import com.hazelcast.dataseries.MemoryInfo;
import com.hazelcast.dataseries.impl.DataSeriesService;
import com.hazelcast.dataseries.impl.operations.MemoryUsageOperation;
import com.hazelcast.dataseries.impl.operations.MemoryUsageOperationFactory;
import com.hazelcast.dictionary.Dictionary;
import com.hazelcast.dictionary.impl.operations.ClearOperationFactory;
import com.hazelcast.dictionary.impl.operations.GetOperation;
import com.hazelcast.dictionary.impl.operations.MemoryInfoOperation;
import com.hazelcast.dictionary.impl.operations.MemoryInfoOperationFactory;
import com.hazelcast.dictionary.impl.operations.PutOperation;
import com.hazelcast.dictionary.impl.operations.RemoveOperation;
import com.hazelcast.dictionary.impl.operations.SizeOperationFactory;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DictionaryProxy<K, V>
        extends AbstractDistributedObject<DictionaryService>
        implements Dictionary<K, V> {

    private final String name;
    private final InternalPartitionService partitionService;
    private final InternalOperationService operationService;

    DictionaryProxy(String name, NodeEngineImpl nodeEngine, DictionaryService dictionaryService) {
        super(nodeEngine, dictionaryService);
        this.name = name;
        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();
    }

    @Override
    public String getServiceName() {
        return DictionaryService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public V get(K key) {
        return getAsync(key).join();
    }

    @Override
    public InternalCompletableFuture<V> getAsync(K key) {
        checkNotNull(key, "key can't be null");

        Data keyData = toData(key);

        Operation op = new GetOperation(name, keyData)
                .setPartitionId(partitionService.getPartitionId(keyData));

        return operationService.invokeOnPartition(op);
    }

    @Override
    public void put(K key, V value) {
        putAsync(key, value).join();
    }

    @Override
    public InternalCompletableFuture<Void> putAsync(K key, V value) {
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data keyData = toData(key);
        Data valueData = toData(value);

        Operation op = new PutOperation(name, keyData, valueData)
                .setPartitionId(partitionService.getPartitionId(keyData));

        return operationService.invokeOnPartition(op);
    }

    @Override
    public V remove(K key) {
        return removeAsync(key).join();
    }

    @Override
    public InternalCompletableFuture<V> removeAsync(K key) {
        checkNotNull(key, "key can't be null");

        Data keyData = toData(key);

        Operation op = new RemoveOperation(name, keyData)
                .setPartitionId(partitionService.getPartitionId(keyData));

        return operationService.invokeOnPartition(op);
    }

    @Override
    public long size() {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DictionaryService.SERVICE_NAME, new SizeOperationFactory(name));

            long size = 0;
            for (Object value : result.values()) {
                size += ((Long) value);
            }
            return size;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clear() {
        try {
            operationService.invokeOnAllPartitions(
                    DictionaryService.SERVICE_NAME, new ClearOperationFactory(name));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MemoryInfo memoryInfo() {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DictionaryService.SERVICE_NAME, new MemoryInfoOperationFactory(name));

            long allocated = 0;
            long consumed = 0;
            long count = 0;
            int segmentsUsed = 0;
            for (Object value : result.values()) {
                MemoryInfo memoryInfo = (MemoryInfo) value;
                allocated += memoryInfo.allocatedBytes();
                consumed += memoryInfo.consumedBytes();
                segmentsUsed += memoryInfo.segmentsInUse();
                count += memoryInfo.count();
            }
            return new MemoryInfo(consumed, allocated, segmentsUsed, count);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MemoryInfo memoryInfo(int partitionId) {
        Operation op = new MemoryInfoOperation(name).setPartitionId(partitionId);
        InternalCompletableFuture<MemoryInfo> f = operationService.invokeOnPartition(op);
        return f.join();
    }
}
