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

import com.hazelcast.dictionary.AggregationRecipe;
import com.hazelcast.dictionary.Dictionary;
import com.hazelcast.dictionary.MemoryInfo;
import com.hazelcast.dictionary.PreparedAggregation;
import com.hazelcast.dictionary.impl.operations.ClearOperationFactory;
import com.hazelcast.dictionary.impl.operations.ContainsKeyOperation;
import com.hazelcast.dictionary.impl.operations.EntriesOperation;
import com.hazelcast.dictionary.impl.operations.GetAndReplaceOperation;
import com.hazelcast.dictionary.impl.operations.GetOperation;
import com.hazelcast.dictionary.impl.operations.MemoryInfoOperation;
import com.hazelcast.dictionary.impl.operations.MemoryInfoOperationFactory;
import com.hazelcast.dictionary.impl.operations.PrepareAggregationOperationFactory;
import com.hazelcast.dictionary.impl.operations.PutOperation;
import com.hazelcast.dictionary.impl.operations.RemoveOperation;
import com.hazelcast.dictionary.impl.operations.ReplaceOperation;
import com.hazelcast.dictionary.impl.operations.SizeOperationFactory;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.UuidUtil;

import javax.cache.CacheManager;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.integration.CompletionListener;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class DictionaryProxy<K, V>
        extends AbstractDistributedObject<DictionaryService>
        implements Dictionary<K, V> {

    private final String name;
    private final InternalPartitionService partitionService;
    private final InternalOperationService operationService;
    private final int partitionCount;

    DictionaryProxy(String name, NodeEngineImpl nodeEngine, DictionaryService dictionaryService) {
        super(nodeEngine, dictionaryService);
        this.name = name;
        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();

        // causes the config to be validated.
        memoryInfo(0);
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
    public Iterator<Map.Entry<K, V>> entries(int partitionId, int segmentId) {
        Operation op = new EntriesOperation(name, segmentId)
                .setPartitionId(partitionId);
        // returning a list isn't the most efficient way; it would be better to deserialize when needed
        // but for the time being it is more important to just get the data.
        List<Map.Entry<K,V>> entries = (List<Map.Entry<K,V>>) operationService.invokeOnPartition(op).join();
        return entries.iterator();
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

        Operation op = new PutOperation(name, true, keyData, valueData)
                .setPartitionId(partitionService.getPartitionId(keyData));
        return operationService.invokeOnPartition(op);
    }

    @Override
    public boolean remove(K key) {
        return removeAsync(key).join();
    }

    @Override
    public InternalCompletableFuture<Boolean> removeAsync(K key) {
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
        checkPartitionId(partitionId);

        Operation op = new MemoryInfoOperation(name)
                .setPartitionId(partitionId);
        InternalCompletableFuture<MemoryInfo> f = operationService.invokeOnPartition(op);
        return f.join();
    }

    private void checkPartitionId(int partitionId) {
        checkNotNegative(partitionId, "partitionId can't be negative");
        if (partitionId >= partitionCount) {
            throw new IllegalArgumentException("PartitionId can't be larger than " + partitionCount);
        }
    }

    @Override
    public PreparedAggregation prepare(AggregationRecipe recipe) {
        checkNotNull(recipe, "recipe can't be null");

        String preparationId = newPreparationId();

        try {
            operationService.invokeOnAllPartitions(
                    DictionaryService.SERVICE_NAME, new PrepareAggregationOperationFactory(name, preparationId, recipe));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new PreparedAggregation(operationService, name, preparationId);
    }

    private String newPreparationId() {
        return name + "_" + UuidUtil.newUnsecureUuidString().replace("-", "");
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean containsKey(K key) {
        checkNotNull(key, "key can't be null");

        Data keyData = toData(key);

        Operation op = new ContainsKeyOperation(name, keyData)
                .setPartitionId(partitionService.getPartitionId(keyData));
        return (Boolean) operationService.invokeOnPartition(op).join();
    }

    @Override
    public void loadAll(Set<? extends K> keys, boolean replaceExistingValues, CompletionListener completionListener) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public V getAndPut(K key, V value) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean putIfAbsent(K key, V value) {
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data keyData = toData(key);
        Data valueData = toData(value);

        Operation op = new PutOperation(name, false, keyData, valueData)
                .setPartitionId(partitionService.getPartitionId(keyData));
        return (boolean) operationService.invokeOnPartition(op).join();
    }

    @Override
    public boolean remove(K key, V oldValue) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public V getAndRemove(K key) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean replace(K key, V value) {
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data keyData = toData(key);
        Data valueData = toData(value);

        Operation op = new ReplaceOperation(name, keyData, valueData)
                .setPartitionId(partitionService.getPartitionId(keyData));
        return (boolean) operationService.invokeOnPartition(op).join();
    }

    @Override
    public V getAndReplace(K key, V value) {
        checkNotNull(key, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data keyData = toData(key);
        Data valueData = toData(value);

        Operation op = new GetAndReplaceOperation(name, keyData, valueData)
                .setPartitionId(partitionService.getPartitionId(keyData));
        return (V) operationService.invokeOnPartition(op).join();
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void removeAll() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public <C extends Configuration<K, V>> C getConfiguration(Class<C> clazz) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public <T> T invoke(K key, EntryProcessor<K, V, T> entryProcessor, Object... arguments) throws EntryProcessorException {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public <T> Map<K, EntryProcessorResult<T>> invokeAll(
            Set<? extends K> keys, EntryProcessor<K, V, T> entryProcessor, Object... arguments) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public CacheManager getCacheManager() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public boolean isClosed() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void registerCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void deregisterCacheEntryListener(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void forEach(Consumer<? super Entry<K, V>> action) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Spliterator<Entry<K, V>> spliterator() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
