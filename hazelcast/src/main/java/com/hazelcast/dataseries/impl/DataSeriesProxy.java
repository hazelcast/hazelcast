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

package com.hazelcast.dataseries.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.IMap;
import com.hazelcast.dataseries.AggregationRecipe;
import com.hazelcast.dataseries.DataSeries;
import com.hazelcast.dataseries.EntryProcessorRecipe;
import com.hazelcast.dataseries.MemoryInfo;
import com.hazelcast.dataseries.PreparedAggregation;
import com.hazelcast.dataseries.PreparedEntryProcessor;
import com.hazelcast.dataseries.PreparedProjection;
import com.hazelcast.dataseries.PreparedQuery;
import com.hazelcast.dataseries.ProjectionRecipe;
import com.hazelcast.dataseries.impl.aggregation.FetchAggregateOperationFactory;
import com.hazelcast.dataseries.impl.aggregation.PrepareAggregationOperationFactory;
import com.hazelcast.dataseries.impl.entryprocessor.PrepareEntryProcessorOperationFactory;
import com.hazelcast.dataseries.impl.operations.CountOperationFactory;
import com.hazelcast.dataseries.impl.operations.FillOperation;
import com.hazelcast.dataseries.impl.operations.FreezeOperationFactory;
import com.hazelcast.dataseries.impl.operations.InsertOperation;
import com.hazelcast.dataseries.impl.operations.IteratorOperation;
import com.hazelcast.dataseries.impl.operations.MemoryUsageOperation;
import com.hazelcast.dataseries.impl.operations.MemoryUsageOperationFactory;
import com.hazelcast.dataseries.impl.operations.PopulateOperationFactory;
import com.hazelcast.dataseries.impl.projection.PrepareProjectionOperationFactory;
import com.hazelcast.dataseries.impl.query.PrepareQueryOperationFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.function.Supplier;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class DataSeriesProxy<K, V> extends AbstractDistributedObject<DataSeriesService> implements DataSeries<K, V> {

    protected final IPartitionService partitionService;
    protected final OperationService operationService;
    protected final Object[] bogusKeys;
    private final String name;

    public DataSeriesProxy(String name, NodeEngine nodeEngine, DataSeriesService dataSeriesService) {
        super(nodeEngine, dataSeriesService);
        this.name = name;
        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();

        bogusKeys = new Object[nodeEngine.getPartitionService().getPartitionCount()];
        int count = bogusKeys.length;
        long l = 0;
        while (count > 0) {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(l);
            if (bogusKeys[partitionId] == null) {
                bogusKeys[partitionId] = bogusKeys;
                count--;
            }
            l++;
        }
    }

    @Override
    public void fill(long count, Supplier<V> supplier) {
        checkNotNull(supplier, "supplier can't be null");
        checkNotNegative(count, "count can't be smaller than 0");

        int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();
        long countPerPartition = count / partitionCount;
        long remaining = count % partitionCount;

        SerializationService ss = getNodeEngine().getSerializationService();
        List<InternalCompletableFuture> futures = new LinkedList<>();
        for (int k = 0; k < partitionCount; k++) {
            long c = k == partitionCount - 1
                    ? countPerPartition + remaining
                    : countPerPartition;

            // we need to clone the supplier
            Supplier s = ss.toObject(ss.toData(supplier));
            Operation op = new FillOperation(name, s, c)
                    .setPartitionId(k);
            InternalCompletableFuture<Object> f = operationService.invokeOnPartition(op);
            futures.add(f);
        }

        for (InternalCompletableFuture f : futures) {
            f.join();
        }
    }

    public Iterator<V> iterator(int partitionId) {
        Operation op = new IteratorOperation(name).setPartitionId(partitionId);
        return (Iterator) operationService.invokeOnPartition(op).join();
    }

    // needed for Jet integration
    public Iterator<Map.Entry> shiterator(int partitionId) {
        Iterator it = iterator(partitionId);
        Object bogusKey = bogusKeys[partitionId];
        return new Iterator<Map.Entry>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Map.Entry next() {
                Object value = it.next();
                return new Map.Entry() {
                    @Override
                    public Object getKey() {
                        return bogusKey;
                    }

                    @Override
                    public Object getValue() {
                        return value;
                    }

                    @Override
                    public Object setValue(Object value) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public void populate(IMap src) {
        checkNotNull(src, "map can't be null");

        try {
            operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new PopulateOperationFactory(name, src.getName()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void append(K partitionKey, V value) {
        appendAsync(partitionKey, value).join();
    }

    @Override
    public InternalCompletableFuture<Object> appendAsync(K partitionKey, V value) {
        checkNotNull(partitionKey, "key can't be null");
        checkNotNull(value, "value can't be null");

        Data keyData = toData(partitionKey);
        Data valueData = toData(value);

        Operation op = new InsertOperation(name, keyData, valueData)
                .setPartitionId(partitionService.getPartitionId(partitionKey));

        return operationService.invokeOnPartition(op);
    }

    @Override
    public PreparedQuery<V> prepare(Predicate query) {
        checkNotNull(query, "query can't be null");

        String preparationId = newPreparationId();

        try {
            operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new PrepareQueryOperationFactory(name, preparationId, query));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new PreparedQuery<V>(operationService, name, preparationId);
    }

    private String newPreparationId() {
        return name + "_" + UuidUtil.newUnsecureUuidString().replace("-", "");
    }

    @Override
    public PreparedEntryProcessor prepare(EntryProcessorRecipe recipe) {
        checkNotNull(recipe, "recipe can't be null");

        String preparationId = newPreparationId();

        try {
            operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new PrepareEntryProcessorOperationFactory(name, preparationId, recipe));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new PreparedEntryProcessor(operationService, name, preparationId);
    }

    @Override
    public <E> PreparedProjection<K, E> prepare(ProjectionRecipe<E> recipe) {
        checkNotNull(recipe, "projectionRecipe can't be null");

        String preparationId = newPreparationId();

        try {
            operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new PrepareProjectionOperationFactory(name, preparationId, recipe));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new PreparedProjection<K, E>(operationService, getNodeEngine(), name, preparationId, getService(), recipe);
    }

    @Override
    public <T, E> PreparedAggregation<E> prepare(AggregationRecipe<T, E> recipe) {
        checkNotNull(recipe, "aggregationRecipe can't be null");

        String preparationId = newPreparationId();

        try {
            operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new PrepareAggregationOperationFactory(name, preparationId, recipe));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new PreparedAggregation<E>(operationService, name, preparationId);
    }

    @Override
    public <E> E aggregate(String aggregatorId) {
        checkNotNull(aggregatorId, "aggregatorId can't be null");

        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new FetchAggregateOperationFactory(name, aggregatorId));

            Aggregator aggregator = null;
            for (Object v : result.values()) {
                Aggregator a = (Aggregator) v;
                if (aggregator == null) {
                    aggregator = a;
                } else {
                    aggregator.combine(a);
                }
            }

            aggregator.onCombinationFinished();
            return (E) aggregator.aggregate();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long count() {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new CountOperationFactory(name));

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
    public void freeze() {
        try {
            operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new FreezeOperationFactory(name));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MemoryInfo memoryInfo() {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSeriesService.SERVICE_NAME, new MemoryUsageOperationFactory(name));

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
        Operation op = new MemoryUsageOperation(name).setPartitionId(partitionId);
        InternalCompletableFuture<MemoryInfo> f = operationService.invokeOnPartition(op);
        return f.join();
    }

    @Override
    public String getServiceName() {
        return DataSeriesService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }
}
