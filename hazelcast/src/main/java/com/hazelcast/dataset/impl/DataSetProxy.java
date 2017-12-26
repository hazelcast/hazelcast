package com.hazelcast.dataset.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.IMap;
import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.DataSet;
import com.hazelcast.dataset.EntryProcessorRecipe;
import com.hazelcast.dataset.PreparedAggregation;
import com.hazelcast.dataset.PreparedEntryProcessor;
import com.hazelcast.dataset.PreparedProjection;
import com.hazelcast.dataset.PreparedQuery;
import com.hazelcast.dataset.ProjectionRecipe;
import com.hazelcast.dataset.impl.aggregation.FetchAggregateOperationFactory;
import com.hazelcast.dataset.impl.aggregation.PrepareAggregationOperationFactory;
import com.hazelcast.dataset.impl.entryprocessor.PrepareEntryProcessorOperationFactory;
import com.hazelcast.dataset.impl.operations.CountOperationFactory;
import com.hazelcast.dataset.impl.operations.FreezeOperationFactory;
import com.hazelcast.dataset.impl.operations.InsertOperation;
import com.hazelcast.dataset.impl.operations.MemoryUsageOperationFactory;
import com.hazelcast.dataset.impl.operations.PopulateOperationFactory;
import com.hazelcast.dataset.impl.operations.FillOperation;
import com.hazelcast.dataset.impl.projection.PrepareProjectionOperationFactory;
import com.hazelcast.dataset.impl.query.PrepareQueryOperationFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.function.Supplier;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class DataSetProxy<K, V> extends AbstractDistributedObject<DataSetService> implements DataSet<K, V> {

    private final String name;
    protected final IPartitionService partitionService;
    protected final OperationService operationService;

    public DataSetProxy(String name, NodeEngine nodeEngine, DataSetService dataSetService) {
        super(nodeEngine, dataSetService);
        this.name = name;
        this.partitionService = nodeEngine.getPartitionService();
        this.operationService = nodeEngine.getOperationService();
    }

    @Override
    public void fill(long count, Supplier<V> supplier) {
        checkNotNull(supplier, "supplier can't be null");
        checkNotNegative(count, "count can't be smaller than 0");

        int partitionCount = getNodeEngine().getPartitionService().getPartitionCount();
        long countPerPartition = count / partitionCount;
        long remaining = count % partitionCount;

        List<InternalCompletableFuture> futures = new LinkedList<>();
        for (int k = 0; k < partitionCount; k++) {
            long c = k == partitionCount - 1
                    ? countPerPartition + remaining
                    : countPerPartition;
            Operation op = new FillOperation(name, supplier, c).setPartitionId(k);
            InternalCompletableFuture<Object> f = operationService.invokeOnPartition(op);
            futures.add(f);
        }

        for (InternalCompletableFuture f : futures) {
            f.join();
        }
    }

    public void populate(IMap map) {
        checkNotNull(map, "map can't be null");

        try {
            operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new PopulateOperationFactory(name, map.getName()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insert(K partitionKey, V value) {
        insertAsync(partitionKey, value).join();
    }

    @Override
    public InternalCompletableFuture<Object> insertAsync(K partitionKey, V value) {
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
                    DataSetService.SERVICE_NAME, new PrepareQueryOperationFactory(name, preparationId, query));
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
                    DataSetService.SERVICE_NAME, new PrepareEntryProcessorOperationFactory(name, preparationId, recipe));
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
                    DataSetService.SERVICE_NAME, new PrepareProjectionOperationFactory(name, preparationId, recipe));
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
                    DataSetService.SERVICE_NAME, new PrepareAggregationOperationFactory(name, preparationId, recipe));
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
                    DataSetService.SERVICE_NAME, new FetchAggregateOperationFactory(name, aggregatorId));

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
                    DataSetService.SERVICE_NAME, new CountOperationFactory(name));

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
                    DataSetService.SERVICE_NAME, new FreezeOperationFactory(name));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MemoryInfo memoryInfo() {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new MemoryUsageOperationFactory(name));

            long allocated = 0;
            long consumed = 0;
            int segmentsUsed = 0;
            for (Object value : result.values()) {
                MemoryInfo memoryInfo = (MemoryInfo) value;
                allocated += memoryInfo.allocatedBytes();
                consumed += memoryInfo.consumedBytes();
                segmentsUsed += memoryInfo.segmentsInUse();
            }
            return new MemoryInfo(consumed, allocated, segmentsUsed);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getServiceName() {
        return DataSetService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }
}
