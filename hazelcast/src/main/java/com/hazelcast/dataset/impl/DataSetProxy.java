package com.hazelcast.dataset.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.core.IMap;
import com.hazelcast.dataset.AggregationRecipe;
import com.hazelcast.dataset.CompiledAggregation;
import com.hazelcast.dataset.CompiledEntryProcessor;
import com.hazelcast.dataset.CompiledPredicate;
import com.hazelcast.dataset.CompiledProjection;
import com.hazelcast.dataset.DataSet;
import com.hazelcast.dataset.EntryProcessorRecipe;
import com.hazelcast.dataset.ProjectionRecipe;
import com.hazelcast.dataset.impl.aggregation.AggregateOperationFactory;
import com.hazelcast.dataset.impl.aggregation.CompileAggregationOperationFactory;
import com.hazelcast.dataset.impl.aggregation.FetchAggregateOperationFactory;
import com.hazelcast.dataset.impl.entryprocessor.CompileEpOperationFactory;
import com.hazelcast.dataset.impl.operations.CountOperationFactory;
import com.hazelcast.dataset.impl.operations.InsertOperation;
import com.hazelcast.dataset.impl.operations.MemoryUsageOperationFactory;
import com.hazelcast.dataset.impl.operations.PopulateOperationFactory;
import com.hazelcast.dataset.impl.projection.CompileProjectionOperationFactory;
import com.hazelcast.dataset.impl.query.CompilePredicateOperationFactory;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.UuidUtil;

import java.util.Map;

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
    public void insert(K partitionKey, V value) {
        insertAsync(partitionKey, value).join();
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
    public CompiledPredicate<V> compile(Predicate query) {
        checkNotNull(query, "query can't be null");

        String compileId = newCompileId();

        try {
            operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new CompilePredicateOperationFactory(name, compileId, query));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new CompiledPredicate<V>(operationService, name, compileId);
    }

    private String newCompileId() {
        return name + "_" + UuidUtil.newUnsecureUuidString().replace("-", "");
    }

    @Override
    public CompiledEntryProcessor compile(EntryProcessorRecipe recipe) {
        checkNotNull(recipe, "recipe can't be null");

        String compileId = newCompileId();

        try {
            operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new CompileEpOperationFactory(name, compileId, recipe));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new CompiledEntryProcessor(operationService, name, compileId);
    }

    @Override
    public <E> CompiledProjection<K,E> compile(ProjectionRecipe<E> recipe) {
        checkNotNull(recipe, "projectionRecipe can't be null");

        String compileId = newCompileId();

        try {
            operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new CompileProjectionOperationFactory(name, compileId, recipe));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new CompiledProjection<K,E>(operationService, getNodeEngine(), name, compileId, getService(), recipe);
    }

    @Override
    public <T, E> CompiledAggregation<E> compile(AggregationRecipe<T, E> recipe) {
        checkNotNull(recipe, "aggregationRecipe can't be null");

        String compileId = newCompileId();

        try {
            operationService.invokeOnAllPartitions(
                    DataSetService.SERVICE_NAME, new CompileAggregationOperationFactory(name, compileId, recipe));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return new CompiledAggregation<E>(operationService, name, compileId);
    }

    @Override
    public <E> E aggregate(String aggregatorId) {
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
    public MemoryInfo memoryUsage() {
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
                segmentsUsed+=memoryInfo.getSegmentsInUse();
            }
            return new MemoryInfo(consumed, allocated,segmentsUsed);
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
