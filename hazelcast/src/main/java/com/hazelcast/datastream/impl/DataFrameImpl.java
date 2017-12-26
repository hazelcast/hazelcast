/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.datastream.AggregationRecipe;
import com.hazelcast.datastream.DataFrame;
import com.hazelcast.datastream.DataStreamInfo;
import com.hazelcast.datastream.EntryProcessorRecipe;
import com.hazelcast.datastream.LongDataSeries;
import com.hazelcast.datastream.PreparedAggregation;
import com.hazelcast.datastream.PreparedEntryProcessor;
import com.hazelcast.datastream.PreparedProjection;
import com.hazelcast.datastream.PreparedQuery;
import com.hazelcast.datastream.ProjectionRecipe;
import com.hazelcast.datastream.impl.aggregation.FetchAggregateOperationFactory;
import com.hazelcast.datastream.impl.aggregation.PrepareAggregationOperationFactory;
import com.hazelcast.datastream.impl.entryprocessor.PrepareEntryProcessorOperationFactory;
import com.hazelcast.datastream.impl.operations.CountOperationFactory;
import com.hazelcast.datastream.impl.operations.FreezeOperationFactory;
import com.hazelcast.datastream.impl.operations.InfoOperation;
import com.hazelcast.datastream.impl.operations.InfoOperationFactory;
import com.hazelcast.datastream.impl.projection.PrepareProjectionOperationFactory;
import com.hazelcast.datastream.impl.query.PrepareQueryOperationFactory;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.UuidUtil;

import java.util.Map;
import java.util.concurrent.CompletionException;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DataFrameImpl<R> implements DataFrame<R> {

    private final String name;
    private final OperationService operationService;
    private final NodeEngine nodeEngine;
    private final DSService service;

    public DataFrameImpl(String name,
                         OperationService operationService,
                         NodeEngine nodeEngine,
                         DSService service) {
        this.operationService = operationService;
        this.name = name;
        this.nodeEngine = nodeEngine;
        this.service = service;
    }

    @Override
    public LongDataSeries getLongDataSeries(String field) {
        checkNotNull(field, "field can't be null");
        return new LongDataSeriesImpl(field, operationService, name);
    }

    @Override
    public PreparedQuery<R> prepare(Predicate query) {
        checkNotNull(query, "query can't be null");

        String preparationId = newPreparationId();

        try {
            operationService.invokeOnAllPartitions(
                    DSService.SERVICE_NAME, new PrepareQueryOperationFactory(name, preparationId, query));
        } catch (Exception e) {
            throw new CompletionException(e);
        }

        return new PreparedQuery<R>(operationService, name, preparationId);
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
                    DSService.SERVICE_NAME, new PrepareEntryProcessorOperationFactory(name, preparationId, recipe));
        } catch (Exception e) {
            throw new CompletionException(e);
        }

        return new PreparedEntryProcessor(operationService, name, preparationId);
    }

    @Override
    public <E> PreparedProjection<E> prepare(ProjectionRecipe<E> recipe) {
        checkNotNull(recipe, "projectionRecipe can't be null");

        String preparationId = newPreparationId();

        try {
            operationService.invokeOnAllPartitions(
                    DSService.SERVICE_NAME, new PrepareProjectionOperationFactory(name, preparationId, recipe));
        } catch (Exception e) {
            throw new CompletionException(e);
        }

        return new PreparedProjection<E>(operationService, nodeEngine, name, preparationId, service, recipe);
    }

    @Override
    public <T, E> PreparedAggregation<E> prepare(AggregationRecipe<T, E> recipe) {
        checkNotNull(recipe, "aggregationRecipe can't be null");

        String preparationId = newPreparationId();

        try {
            operationService.invokeOnAllPartitions(
                    DSService.SERVICE_NAME, new PrepareAggregationOperationFactory(name, preparationId, recipe));
        } catch (Exception e) {
            throw new CompletionException(e);
        }

        return new PreparedAggregation<E>(operationService, name, preparationId);
    }

    @Override
    public <E> E aggregate(String aggregatorId) {
        checkNotNull(aggregatorId, "aggregatorId can't be null");

        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DSService.SERVICE_NAME, new FetchAggregateOperationFactory(name, aggregatorId));

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
                    DSService.SERVICE_NAME, new CountOperationFactory(name));

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
                    DSService.SERVICE_NAME, new FreezeOperationFactory(name));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataStreamInfo memoryInfo() {
        try {
            Map<Integer, Object> result = operationService.invokeOnAllPartitions(
                    DSService.SERVICE_NAME, new InfoOperationFactory(name));

            long allocated = 0;
            long consumed = 0;
            long count = 0;
            int regionsUsed = 0;
            for (Object value : result.values()) {
                DataStreamInfo memoryInfo = (DataStreamInfo) value;
                allocated += memoryInfo.bytesAllocated();
                consumed += memoryInfo.bytesConsumed();
                regionsUsed += memoryInfo.regionsInUse();
                count += memoryInfo.count();
            }
            return new DataStreamInfo(consumed, allocated, regionsUsed, count);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataStreamInfo memoryInfo(int partitionId) {
        Operation op = new InfoOperation(name).setPartitionId(partitionId);
        InternalCompletableFuture<DataStreamInfo> f = operationService.invokeOnPartition(op);
        return f.join();
    }

}
