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

package com.hazelcast.map.impl.querycache.subscriber.operation;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryEngine;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.query.Target;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.event.QueryCacheEventData;
import com.hazelcast.map.impl.querycache.publisher.MapListenerRegistry;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.QueryCacheListenerRegistry;
import com.hazelcast.map.impl.querycache.utils.QueryCacheUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.FutureUtil.returnWithDeadline;

/**
 * An idempotent create operation which creates
 * publisher side functionality. And also responsible
 * for running initial snapshot creation phase.
 */
public class PublisherCreateOperation extends AbstractNamedOperation {

    private static final long ACCUMULATOR_READ_OPERATION_TIMEOUT_MINUTES = 5;

    private AccumulatorInfo info;

    private transient QueryResult queryResult;

    public PublisherCreateOperation() {
    }

    public PublisherCreateOperation(AccumulatorInfo info) {
        super(info.getMapName());
        this.info = info;
    }

    @Override
    public void run() {
        boolean populate = info.isPopulate();
        if (populate) {
            info.setPublishable(false);
        }
        init();
        if (populate) {
            this.queryResult = createSnapshot();
        } else {
            this.queryResult = null;
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(info);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        info = in.readObject();
    }

    @Override
    public Object getResponse() {
        return queryResult;
    }

    private void init() {
        registerAccumulatorInfo();
        registerPublisherAccumulator();
        registerLocalIMapListener();
    }

    private void registerLocalIMapListener() {
        String mapName = info.getMapName();
        String cacheId = info.getCacheId();
        PublisherContext publisherContext = getPublisherContext();
        MapListenerRegistry registry = publisherContext.getMapListenerRegistry();
        QueryCacheListenerRegistry listenerRegistry = registry.getOrCreate(mapName);
        listenerRegistry.getOrCreate(cacheId);
    }

    private void registerAccumulatorInfo() {
        String mapName = info.getMapName();
        String cacheId = info.getCacheId();
        PublisherContext publisherContext = getPublisherContext();
        AccumulatorInfoSupplier infoSupplier = publisherContext.getAccumulatorInfoSupplier();
        infoSupplier.putIfAbsent(mapName, cacheId, info);
    }

    private void registerPublisherAccumulator() {
        String mapName = info.getMapName();
        String cacheId = info.getCacheId();
        PublisherContext publisherContext = getPublisherContext();
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrCreate(mapName);
        // If InternalQueryCache#recreate is called, we forcibly
        // remove and recreate registration matching with cacheId
        publisherRegistry.remove(cacheId);
        PartitionAccumulatorRegistry partitionAccumulatorRegistry = publisherRegistry.getOrCreate(cacheId);

        partitionAccumulatorRegistry.setUuid(getCallerUuid());
    }

    private PublisherContext getPublisherContext() {
        QueryCacheContext queryCacheContext = getContext();
        return queryCacheContext.getPublisherContext();
    }

    private QueryCacheContext getContext() {
        return getMapServiceContext().getQueryCacheContext();
    }

    private MapServiceContext getMapServiceContext() {
        MapService mapService = getService();
        return mapService.getMapServiceContext();
    }

    private QueryResult createSnapshot() {
        try {
            QueryResult queryResult = runInitialQuery();
            replayEventsOverResultSet(queryResult);
            return queryResult;
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private QueryResult runInitialQuery() {
        QueryEngine queryEngine = getMapServiceContext().getQueryEngine(name);
        IterationType iterationType = info.isIncludeValue() ? IterationType.ENTRY : IterationType.KEY;
        Query query = Query.of().mapName(name).predicate(info.getPredicate()).iterationType(iterationType).build();
        return queryEngine.execute(query, Target.LOCAL_NODE);
    }

    /**
     * Replay events over the result set of initial query. These events are
     * received events during execution of the initial query.
     */
    private void replayEventsOverResultSet(QueryResult queryResult) throws Exception {
        Map<Integer, Future<Object>> future = readAccumulators();
        for (Map.Entry<Integer, Future<Object>> entry : future.entrySet()) {
            int partitionId = entry.getKey();
            Object eventsInOneAcc = entry.getValue().get();
            if (eventsInOneAcc == null) {
                continue;
            }
            eventsInOneAcc = getContext().toObject(eventsInOneAcc);
            List<QueryCacheEventData> eventDataList = (List<QueryCacheEventData>) eventsInOneAcc;
            for (QueryCacheEventData eventData : eventDataList) {
                if (eventData.getDataKey() == null) {
                    // this means a map-wide event like clear-all or evict-all
                    // is inside the accumulator buffers
                    removePartitionResults(queryResult, partitionId);
                } else {
                    add(queryResult, newQueryResultRow(eventData));
                }
            }
        }
    }

    /**
     * Remove matching entries from given result set with the given
     * partition ID.
     */
    private void removePartitionResults(QueryResult queryResult, int partitionId) {
        List<QueryResultRow> rows = queryResult.getRows();
        rows.removeIf(resultRow -> getPartitionId(resultRow) == partitionId);
    }

    private int getPartitionId(QueryResultRow resultRow) {
        return getNodeEngine().getPartitionService().getPartitionId(resultRow.getKey());
    }

    private Map<Integer, Future<Object>> readAccumulators() {
        String mapName = info.getMapName();
        String cacheId = info.getCacheId();

        Collection<Integer> partitionIds = getPartitionIdsOfAccumulators();
        if (partitionIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<Integer, Future<Object>> futuresByPartitionId
                = new Int2ObjectHashMap<>(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            futuresByPartitionId.put(partitionId,
                    readAndResetAccumulator(mapName, cacheId, partitionId));
        }

        waitResult(futuresByPartitionId.values());
        return futuresByPartitionId;
    }

    /**
     * Read and reset the accumulator of query cache inside the given partition.
     */
    private Future<Object> readAndResetAccumulator(String mapName, String cacheId, Integer partitionId) {
        Operation operation = new ReadAndResetAccumulatorOperation(mapName, cacheId);
        OperationService operationService = getNodeEngine().getOperationService();
        return operationService.invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
    }

    private void add(QueryResult result, QueryResultRow row) {
        // row in the queryResultSet and new row is compared by the keyData of QueryResultEntryImpl instances.
        // values of the entries may be different if keyData-s are equal
        // so this `if` checks the existence of keyData in the set. If it is there, just removing it and adding
        // `the new row with the same keyData but possibly with the new value`.
        // TODO can cause duplicate event receive on client side
        //if (queryResultSet.contains(row)) {
        //    queryResultSet.remove(row);
        //}
        result.addRow(row);
    }

    private QueryResultRow newQueryResultRow(QueryCacheEventData eventData) {
        Data dataKey = eventData.getDataKey();
        Data dataNewValue = eventData.getDataNewValue();
        return new QueryResultRow(dataKey, dataNewValue);
    }

    private Collection<Integer> getPartitionIdsOfAccumulators() {
        String mapName = info.getMapName();
        String cacheId = info.getCacheId();
        QueryCacheContext context = getContext();
        return QueryCacheUtil.getAccumulators(context, mapName, cacheId).keySet();
    }

    private static Collection<Object> waitResult(Collection<Future<Object>> lsFutures) {
        return returnWithDeadline(lsFutures, ACCUMULATOR_READ_OPERATION_TIMEOUT_MINUTES,
                TimeUnit.MINUTES, FutureUtil.RETHROW_EVERYTHING);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PUBLISHER_CREATE;
    }
}
