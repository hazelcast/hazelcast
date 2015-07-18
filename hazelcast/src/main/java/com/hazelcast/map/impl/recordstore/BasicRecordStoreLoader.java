/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.operation.PutFromLoadAllOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.spi.ExecutionService.MAP_LOADER_EXECUTOR;

/**
 * Responsible for loading keys from configured map store.
 */
class BasicRecordStoreLoader implements RecordStoreLoader {

    private final AtomicBoolean loaded;

    private final ILogger logger;

    private final String name;

    private final MapServiceContext mapServiceContext;

    private final MapDataStore mapDataStore;

    private final RecordStore recordStore;

    private final int partitionId;

    public BasicRecordStoreLoader(RecordStore recordStore) {
        this.recordStore = recordStore;
        final MapContainer mapContainer = recordStore.getMapContainer();
        this.name = mapContainer.getName();
        this.mapServiceContext = mapContainer.getMapServiceContext();
        this.partitionId = recordStore.getPartitionId();
        this.mapDataStore = recordStore.getMapDataStore();
        this.logger = mapServiceContext.getNodeEngine().getLogger(getClass());
        this.loaded = new AtomicBoolean(false);
    }

    @Override
    public Future<?> loadValues(List<Data> keys, boolean replaceExistingValues) {
        final Callable task = new GivenKeysLoaderTask(keys, replaceExistingValues);
        return executeTask(MAP_LOADER_EXECUTOR, task);
    }

    private Future<?> executeTask(String executorName, Callable task) {
        return getExecutionService().submit(executorName, task);
    }

    private ExecutionService getExecutionService() {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        return nodeEngine.getExecutionService();
    }

    /**
     * Task for loading values of given keys.
     * This task is used to make load in an outer thread instead of partition thread.
     */
    private final class GivenKeysLoaderTask implements Callable<Object> {

        private final List<Data> keys;
        private final boolean replaceExistingValues;

        private GivenKeysLoaderTask(List<Data> keys, boolean replaceExistingValues) {
            this.keys = keys;
            this.replaceExistingValues = replaceExistingValues;
        }

        @Override
        public Object call() throws Exception {
            loadValuesInternal(keys, replaceExistingValues);
            return null;
        }
    }

    private void loadValuesInternal(List<Data> keys, boolean replaceExistingValues) throws Exception {

        if (!replaceExistingValues) {
            removeExistingKeys(keys);
        }
        removeUnloadableKeys(keys);

        if (keys.isEmpty()) {
            loaded.set(true);
            return;
        }

        List<Future> futures = doBatchLoad(keys);
        for (Future future : futures) {
            future.get();
        }
    }

    private List<Future> doBatchLoad(List<Data> keys) {
        final Queue<List<Data>> batchChunks = createBatchChunks(keys);
        final int size = batchChunks.size();
        final AtomicInteger finishedBatchCounter = new AtomicInteger(size);
        List<Future> futures = new ArrayList<Future>();

        while (!batchChunks.isEmpty()) {
            final List<Data> chunk = batchChunks.poll();
            final List<Data> keyValueSequence = loadAndGet(chunk);
            if (keyValueSequence.isEmpty()) {
                if (finishedBatchCounter.decrementAndGet() == 0) {
                    loaded.set(true);
                }
                continue;
            }
            futures.add(sendOperation(keyValueSequence, finishedBatchCounter));
        }

        return futures;
    }

    private Queue<List<Data>> createBatchChunks(List<Data> keys) {
        final Queue<List<Data>> chunks = new LinkedList<List<Data>>();
        final int loadBatchSize = getLoadBatchSize();
        int page = 0;
        List<Data> tmpKeys;
        while ((tmpKeys = getBatchChunk(keys, loadBatchSize, page++)) != null) {
            chunks.add(tmpKeys);
        }
        return chunks;
    }

    private List<Data> loadAndGet(List<Data> keys) {
        Map<Object, Object> entries = Collections.emptyMap();
        try {
            entries = mapDataStore.loadAll(keys);
        } catch (Throwable t) {
            logger.warning("Could not load keys from map store", t);
            ExceptionUtil.rethrow(t);
        }
        return getKeyValueSequence(entries);
    }

    private List<Data> getKeyValueSequence(Map<Object, Object> entries) {
        if (entries == null || entries.isEmpty()) {
            return Collections.emptyList();
        }
        final List<Data> keyValueSequence = new ArrayList<Data>(entries.size());
        for (final Map.Entry<Object, Object> entry : entries.entrySet()) {
            final Object key = entry.getKey();
            final Object value = entry.getValue();
            final Data dataKey = mapServiceContext.toData(key);
            final Data dataValue = mapServiceContext.toData(value);
            keyValueSequence.add(dataKey);
            keyValueSequence.add(dataValue);
        }
        return keyValueSequence;
    }

    /**
     * Used to partition the list to chunks.
     *
     * @param list        to be paged.
     * @param batchSize   batch operation size.
     * @param chunkNumber batch chunk number.
     * @return sub-list of list if any or null.
     */
    private List<Data> getBatchChunk(List<Data> list, int batchSize, int chunkNumber) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        final int start = chunkNumber * batchSize;
        final int end = Math.min(start + batchSize, list.size());
        if (start >= end) {
            return null;
        }
        return list.subList(start, end);
    }

    private Future<?> sendOperation(List<Data> keyValueSequence, AtomicInteger finishedBatchCounter) {
        OperationService operationService = mapServiceContext.getNodeEngine().getOperationService();
        final Operation operation = createOperation(keyValueSequence, finishedBatchCounter);
        //operationService.executeOperation(operation);
        return operationService.invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
    }

    private Operation createOperation(List<Data> keyValueSequence, final AtomicInteger finishedBatchCounter) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final Operation operation = new PutFromLoadAllOperation(name, keyValueSequence);
        operation.setNodeEngine(nodeEngine);
        operation.setOperationResponseHandler(new OperationResponseHandler() {
            @Override
            public void sendResponse(Operation op, Object obj) {
                if (finishedBatchCounter.decrementAndGet() == 0) {
                    loaded.set(true);
                }
            }

            @Override
            public boolean isLocal() {
                return true;
            }
        });
        operation.setPartitionId(partitionId);
        OperationAccessor.setCallerAddress(operation, nodeEngine.getThisAddress());
        operation.setCallerUuid(nodeEngine.getLocalMember().getUuid());
        operation.setServiceName(MapService.SERVICE_NAME);
        return operation;
    }

    private void removeExistingKeys(Collection<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        final Map<Data, Record> records = recordStore.getRecordMap();
        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            final Data nextKey = iterator.next();
            if (records.containsKey(nextKey)) {
                iterator.remove();
            }
        }
    }

    private void removeUnloadableKeys(Collection<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            final Data key = iterator.next();
            if (!mapDataStore.loadable(key)) {
                iterator.remove();
            }
        }
    }

    private int getLoadBatchSize() {
        return mapServiceContext.getNodeEngine().getGroupProperties().MAP_LOAD_CHUNK_SIZE.getInteger();
    }
}
