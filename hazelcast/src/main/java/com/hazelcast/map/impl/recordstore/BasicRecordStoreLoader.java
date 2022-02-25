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

package com.hazelcast.map.impl.recordstore;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.operation.RemoveFromLoadAllOperation;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationAccessor;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.internal.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import static com.hazelcast.spi.impl.executionservice.ExecutionService.MAP_LOADER_EXECUTOR;

/**
 * Responsible for loading keys from configured map store for a single partition.
 */
class BasicRecordStoreLoader implements RecordStoreLoader {
    protected final String name;
    protected final MapServiceContext mapServiceContext;
    private final ILogger logger;
    private final MapDataStore mapDataStore;
    private final int partitionId;

    BasicRecordStoreLoader(RecordStore recordStore) {
        final MapContainer mapContainer = recordStore.getMapContainer();
        this.name = mapContainer.getName();
        this.mapServiceContext = mapContainer.getMapServiceContext();
        this.partitionId = recordStore.getPartitionId();
        this.mapDataStore = recordStore.getMapDataStore();
        this.logger = mapServiceContext.getNodeEngine().getLogger(getClass());
    }

    /**
     * {@inheritDoc}
     * <p>
     * Offloads the value loading task to the
     * {@link ExecutionService#MAP_LOADER_EXECUTOR} executor.
     */
    @Override
    public Future<?> loadValues(List<Data> keys, boolean replaceExistingValues) {
        Callable task = new GivenKeysLoaderTask(keys, replaceExistingValues);
        return executeTask(MAP_LOADER_EXECUTOR, task);
    }

    private Future<?> executeTask(String executorName, Callable task) {
        return getExecutionService().submit(executorName, task);
    }

    private ExecutionService getExecutionService() {
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        return nodeEngine.getExecutionService();
    }

    /**
     * Task for loading values of given keys and dispatching the loaded key-value
     * pairs to the partition threads to update the record stores.
     * <p>
     * This task is used to load the values on a thread different than the partition thread.
     *
     * @see MapLoader#loadAll(Collection)
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

    /**
     * Loads the values for the provided keys and invokes partition operations
     * to put the loaded entries into the partition record store. The method
     * will block until all entries have been put into the partition record store.
     * <p>
     * Unloadable keys will be removed before loading the values. Also, if
     * {@code replaceExistingValues} is {@code false}, the list of keys will
     * be filtered for existing keys in the partition record store.
     *
     * @param keys                  the keys for which values will be loaded
     * @param replaceExistingValues if the existing entries for the keys should
     *                              be replaced with the loaded values
     * @throws Exception if there is an exception when invoking partition operations
     *                   to remove the existing keys or to put the loaded values into
     *                   the record store
     */
    private void loadValuesInternal(List<Data> keys, boolean replaceExistingValues) throws Exception {
        if (!replaceExistingValues) {
            Future removeKeysFuture = removeExistingKeys(keys);
            removeKeysFuture.get();
        }
        removeUnloadableKeys(keys);
        if (keys.isEmpty()) {
            return;
        }
        List<Future> futures = doBatchLoad(keys);
        for (Future future : futures) {
            future.get();
        }
    }

    /**
     * Removes keys already present in the partition record store from
     * the provided keys list.
     * This is done by sending a partition operation. This operation is
     * supposed to be invoked locally and the provided parameter is supposed
     * to be thread-safe as it will be mutated directly from the partition
     * thread.
     *
     * @param keys the keys to be filtered
     * @return the future representing the pending completion of the key
     * filtering task
     */
    private Future removeExistingKeys(List<Data> keys) {
        OperationService operationService = mapServiceContext.getNodeEngine().getOperationService();
        Operation operation = new RemoveFromLoadAllOperation(name, keys);
        return operationService.invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
    }

    /**
     * Loads the values for the provided keys in batches and invokes
     * partition operations to put the loaded entry batches into the
     * record store.
     *
     * @param keys the keys for which entries are loaded and put into the
     *             record store
     * @return the list of futures representing the pending completion of
     * the operations storing the loaded entries into the partition record
     * store
     */
    private List<Future> doBatchLoad(List<Data> keys) {
        Queue<List<Data>> batchChunks = createBatchChunks(keys);
        int size = batchChunks.size();
        List<Future> futures = new ArrayList<>(size);

        while (!batchChunks.isEmpty()) {
            List<Data> chunk = batchChunks.poll();
            List<Data> loadingSequence = loadAndGet(chunk);
            if (loadingSequence.isEmpty()) {
                continue;
            }
            futures.add(sendOperation(loadingSequence));
        }

        return futures;
    }

    /**
     * Returns a queue of key batches
     *
     * @param keys the keys to be batched
     */
    private Queue<List<Data>> createBatchChunks(List<Data> keys) {
        Queue<List<Data>> chunks = new LinkedList<>();
        int loadBatchSize = getLoadBatchSize();
        int page = 0;
        List<Data> tmpKeys;
        while ((tmpKeys = getBatchChunk(keys, loadBatchSize, page++)) != null) {
            chunks.add(tmpKeys);
        }
        return chunks;
    }

    /**
     * Loads the provided keys from the underlying map store
     * and transforms them to a list of alternating serialised key-value pairs.
     *
     * @param keys the keys for which values are loaded
     * @return the list of loaded key-values
     * @see MapLoader#loadAll(Collection)
     */
    private List<Data> loadAndGet(List<Data> keys) {
        try {
            Map entries = mapDataStore.loadAll(keys);
            return getLoadingSequence(entries);
        } catch (Throwable t) {
            logger.warning("Could not load keys from map store", t);
            throw ExceptionUtil.rethrow(t);
        }
    }

    /**
     * Transforms a map to a list of serialised alternating key-value pairs.
     *
     * @param entries the map to be transformed
     * @return the list of serialised alternating key-value pairs
     */
    protected List<Data> getLoadingSequence(Map<?, ?> entries) {
        if (entries == null || entries.isEmpty()) {
            return Collections.emptyList();
        }
        List<Data> keyValueSequence = new ArrayList<>(entries.size() * 2);
        for (Map.Entry<?, ?> entry : entries.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            Data dataKey = mapServiceContext.toData(key);
            Data dataValue = mapServiceContext.toData(value);
            keyValueSequence.add(dataKey);
            keyValueSequence.add(dataValue);
        }
        return keyValueSequence;
    }

    /**
     * Returns an operation to put the provided key-value-(expirationTime)
     * sequences into the partition record store.
     *
     * @param loadingSequence the list of serialised alternating key-value pairs
     */
    protected Operation createOperation(List<Data> loadingSequence) {
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(name);
        return operationProvider.createPutFromLoadAllOperation(name, loadingSequence, false);
    }

    /**
     * Returns a sublist (page) of items in the provided list. The start and
     * end index of the sublist are determined by the {@code pageSize} and
     * {@code pageNumber} parameters. This method may be used to paginate
     * through the entire list - the {@code pageSize} should be kept the same
     * and the {@code pageNumber} should be incremented from {@code 0} until
     * the returned sublist is {@code null}.
     *
     * @param list       list to be paged
     * @param pageSize   size of a page batch operation size
     * @param pageNumber batch chunk number
     * @return a sublist of items or {@code null} if the provided list is empty or
     * the sub-list does not contain any items
     */
    private List<Data> getBatchChunk(List<Data> list, int pageSize, int pageNumber) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        int start = pageNumber * pageSize;
        int end = Math.min(start + pageSize, list.size());
        if (start >= end) {
            return null;
        }
        return list.subList(start, end);
    }

    /**
     * Invokes an operation to put the provided key-value pairs to the partition
     * record store.
     *
     * @param loadingSequence the list of serialised key-value-(expirationTime)
     *                        sequences
     * @return the future representing the pending completion of the put operation
     */
    private Future<?> sendOperation(List<Data> loadingSequence) {
        OperationService operationService = mapServiceContext.getNodeEngine().getOperationService();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        Operation operation = createOperation(loadingSequence);
        operation.setNodeEngine(nodeEngine);
        operation.setPartitionId(partitionId);
        OperationAccessor.setCallerAddress(operation, nodeEngine.getThisAddress());
        operation.setCallerUuid(nodeEngine.getLocalMember().getUuid());
        operation.setServiceName(MapService.SERVICE_NAME);
        return operationService.invokeOnPartition(MapService.SERVICE_NAME, operation, partitionId);
    }

    /**
     * Removes unloadable keys from the provided key collection.
     *
     * @see MapDataStore#loadable(Object)
     */
    private void removeUnloadableKeys(Collection<Data> keys) {
        if (keys == null || keys.isEmpty()) {
            return;
        }
        keys.removeIf(key -> !mapDataStore.loadable(key));
    }

    /**
     * Returns the size of the key batch for which values are loaded.
     * Each partition may load this many values at any moment independently
     * of other value loading tasks on other partitions.
     */
    private int getLoadBatchSize() {
        return mapServiceContext.getNodeEngine().getProperties().getInteger(ClusterProperty.MAP_LOAD_CHUNK_SIZE);
    }
}
