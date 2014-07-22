package com.hazelcast.map;

import com.hazelcast.logging.ILogger;
import com.hazelcast.map.mapstore.MapDataStore;
import com.hazelcast.map.operation.PutAllOperation;
import com.hazelcast.map.operation.PutFromLoadAllOperation;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    private volatile Throwable throwable;

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
    public boolean isLoaded() {
        return loaded.get();
    }

    @Override
    public void setLoaded(boolean loaded) {
        this.loaded.set(loaded);
    }

    @Override
    public void loadKeys(List<Data> keys, boolean replaceExistingValues) {
        setLoaded(false);
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.submit("hz:map-loadAllKeys", new LoadAllKeysTask(keys, replaceExistingValues));
    }

    @Override
    public void loadAllKeys() {
        if (isLoaded()) {
            return;
        }
        if (isMigrating()) {
            return;
        }
        if (!isOwner()) {
            setLoaded(true);
            return;
        }
        final Map<Data, Object> loadedKeys = recordStore.getMapContainer().getInitialKeys();
        if (loadedKeys == null || loadedKeys.isEmpty()) {
            setLoaded(true);
            return;
        }
        doChunkedLoad(loadedKeys, mapServiceContext.getNodeEngine());
    }

    @Override
    public Throwable getExceptionOrNull() {
        return throwable;
    }

    private boolean isMigrating() {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final InternalPartition partition = nodeEngine.getPartitionService().getPartition(partitionId);
        return partition.isMigrating();
    }

    private boolean isOwner() {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final Address partitionOwner = nodeEngine.getPartitionService().getPartitionOwner(partitionId);
        return nodeEngine.getThisAddress().equals(partitionOwner);
    }

    /**
     * Task for loading keys.
     * This task is used to make load in an outer thread instead of partition thread.
     */
    private final class LoadAllKeysTask implements Runnable {

        private final List<Data> keys;
        private final boolean replaceExistingValues;

        private LoadAllKeysTask(List<Data> keys, boolean replaceExistingValues) {
            this.keys = keys;
            this.replaceExistingValues = replaceExistingValues;
        }

        @Override
        public void run() {
            loadKeysInternal(keys, replaceExistingValues);
        }
    }

    private void loadKeysInternal(List<Data> keys, boolean replaceExistingValues) {
        if (!replaceExistingValues) {
            removeExistingKeys(keys);
        }
        removeUnloadableKeys(keys);

        if (keys.isEmpty()) {
            loaded.set(true);
            return;
        }
        doBatchLoad(keys);
    }

    private void doBatchLoad(List<Data> keys) {
        final Queue<List<Data>> batchChunks = createBatchChunks(keys);
        final int size = batchChunks.size();
        final AtomicInteger finishedBatchCounter = new AtomicInteger(size);
        while (!batchChunks.isEmpty()) {
            final List<Data> chunk = batchChunks.poll();
            final List<Data> keyValueSequence = loadAndGet(chunk);
            if (keyValueSequence.isEmpty()) {
                if (finishedBatchCounter.decrementAndGet() == 0) {
                    loaded.set(true);
                }
                continue;
            }
            sendOperation(keyValueSequence, finishedBatchCounter);
        }
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

    private void sendOperation(List<Data> keyValueSequence, AtomicInteger finishedBatchCounter) {
        OperationService operationService = mapServiceContext.getNodeEngine().getOperationService();
        final Operation operation = createOperation(keyValueSequence, finishedBatchCounter);
        operationService.executeOperation(operation);
    }

    private Operation createOperation(List<Data> keyValueSequence, final AtomicInteger finishedBatchCounter) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final Operation operation = new PutFromLoadAllOperation(name, keyValueSequence);
        operation.setNodeEngine(nodeEngine);
        operation.setResponseHandler(new ResponseHandler() {
            @Override
            public void sendResponse(Object obj) {
                if (finishedBatchCounter.decrementAndGet() == 0) {
                    loaded.set(true);
                }
            }

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
        final long now = getNow();
        final Iterator<Data> iterator = keys.iterator();
        while (iterator.hasNext()) {
            final Data key = iterator.next();
            final Record record = recordStore.getRecord(key);
            final long lastUpdateTime = record == null ? 0L : record.getLastUpdateTime();
            if (!mapDataStore.loadable(key, lastUpdateTime, now)) {
                iterator.remove();
            }
        }
    }

    private int getLoadBatchSize() {
        return mapServiceContext.getNodeEngine().getGroupProperties().MAP_LOAD_CHUNK_SIZE.getInteger();
    }

    private long getNow() {
        return Clock.currentTimeMillis();
    }

    private void doChunkedLoad(Map<Data, Object> loadedKeys, NodeEngine nodeEngine) {
        final int mapLoadChunkSize = getLoadBatchSize();
        final Queue<Map> chunks = new LinkedList<Map>();
        Map<Data, Object> partitionKeys = new HashMap<Data, Object>();
        final int partitionId = this.partitionId;
        Iterator<Map.Entry<Data, Object>> iterator = loadedKeys.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<Data, Object> entry = iterator.next();
            final Data data = entry.getKey();
            if (partitionId == nodeEngine.getPartitionService().getPartitionId(data)) {
                partitionKeys.put(data, entry.getValue());
                //split into chunks
                if (partitionKeys.size() >= mapLoadChunkSize) {
                    chunks.add(partitionKeys);
                    partitionKeys = new HashMap<Data, Object>();
                }
                iterator.remove();
            }
        }
        if (!partitionKeys.isEmpty()) {
            chunks.add(partitionKeys);
        }
        if (chunks.isEmpty()) {
            setLoaded(true);
            return;
        }
        try {
            this.throwable = null;
            final AtomicInteger checkIfMapLoaded = new AtomicInteger(chunks.size());
            ExecutionService executionService = nodeEngine.getExecutionService();
            Map<Data, Object> chunkedKeys;
            while ((chunkedKeys = chunks.poll()) != null) {
                final Callback<Throwable> callback = createCallbackForThrowable();
                executionService.submit("hz:map-load", new MapLoadAllTask(chunkedKeys, checkIfMapLoaded, callback));
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    private Callback<Throwable> createCallbackForThrowable() {
        return new Callback<Throwable>() {
            @Override
            public void notify(Throwable throwable) {
                BasicRecordStoreLoader.this.throwable = throwable;
            }
        };
    }

    private final class MapLoadAllTask implements Runnable {
        private final Map<Data, Object> keys;
        private final AtomicInteger checkIfMapLoaded;
        private final Callback callback;

        private MapLoadAllTask(Map<Data, Object> keys, AtomicInteger checkIfMapLoaded, Callback callback) {
            this.keys = keys;
            this.checkIfMapLoaded = checkIfMapLoaded;
            this.callback = callback;
        }

        public void run() {
            final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
            try {
                Map values = mapDataStore.loadAll(keys.values());
                if (values == null || values.isEmpty()) {
                    decrementCounterAndMarkAsLoaded();
                    return;
                }
                final MapEntrySet entrySet = new MapEntrySet();
                for (Data dataKey : keys.keySet()) {
                    Object key = keys.get(dataKey);
                    Object value = values.get(key);
                    if (value != null) {
                        Data dataValue = mapServiceContext.toData(value);
                        entrySet.add(dataKey, dataValue);
                    }
                }
                PutAllOperation operation = new PutAllOperation(name, entrySet, true);
                final OperationService operationService = nodeEngine.getOperationService();
                operationService.createInvocationBuilder(MapService.SERVICE_NAME, operation, partitionId)
                        .setCallback(new Callback<Object>() {
                            @Override
                            public void notify(Object obj) {
                                if (obj instanceof Throwable) {
                                    return;
                                }
                                decrementCounterAndMarkAsLoaded();
                            }
                        }).invoke();
            } catch (Throwable t) {
                decrementCounterAndMarkAsLoaded();
                logger.warning("Exception while load all task:" + t.toString());
                callback.notify(t);
            }
        }

        private void decrementCounterAndMarkAsLoaded() {
            if (checkIfMapLoaded.decrementAndGet() == 0) {
                setLoaded(true);
            }
        }
    }
}
