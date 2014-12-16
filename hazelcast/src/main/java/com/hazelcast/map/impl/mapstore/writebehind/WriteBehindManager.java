package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.map.impl.mapstore.MapDataStore;
import com.hazelcast.map.impl.mapstore.MapDataStores;
import com.hazelcast.map.impl.mapstore.MapStoreContext;
import com.hazelcast.map.impl.mapstore.MapStoreManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.ExecutorType;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Write behind map store manager.
 */
public class WriteBehindManager implements MapStoreManager {

    private static final String EXECUTOR_NAME_PREFIX = "hz:scheduled:mapstore:";

    private static final int EXECUTOR_DEFAULT_QUEUE_CAPACITY = 10000;

    private final ScheduledExecutorService scheduledExecutor;

    private WriteBehindProcessor writeBehindProcessor;

    private StoreWorker storeWorker;

    private String executorName;

    private final MapStoreContext mapStoreContext;

    public WriteBehindManager(MapStoreContext mapStoreContext) {
        this.mapStoreContext = mapStoreContext;
        writeBehindProcessor = createWriteBehindProcessor(mapStoreContext);
        storeWorker = new StoreWorker(mapStoreContext, writeBehindProcessor);
        executorName = EXECUTOR_NAME_PREFIX + mapStoreContext.getMapName();
        final MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        scheduledExecutor = getScheduledExecutorService(mapServiceContext);
    }

    public void start() {
        scheduledExecutor.scheduleAtFixedRate(storeWorker, 1, 1, TimeUnit.SECONDS);
    }

    public void stop() {
        final MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        nodeEngine.getExecutionService().shutdownExecutor(executorName);
    }

    //todo get this via constructor function.
    @Override
    public MapDataStore getMapDataStore(int partitionId) {
        return MapDataStores.createWriteBehindStore(mapStoreContext, partitionId, writeBehindProcessor);
    }

    private WriteBehindProcessor createWriteBehindProcessor(final MapStoreContext mapStoreContext) {
        final MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        final WriteBehindProcessor writeBehindProcessor
                = WriteBehindProcessors.createWriteBehindProcessor(mapStoreContext);
        writeBehindProcessor.addStoreListener(new StoreListener<DelayedEntry>() {
            @Override
            public void beforeStore(StoreEvent<DelayedEntry> storeEvent) {

            }

            @Override
            public void afterStore(StoreEvent<DelayedEntry> storeEvent) {
                final DelayedEntry delayedEntry = storeEvent.getSource();
                final Object value = delayedEntry.getValue();
                // only process store delete operations.
                if (value != null) {
                    return;
                }
                final Data key = (Data) storeEvent.getSource().getKey();
                final int partitionId = delayedEntry.getPartitionId();
                final PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
                final RecordStore recordStore = partitionContainer.getExistingRecordStore(mapStoreContext.getMapName());
                if (recordStore != null) {
                    recordStore.getMapDataStore().addTransient(key, Clock.currentTimeMillis());
                }
            }
        });
        return writeBehindProcessor;
    }


    private ScheduledExecutorService getScheduledExecutorService(MapServiceContext mapServiceContext) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.register(executorName, 1, EXECUTOR_DEFAULT_QUEUE_CAPACITY, ExecutorType.CACHED);
        return executionService.getScheduledExecutor(executorName);
    }
}
