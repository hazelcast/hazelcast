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
import com.hazelcast.util.executor.ExecutorType;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.mapstore.writebehind.WriteBehindProcessors.createWriteBehindProcessor;

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
        this.writeBehindProcessor = newWriteBehindProcessor(mapStoreContext);
        this.storeWorker = new StoreWorker(mapStoreContext, writeBehindProcessor);
        this.executorName = EXECUTOR_NAME_PREFIX + mapStoreContext.getMapName();
        final MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
        this.scheduledExecutor = getScheduledExecutorService(mapServiceContext);
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

    private WriteBehindProcessor newWriteBehindProcessor(final MapStoreContext mapStoreContext) {
        WriteBehindProcessor writeBehindProcessor = createWriteBehindProcessor(mapStoreContext);
        StoreListener<DelayedEntry> storeListener = new InternalStoreListener(mapStoreContext);
        writeBehindProcessor.addStoreListener(storeListener);
        return writeBehindProcessor;
    }

    private ScheduledExecutorService getScheduledExecutorService(MapServiceContext mapServiceContext) {
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        final ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.register(executorName, 1, EXECUTOR_DEFAULT_QUEUE_CAPACITY, ExecutorType.CACHED);
        return executionService.getScheduledExecutor(executorName);
    }

    /**
     * Store listener which is responsible for
     * {@link com.hazelcast.map.impl.mapstore.writebehind.WriteBehindStore#stagingArea cleaning.
     */
    private static class InternalStoreListener implements StoreListener<DelayedEntry> {

        private final MapStoreContext mapStoreContext;

        public InternalStoreListener(MapStoreContext mapStoreContext) {
            this.mapStoreContext = mapStoreContext;
        }

        @Override
        public void beforeStore(StoreEvent<DelayedEntry> storeEvent) {

        }

        /**
         * Here we are cleaning staging area upon a store operation.
         */
        @Override
        public void afterStore(StoreEvent<DelayedEntry> storeEvent) {
            final DelayedEntry delayedEntry = storeEvent.getSource();
            int partitionId = delayedEntry.getPartitionId();
            WriteBehindStore writeBehindStore = getWriteBehindStoreOrNull(partitionId);
            if (writeBehindStore == null) {
                return;
            }
            Data key = (Data) delayedEntry.getKey();
            Object value = delayedEntry.getValue();
            // remove from additions.
            writeBehindStore.removeFromStagingArea(key, value);
            // remove from deletions.
            if (value == null) {
                writeBehindStore.removeFromWaitingDeletions(key);
            }
        }

        private WriteBehindStore getWriteBehindStoreOrNull(int partitionId) {
            MapStoreContext mapStoreContext = this.mapStoreContext;
            MapServiceContext mapServiceContext = mapStoreContext.getMapServiceContext();
            PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
            RecordStore recordStore = partitionContainer.getExistingRecordStore(mapStoreContext.getMapName());
            if (recordStore == null) {
                return null;
            }
            return (WriteBehindStore) recordStore.getMapDataStore();
        }

    }
}
