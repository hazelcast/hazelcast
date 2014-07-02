package com.hazelcast.map.mapstore.writebehind;

import com.hazelcast.map.MapContainer;
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.PartitionContainer;
import com.hazelcast.map.RecordStore;
import com.hazelcast.map.mapstore.MapDataStore;
import com.hazelcast.map.mapstore.MapDataStores;
import com.hazelcast.map.mapstore.MapStoreManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.concurrent.TimeUnit;

/**
 * Write behind map store manager.
 */
public class WriteBehindManager implements MapStoreManager {

    private static final String EXECUTOR_NAME_PREFIX = "hz:scheduled:mapstore:";

    private static final int EXECUTOR_DEFAULT_QUEUE_CAPACITY = 10000;

    private WriteBehindProcessor writeBehindProcessor;

    private StoreWorker storeWorker;

    private MapContainer mapContainer;

    private ExecutionService executionService;

    private String executorName;


    public WriteBehindManager(MapContainer mapContainer) {
        this.mapContainer = mapContainer;
        writeBehindProcessor = createWriteBehindProcessor(mapContainer);
        storeWorker = new StoreWorker(mapContainer, writeBehindProcessor);
        MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        executionService = nodeEngine.getExecutionService();
        executorName = EXECUTOR_NAME_PREFIX + mapContainer.getName();
    }

    public void start() {
        executionService.getScheduledExecutor(executorName)
                .scheduleAtFixedRate(storeWorker, 1, 1, TimeUnit.SECONDS);
    }

    public void stop() {
        executionService.shutdownExecutor(executorName);
    }

    @Override
    public MapDataStore getMapDataStore(int partitionId) {
        return MapDataStores.createWriteBehindStore(mapContainer, partitionId, writeBehindProcessor);
    }

    private WriteBehindProcessor createWriteBehindProcessor(final MapContainer mapContainer) {
        final MapServiceContext mapServiceContext = mapContainer.getMapServiceContext();
        final WriteBehindProcessor writeBehindProcessor
                = WriteBehindProcessors.createWriteBehindProcessor(mapContainer);
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
                final RecordStore recordStore = partitionContainer.getExistingRecordStore(mapContainer.getName());
                if (recordStore != null) {
                    recordStore.getMapDataStore().addTransient(key, Clock.currentTimeMillis());
                }
            }
        });
        return writeBehindProcessor;
    }

}
