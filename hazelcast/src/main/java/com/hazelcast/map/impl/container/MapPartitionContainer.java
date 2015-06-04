package com.hazelcast.map.impl.container;

import com.hazelcast.map.impl.*;
import com.hazelcast.config.MapConfig;

public class MapPartitionContainer extends PartitionContainer {
    private final MapConfig mapConfig;
    private final RecordStore recordStore;
    private final MapContainer mapContainer;

    public MapPartitionContainer(MapService mapService, MapServiceContext mapServiceContext, String name, int partitionId) {
        super(mapService, partitionId);

        this.mapConfig = mapServiceContext.getMapContainer(name).getMapConfig();
        this.recordStore = mapServiceContext.getPartitionContainer(partitionId).getRecordStore(name);
        this.mapContainer = mapServiceContext.getMapContainer(name);
    }

    public MapConfig getMapConfig() {
        return this.mapConfig;
    }

    public RecordStore getPartitionLocalRecordStore() {
        return recordStore;
    }

    public MapContainer getSharedMapContainer() {
        return mapContainer;
    }
}
