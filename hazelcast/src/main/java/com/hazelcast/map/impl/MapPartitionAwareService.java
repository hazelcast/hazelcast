package com.hazelcast.map.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PartitionAwareService;

import java.util.Map.Entry;

/**
 * Defines partition-aware operations' behavior of map service.
 * Currently, it only defines the behavior for partition lost occurrences
 *
 * @see com.hazelcast.partition.InternalPartitionLostEvent
 */
class MapPartitionAwareService implements PartitionAwareService {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    public MapPartitionAwareService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @Override
    public void onPartitionLost(InternalPartitionLostEvent partitionLostEvent) {
        final Address thisAddress = nodeEngine.getThisAddress();
        final int partitionId = partitionLostEvent.getPartitionId();

        for (Entry<String, MapContainer> entry : mapServiceContext.getMapContainers().entrySet()) {
            final String mapName = entry.getKey();
            final MapContainer mapContainer = entry.getValue();

            if (mapContainer.getBackupCount() <= partitionLostEvent.getLostReplicaIndex()) {
                mapServiceContext.getMapEventPublisher().publishMapPartitionLostEvent(thisAddress, mapName, partitionId);
            }
        }
    }

}
