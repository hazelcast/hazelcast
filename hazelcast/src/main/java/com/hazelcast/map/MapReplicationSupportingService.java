package com.hazelcast.map;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.operation.MergeOperation;
import com.hazelcast.map.operation.WanOriginatedDeleteOperation;
import com.hazelcast.map.wan.MapReplicationRemove;
import com.hazelcast.map.wan.MapReplicationUpdate;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.concurrent.Future;

class MapReplicationSupportingService implements ReplicationSupportingService {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    public MapReplicationSupportingService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        Object eventObject = replicationEvent.getEventObject();
        if (eventObject instanceof MapReplicationUpdate) {
            MapReplicationUpdate replicationUpdate = (MapReplicationUpdate) eventObject;
            EntryView entryView = replicationUpdate.getEntryView();
            MapMergePolicy mergePolicy = replicationUpdate.getMergePolicy();
            String mapName = replicationUpdate.getMapName();
            MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
            MergeOperation operation = new MergeOperation(mapName, mapServiceContext.toData(entryView.getKey(),
                    mapContainer.getPartitioningStrategy()), entryView, mergePolicy);
            try {
                int partitionId = nodeEngine.getPartitionService().getPartitionId(entryView.getKey());
                Future f = nodeEngine.getOperationService()
                        .invokeOnPartition(mapServiceContext.serviceName(), operation, partitionId);
                f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        } else if (eventObject instanceof MapReplicationRemove) {
            MapReplicationRemove replicationRemove = (MapReplicationRemove) eventObject;
            WanOriginatedDeleteOperation operation = new WanOriginatedDeleteOperation(replicationRemove.getMapName(),
                    replicationRemove.getKey());
            try {
                int partitionId = nodeEngine.getPartitionService().getPartitionId(replicationRemove.getKey());
                Future f = nodeEngine.getOperationService()
                        .invokeOnPartition(mapServiceContext.serviceName(), operation, partitionId);
                f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }
}
