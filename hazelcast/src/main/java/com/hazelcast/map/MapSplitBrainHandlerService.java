package com.hazelcast.map;

import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.operation.MergeOperation;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Future;

class MapSplitBrainHandlerService implements SplitBrainHandlerService {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    public MapSplitBrainHandlerService(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @Override
    public Runnable prepareMergeRunnable() {
        final long now = getNow();

        final Map<String, MapContainer> mapContainers = mapServiceContext.getMapContainers();
        final Map<MapContainer, Collection<Record>> recordMap = new HashMap<MapContainer,
                Collection<Record>>(mapContainers.size());
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();
        final int partitionCount = partitionService.getPartitionCount();
        final Address thisAddress = nodeEngine.getClusterService().getThisAddress();

        for (MapContainer mapContainer : mapContainers.values()) {
            for (int i = 0; i < partitionCount; i++) {
                RecordStore recordStore = mapServiceContext.getPartitionContainer(i).getRecordStore(mapContainer.getName());
                // add your owned entries to the map so they will be merged
                if (thisAddress.equals(partitionService.getPartitionOwner(i))) {
                    Collection<Record> records = recordMap.get(mapContainer);
                    if (records == null) {
                        records = new ArrayList<Record>();
                        recordMap.put(mapContainer, records);
                    }
                    final Iterator<Record> iterator = recordStore.iterator(now);
                    while (iterator.hasNext()) {
                        final Record record = iterator.next();
                        records.add(record);
                    }
                }
                // clear all records either owned or backup
                recordStore.reset();
            }
        }
        return new Merger(recordMap);
    }

    private long getNow() {
        return Clock.currentTimeMillis();
    }

    private class Merger implements Runnable {

        Map<MapContainer, Collection<Record>> recordMap;

        public Merger(Map<MapContainer, Collection<Record>> recordMap) {
            this.recordMap = recordMap;
        }

        public void run() {
            for (final MapContainer mapContainer : recordMap.keySet()) {
                Collection<Record> recordList = recordMap.get(mapContainer);
                String mergePolicyName = mapContainer.getMapConfig().getMergePolicy();

                // todo number of records may be high.
                // todo below can be optimized a many records can be send in single invocation
                final MapMergePolicy finalMergePolicy
                        = mapServiceContext.getMergePolicyProvider().getMergePolicy(mergePolicyName);
                for (final Record record : recordList) {
                    // todo too many submission. should submit them in subgroups
                    nodeEngine.getExecutionService().submit("hz:map-merge", new Runnable() {
                        public void run() {
                            final EntryView entryView = EntryViews.createSimpleEntryView(record.getKey(),
                                    mapServiceContext.toData(record.getValue()), record);
                            MergeOperation operation = new MergeOperation(mapContainer.getName(),
                                    record.getKey(), entryView, finalMergePolicy);
                            try {
                                int partitionId = nodeEngine.getPartitionService().getPartitionId(record.getKey());
                                Future f = nodeEngine.getOperationService()
                                        .invokeOnPartition(mapServiceContext.serviceName(), operation, partitionId);
                                f.get();
                            } catch (Throwable t) {
                                throw ExceptionUtil.rethrow(t);
                            }
                        }
                    });
                }
            }
        }

    }
}
