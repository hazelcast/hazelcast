package com.hazelcast.map;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.map.merge.HigherHitsMapMergePolicy;
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.map.operation.MergeOperation;
import com.hazelcast.map.record.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.util.ExceptionUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

/**
 * Contains map split brain handler functionality.
 */
abstract class MapSplitBrainSupport extends AbstractMapServiceSupport implements SplitBrainHandlerService {

    protected Map<String, MapMergePolicy> mergePolicyMap;

    protected MapSplitBrainSupport(NodeEngine nodeEngine) {
        super(nodeEngine);
        mergePolicyMap = createMergePolicies();
    }

    private Map<String, MapMergePolicy> createMergePolicies() {
        mergePolicyMap = new ConcurrentHashMap<String, MapMergePolicy>();
        mergePolicyMap.put(PutIfAbsentMapMergePolicy.class.getName(), new PutIfAbsentMapMergePolicy());
        mergePolicyMap.put(HigherHitsMapMergePolicy.class.getName(), new HigherHitsMapMergePolicy());
        mergePolicyMap.put(PassThroughMergePolicy.class.getName(), new PassThroughMergePolicy());
        mergePolicyMap.put(LatestUpdateMapMergePolicy.class.getName(), new LatestUpdateMapMergePolicy());
        return mergePolicyMap;
    }

    public MapMergePolicy getMergePolicy(String mergePolicyName) {
        MapMergePolicy mergePolicy = mergePolicyMap.get(mergePolicyName);
        if (mergePolicy == null && mergePolicyName != null) {
            try {

                // check if user has entered custom class name instead of policy name
                mergePolicy = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(), mergePolicyName);
                mergePolicyMap.put(mergePolicyName, mergePolicy);
            } catch (Exception e) {
                logger.severe(e);
                throw ExceptionUtil.rethrow(e);
            }
        }
        if (mergePolicy == null) {
            return mergePolicyMap.get(MapConfig.DEFAULT_MAP_MERGE_POLICY);
        }
        return mergePolicy;
    }

    @Override
    public Runnable prepareMergeRunnable() {
        Map<MapContainer, Collection<Record>> recordMap = new HashMap<MapContainer, Collection<Record>>(mapContainers.size());
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        Address thisAddress = nodeEngine.getClusterService().getThisAddress();

        for (MapContainer mapContainer : mapContainers.values()) {
            for (int i = 0; i < partitionCount; i++) {
                RecordStore recordStore = getPartitionContainer(i).getRecordStore(mapContainer.getName());
                // add your owned entries to the map so they will be merged
                if (thisAddress.equals(partitionService.getPartitionOwner(i))) {
                    Collection<Record> records = recordMap.get(mapContainer);
                    if (records == null) {
                        records = new ArrayList<Record>();
                        recordMap.put(mapContainer, records);
                    }
                    records.addAll(recordStore.getReadonlyRecordMap().values());
                }
                // clear all records either owned or backup
                recordStore.reset();
            }
        }
        return new Merger(recordMap);
    }

    public class Merger implements Runnable {

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
                final MapMergePolicy finalMergePolicy = getMergePolicy(mergePolicyName);
                for (final Record record : recordList) {
                    // todo too many submission. should submit them in subgroups
                    nodeEngine.getExecutionService().submit("hz:map-merge", new Runnable() {
                        public void run() {
                            final EntryView entryView = EntryViews.createSimpleEntryView(record.getKey(),
                                    toData(record.getValue()), record);
                            MergeOperation operation = new MergeOperation(mapContainer.getName(),
                                    record.getKey(), entryView, finalMergePolicy);
                            try {
                                int partitionId = nodeEngine.getPartitionService().getPartitionId(record.getKey());
                                Future f = nodeEngine.getOperationService()
                                        .invokeOnPartition(getServiceName(), operation, partitionId);
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
