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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.util.collection.InflatableSet;
import com.hazelcast.internal.util.collection.InflatableSet.Builder;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.map.impl.query.QueryResultRow;
import com.hazelcast.map.impl.query.QueryRunner;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.collection.InflatableSet.newBuilder;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.internal.util.MapUtil.createInt2ObjectHashMap;

public class PartitionWideEntryWithPredicateOperationFactory extends PartitionAwareOperationFactory {

    private String name;
    private EntryProcessor entryProcessor;
    private Predicate predicate;

    /**
     * Entry keys grouped by partition ID. This map is constructed from data
     * fetched by querying the map from non-partition threads. Because of
     * concurrent migrations, the query running on non-partition threads might
     * fail. In this case, the map is {@code null}.
     */
    private transient Map<Integer, List<Data>> partitionIdToKeysMap;

    public PartitionWideEntryWithPredicateOperationFactory() {
    }

    public PartitionWideEntryWithPredicateOperationFactory(String name, EntryProcessor entryProcessor, Predicate predicate) {
        this.name = name;
        this.entryProcessor = entryProcessor;
        this.predicate = predicate;
    }

    private PartitionWideEntryWithPredicateOperationFactory(String name, EntryProcessor entryProcessor, Predicate predicate,
                                                            Map<Integer, List<Data>> partitionIdToKeysMap) {
        this(name, entryProcessor, predicate);
        this.partitionIdToKeysMap = partitionIdToKeysMap;
    }

    @Override
    public PartitionAwareOperationFactory createFactoryOnRunner(NodeEngine nodeEngine, int[] partitions) {
        Set<Data> keys = tryToObtainKeysFromIndexes(nodeEngine);
        Map<Integer, List<Data>> partitionIdToKeysMap = groupKeysByPartition(keys, nodeEngine.getPartitionService(), partitions);

        return new PartitionWideEntryWithPredicateOperationFactory(name, entryProcessor, predicate, partitionIdToKeysMap);
    }

    @Override
    public Operation createPartitionOperation(int partition) {
        if (partitionIdToKeysMap == null) {
            // Index query failed to run because of ongoing migrations or we are
            // creating an operation on the caller node.
            return new PartitionWideEntryWithPredicateOperation(name, entryProcessor, predicate);
        }

        // index query succeeded
        List<Data> keyList = partitionIdToKeysMap.get(partition);
        assert keyList != null : "unexpected partition " + partition + ", expected partitions " + partitionIdToKeysMap.keySet();
        Set<Data> keys = keyList.isEmpty() ? Collections.emptySet() : newBuilder(keyList).build();
        return new MultipleEntryWithPredicateOperation(name, keys, entryProcessor, predicate);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(name);
        out.writeObject(entryProcessor);
        out.writeObject(predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readString();
        entryProcessor = in.readObject();
        predicate = in.readObject();
    }

    /**
     * Attempts to get keys by running an index query. This method may return
     * {@code null} if there is an ongoing migration, which means that it is not
     * safe to return results from a non-partition thread. The caller must then
     * run a partition query to obtain the results.
     *
     * @param nodeEngine nodeEngine of this cluster node
     * @return the set of keys or {@code null} if we failed to fetch the keys
     * because of ongoing migrations
     */
    private Set<Data> tryToObtainKeysFromIndexes(NodeEngine nodeEngine) {
        // Do not use index in this case, because it requires full-table-scan.
        if (predicate == Predicates.alwaysTrue()) {
            return null;
        }

        MapService mapService = nodeEngine.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapContainer mapContainer = mapServiceContext.getMapContainer(name);
        if (!mapContainer.shouldUseGlobalIndex()) {
            return null;
        }

        QueryRunner runner = mapServiceContext.getMapQueryRunner(name);
        Query query = Query.of().mapName(name).predicate(predicate).iterationType(IterationType.KEY).build();
        final QueryResult result = (QueryResult) runner.runIndexQueryOnOwnedPartitions(query);
        if (result.getPartitionIds() == null) {
            // failed to run query because of ongoing migrations
            return null;
        }

        final Builder<Data> setBuilder = InflatableSet.newBuilder(result.size());
        for (QueryResultRow row : result.getRows()) {
            setBuilder.add(row.getKey());
        }
        return setBuilder.build();
    }

    private Map<Integer, List<Data>> groupKeysByPartition(Set<Data> keys, IPartitionService partitionService, int[] partitions) {
        if (keys == null) {
            return null;
        }

        // Even if the keys are successfully fetched from indexes, we need to
        // filter them to exclude the keys belonging to partitions on which we
        // weren't asked to operate on. Moreover, we need to include the
        // partitions on which we were asked to operate on and for which we
        // don't have any keys since this may indicate an out-migrated partition
        // and we want OperationRunner to throw WrongTargetException to notify
        // the caller about such situations.

        // Using the type of Int2ObjectHashMap allows the get and put operations
        // to avoid auto-boxing.
        final Int2ObjectHashMap<List<Data>> partitionToKeys = createInt2ObjectHashMap(partitions.length);

        // Pre-populate the map with the requested partitions to use it as a set
        // to filter out possible unrequested partitions encountered among the
        // fetched keys.
        for (int partition : partitions) {
            partitionToKeys.put(partition, Collections.emptyList());
        }

        for (Data key : keys) {
            int partitionId = partitionService.getPartitionId(key);
            List<Data> keyList = partitionToKeys.get(partitionId);
            if (keyList == null) {
                // we weren't asked to run on this partition
                continue;
            }

            if (keyList.isEmpty()) {
                // we have a first key for this partition
                keyList = new ArrayList<>();
                partitionToKeys.put(partitionId, keyList);
            }
            keyList.add(key);
        }
        return partitionToKeys;
    }

    @Override
    public Operation createOperation() {
        return new PartitionWideEntryWithPredicateOperation(name, entryProcessor, predicate);
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.PARTITION_WIDE_PREDICATE_ENTRY_FACTORY;
    }
}
