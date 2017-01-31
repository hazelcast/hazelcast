/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.util.collection.InflatableSet;
import com.hazelcast.util.collection.Int2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.CollectionUtil.toIntArray;
import static com.hazelcast.util.MapUtil.isNullOrEmpty;
import static com.hazelcast.util.collection.InflatableSet.newBuilder;
import static java.util.Collections.emptySet;

public class PartitionWideEntryWithPredicateOperationFactory extends PartitionAwareOperationFactory {

    private String name;
    private EntryProcessor entryProcessor;
    private Predicate predicate;

    private transient Map<Integer, List<Data>> partitionIdToKeysMap;

    public PartitionWideEntryWithPredicateOperationFactory() {
    }

    public PartitionWideEntryWithPredicateOperationFactory(String name, EntryProcessor entryProcessor,
                                                           Predicate predicate, Map<Integer, List<Data>> partitionIdToKeysMap) {
        this(name, entryProcessor, predicate);
        this.partitionIdToKeysMap = partitionIdToKeysMap;
        this.partitions = isNullOrEmpty(partitionIdToKeysMap) ? null : toIntArray(partitionIdToKeysMap.keySet());
    }

    public PartitionWideEntryWithPredicateOperationFactory(String name, EntryProcessor entryProcessor, Predicate predicate) {
        this.name = name;
        this.entryProcessor = entryProcessor;
        this.predicate = predicate;
    }

    @Override
    public PartitionAwareOperationFactory createFactoryOnRunner(NodeEngine nodeEngine) {
        Set<Data> keys = getKeysFromIndex(nodeEngine);
        Map<Integer, List<Data>> partitionIdToKeysMap
                = getPartitionIdToKeysMap(keys, ((InternalPartitionService) nodeEngine.getPartitionService()));

        return new PartitionWideEntryWithPredicateOperationFactory(name, entryProcessor, predicate, partitionIdToKeysMap);
    }

    @Override
    public Operation createPartitionOperation(int partition) {
        if (isNullOrEmpty(partitionIdToKeysMap)) {
            // fallback here if we cannot find anything from indexes.
            return new PartitionWideEntryWithPredicateOperation(name, entryProcessor, predicate);
        }

        List<Data> keyList = partitionIdToKeysMap.get(partition);
        InflatableSet<Data> keys = newBuilder(keyList).build();
        return new MultipleEntryWithPredicateOperation(name, keys, entryProcessor, predicate);
    }


    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(entryProcessor);
        out.writeObject(predicate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        entryProcessor = in.readObject();
        predicate = in.readObject();
    }

    private Set<Data> getKeysFromIndex(NodeEngine nodeEngine) {
        // Do not use index in this case, because it requires full-table-scan.
        if (predicate == TruePredicate.INSTANCE) {
            return emptySet();
        }

        // get indexes
        MapService mapService = nodeEngine.getService(SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        Indexes indexes = mapServiceContext.getMapContainer(name).getIndexes();
        // optimize predicate
        QueryOptimizer queryOptimizer = mapServiceContext.getQueryOptimizer();
        predicate = queryOptimizer.optimize(predicate, indexes);

        Set<QueryableEntry> querySet = indexes.query(predicate);
        if (querySet == null) {
            return emptySet();
        }

        List<Data> keys = null;
        for (QueryableEntry e : querySet) {
            if (keys == null) {
                keys = new ArrayList<Data>(querySet.size());
            }
            keys.add(e.getKeyData());
        }

        return keys == null ? Collections.<Data>emptySet() : newBuilder(keys).build();
    }

    private Map<Integer, List<Data>> getPartitionIdToKeysMap(Set<Data> keys, InternalPartitionService partitionService) {
        if (isEmpty(keys)) {
            return Collections.emptyMap();
        }

        Map<Integer, List<Data>> partitionToKeys = new Int2ObjectHashMap<List<Data>>();
        for (Data key : keys) {
            int partitionId = partitionService.getPartitionId(key);
            List<Data> keyList = partitionToKeys.get(partitionId);
            if (keyList == null) {
                keyList = new ArrayList<Data>();
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
    public int getId() {
        return MapDataSerializerHook.PARTITION_WIDE_PREDICATE_ENTRY_FACTORY;
    }
}
