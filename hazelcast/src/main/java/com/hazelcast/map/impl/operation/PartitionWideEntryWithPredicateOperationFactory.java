/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryProcessor;
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

public class PartitionWideEntryWithPredicateOperationFactory implements PartitionAwareOperationFactory {

    private String name;
    private EntryProcessor entryProcessor;
    private Predicate predicate;

    private transient Set<Data> keySet;
    private transient NodeEngine nodeEngine;
    private transient boolean hasIndex;
    private transient Map<Integer, List<Data>> partitionIdToKeys;
    private transient InternalSerializationService serializationService;

    public PartitionWideEntryWithPredicateOperationFactory() {
    }

    public PartitionWideEntryWithPredicateOperationFactory(String name, EntryProcessor entryProcessor,
                                                           Predicate predicate) {
        this.name = name;
        this.entryProcessor = entryProcessor;
        this.predicate = predicate;
    }

    @Override
    public void init(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        queryIndex();
    }

    private void queryIndex() {
        // Do not use index in this case, because it requires full-table-scan.
        if (predicate == TruePredicate.INSTANCE) {
            return;
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
            return;
        }

        List<Data> keys = null;
        for (QueryableEntry e : querySet) {
            if (keys == null) {
                keys = new ArrayList<Data>(querySet.size());
            }
            keys.add(e.getKeyData());
        }

        hasIndex = true;
        keySet = keys == null ? Collections.<Data>emptySet() : InflatableSet.newBuilder(keys).build();
    }

    @Override
    public Operation createPartitionOperation(int partition) {
        if (hasIndex) {
            List<Data> keys = partitionIdToKeys.get(partition);
            InflatableSet<Data> keySet = InflatableSet.newBuilder(keys).build();
            return new MultipleEntryWithPredicateOperation(name, keySet, entryProcessor, serializationService.copy(predicate));
        }

        return createOperation();
    }

    @Override
    public int[] getPartitions() {
        if (!hasIndex) {
            return null;
        }

        partitionIdToKeys = getPartitionIdToKeysMap();
        return toIntArray(partitionIdToKeys.keySet());
    }

    private Map<Integer, List<Data>> getPartitionIdToKeysMap() {
        if (isEmpty(keySet)) {
            return Collections.emptyMap();
        }

        Map<Integer, List<Data>> partitionToKeys = new Int2ObjectHashMap<List<Data>>();
        for (Data key : keySet) {
            int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
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
        return new PartitionWideEntryWithPredicateOperation(name, entryProcessor, serializationService.copy(predicate));
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
}
