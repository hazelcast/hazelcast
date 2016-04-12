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

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class PartitionWideEntryWithPredicateOperationFactory implements OperationFactory {
    private String name;
    private EntryProcessor entryProcessor;
    private Predicate predicate;
    private boolean hasIndex;
    private Set<Data> keySet;

    public PartitionWideEntryWithPredicateOperationFactory() {
    }

    public PartitionWideEntryWithPredicateOperationFactory(String name, EntryProcessor entryProcessor, Predicate predicate) {
        this.name = name;
        this.entryProcessor = entryProcessor;
        this.predicate = predicate;
    }

    @Override
    public Operation createOperation() {
        if (hasIndex) {
            return new MultipleEntryOperation(name, keySet, entryProcessor);
        }
        return new PartitionWideEntryWithPredicateOperation(name, entryProcessor, predicate);
    }

    public void queryIndex(MapService mapService) {
        Indexes indexService = mapService.getMapServiceContext().getMapContainer(name).getIndexes();
        Set<QueryableEntry> querySet = indexService.query(predicate);
        if (querySet != null) {
            Set<Data> keys = new HashSet<Data>();
            for (QueryableEntry e: querySet) {
                keys.add(e.getKeyData());
            }
            hasIndex = true;
            keySet = keys;
        }
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
