/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.collection.CollectionItem;
import com.hazelcast.collection.list.ListContainer;
import com.hazelcast.collection.list.ListService;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ListKeyValueSource<V>
        extends KeyValueSource<String, V>
        implements IdentifiedDataSerializable {

    private final static long serialVersionUID = 1;

    // This prevents excessive creation of map entries for a serialized operation
    private final MapReduceSimpleEntry<String, V> simpleEntry = new MapReduceSimpleEntry<String, V>();

    private String listName;

    private transient SerializationService ss;
    private transient Iterator<CollectionItem> iterator;
    private transient CollectionItem nextElement;

    public ListKeyValueSource() {
    }

    public ListKeyValueSource(String listName) {
        this.listName = listName;
    }

    @Override
    public boolean open(NodeEngine nodeEngine) {
        NodeEngineImpl nei = (NodeEngineImpl) nodeEngine;
        ss = nei.getSerializationService();

        Address thisAddress = nei.getThisAddress();
        PartitionService ps = nei.getPartitionService();
        Data data = ss.toData(listName, StringAndPartitionAwarePartitioningStrategy.INSTANCE);
        int partitionId = ps.getPartitionId(data);
        Address partitionOwner = ps.getPartitionOwner(partitionId);
        if (partitionOwner == null) {
            return false;
        }
        if (thisAddress.equals(partitionOwner)) {
            ListService listService = nei.getService(ListService.SERVICE_NAME);
            ListContainer listContainer = listService.getOrCreateContainer(listName, false);
            List<CollectionItem> items = new ArrayList<CollectionItem>(listContainer.getCollection());
            iterator = items.iterator();
        }
        return true;
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = iterator == null ? false : iterator.hasNext();
        nextElement = hasNext ? iterator.next() : null;
        return hasNext;
    }

    @Override
    public String key() {
        return listName;
    }

    @Override
    public Map.Entry<String, V> element() {
        Object value = nextElement.getValue();
        if (value instanceof Data) {
            value = ss.toObject((Data) value);
        }
        simpleEntry.setKey(listName);
        simpleEntry.setValue((V) value);
        return simpleEntry;
    }

    @Override
    public boolean reset() {
        iterator = null;
        nextElement = null;
        return true;
    }

    @Override
    public void close() throws IOException {
        iterator = null;
        nextElement = null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(listName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        listName = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.KEY_VALUE_SOURCE_LIST;
    }

}
