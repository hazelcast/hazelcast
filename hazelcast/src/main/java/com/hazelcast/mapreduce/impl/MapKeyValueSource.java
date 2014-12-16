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

import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.RecordStore;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.PartitionIdAware;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * This {@link com.hazelcast.mapreduce.KeyValueSource} implementation is used in
 * {@link com.hazelcast.mapreduce.KeyValueSource#fromMap(com.hazelcast.core.IMap)} to generate a default
 * implementation based on a Hazelcast {@link com.hazelcast.core.IMap}.
 *
 * @param <K> type of the key of the IMap
 * @param <V> type of the value of the IMap
 */
public class MapKeyValueSource<K, V>
        extends KeyValueSource<K, V>
        implements IdentifiedDataSerializable, PartitionIdAware {

    // This prevents excessive creation of map entries for a serialized operation
    private final MapReduceSimpleEntry<K, V> simpleEntry = new MapReduceSimpleEntry<K, V>();

    private String mapName;

    private transient int partitionId;
    private transient SerializationService ss;
    private transient Iterator<Map.Entry<Data, Data>> iterator;
    private transient Map.Entry<Data, Data> nextElement;

    MapKeyValueSource() {
    }

    public MapKeyValueSource(String mapName) {
        this.mapName = mapName;
    }

    @Override
    public boolean open(NodeEngine nodeEngine) {
        NodeEngineImpl nei = (NodeEngineImpl) nodeEngine;
        InternalPartitionService ps = nei.getPartitionService();
        MapService mapService = nei.getService(MapService.SERVICE_NAME);
        ss = nei.getSerializationService();
        Address partitionOwner = ps.getPartitionOwner(partitionId);
        if (partitionOwner == null) {
            return false;
        }
        RecordStore recordStore = mapService.getMapServiceContext().getRecordStore(partitionId, mapName);
        iterator = recordStore.entrySetData().iterator();
        return true;
    }

    @Override
    public void close()
            throws IOException {
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = iterator.hasNext();
        nextElement = hasNext ? iterator.next() : null;
        return hasNext;
    }

    @Override
    public K key() {
        if (nextElement == null) {
            throw new IllegalStateException("no more elements");
        }
        Data keyData = nextElement.getKey();
        K key = (K) ss.toObject(keyData);
        simpleEntry.setKeyData(keyData);
        simpleEntry.setKey(key);
        return key;
    }

    @Override
    public Map.Entry<K, V> element() {
        if (nextElement == null) {
            throw new IllegalStateException("no more elements");
        }
        if (!nextElement.getKey().equals(simpleEntry.getKeyData())) {
            simpleEntry.setKey((K) ss.toObject(nextElement.getKey()));
        }
        simpleEntry.setValue((V) ss.toObject(nextElement.getValue()));
        return simpleEntry;
    }

    @Override
    public boolean reset() {
        iterator = null;
        nextElement = null;
        return true;
    }

    @Override
    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(mapName);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        mapName = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.KEY_VALUE_SOURCE_MAP;
    }

}
