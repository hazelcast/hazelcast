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

import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.PartitionIdAware;
import com.hazelcast.multimap.impl.MultiMapContainer;
import com.hazelcast.multimap.impl.MultiMapRecord;
import com.hazelcast.multimap.impl.MultiMapService;
import com.hazelcast.multimap.impl.MultiMapWrapper;
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
 * {@link com.hazelcast.mapreduce.KeyValueSource#fromMultiMap(com.hazelcast.core.MultiMap)} to generate a default
 * implementation based on a Hazelcast {@link com.hazelcast.core.MultiMap}.
 *
 * @param <K> type of the key of the MultiMap
 * @param <V> type of the value of the MultiMap
 */
public class MultiMapKeyValueSource<K, V>
        extends KeyValueSource<K, V>
        implements IdentifiedDataSerializable, PartitionIdAware {

    // This prevents excessive creation of map entries for a serialized operation
    private final MapReduceSimpleEntry<K, V> simpleEntry = new MapReduceSimpleEntry<K, V>();

    private String multiMapName;

    private transient int partitionId;
    private transient SerializationService ss;
    private transient MultiMapContainer multiMapContainer;
    private transient boolean isBinary;

    private transient K key;
    private transient Iterator<Data> keyIterator;
    private transient Iterator<MultiMapRecord> valueIterator;
    private transient MultiMapRecord multiMapRecord;

    MultiMapKeyValueSource() {
    }

    public MultiMapKeyValueSource(String multiMapName) {
        this.multiMapName = multiMapName;
    }

    @Override
    public boolean open(NodeEngine nodeEngine) {
        NodeEngineImpl nei = (NodeEngineImpl) nodeEngine;
        InternalPartitionService ps = nei.getPartitionService();
        MultiMapService multiMapService = nei.getService(MultiMapService.SERVICE_NAME);
        ss = nei.getSerializationService();
        Address partitionOwner = ps.getPartitionOwner(partitionId);
        if (partitionOwner == null) {
            return false;
        }
        multiMapContainer = multiMapService.getOrCreateCollectionContainer(partitionId, multiMapName);
        isBinary = multiMapContainer.getConfig().isBinary();
        keyIterator = multiMapContainer.keySet().iterator();
        return true;
    }

    @Override
    public void close()
            throws IOException {
    }

    @Override
    public boolean hasNext() {
        if (valueIterator != null) {
            boolean hasNext = valueIterator.hasNext();
            multiMapRecord = hasNext ? valueIterator.next() : null;
            if (hasNext) {
                return true;
            }
        }

        if (keyIterator != null && keyIterator.hasNext()) {
            Data dataKey = keyIterator.next();
            key = (K) ss.toObject(dataKey);
            MultiMapWrapper wrapper = multiMapContainer.getMultiMapWrapper(dataKey);
            valueIterator = wrapper.getCollection(true).iterator();
            return hasNext();
        }

        return false;
    }

    @Override
    public K key() {
        if (multiMapRecord == null) {
            throw new IllegalStateException("no more elements");
        }
        return key;
    }

    @Override
    public Map.Entry<K, V> element() {
        if (multiMapRecord == null) {
            throw new IllegalStateException("no more elements");
        }
        simpleEntry.setKey(key);
        Object value = multiMapRecord.getObject();
        simpleEntry.setValue((V) (isBinary ? ss.toObject((Data) value) : value));
        return simpleEntry;
    }

    @Override
    public boolean reset() {
        key = null;
        keyIterator = null;
        valueIterator = null;
        multiMapRecord = null;
        return false;
    }

    @Override
    public void setPartitionId(int partitionId) {
        this.partitionId = partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeUTF(multiMapName);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        multiMapName = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.KEY_VALUE_SOURCE_MULTIMAP;
    }

}
