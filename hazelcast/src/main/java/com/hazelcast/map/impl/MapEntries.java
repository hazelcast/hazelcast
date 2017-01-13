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

package com.hazelcast.map.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * MapEntries is a collection of {@link Data} instances for keys and values of a {@link java.util.Map.Entry}.
 */
public final class MapEntries implements IdentifiedDataSerializable {

    /**
     * Contains key/value in pairs
     */
    private List<Object> entries;
    private SerializationService serializationService;

    public MapEntries() {
    }

    public MapEntries(int initialSize) {
        entries = new ArrayList<Object>(initialSize * 2);
    }

    public MapEntries(List<Map.Entry<Data, Data>> entries) {
        int initialSize = entries.size();
        this.entries = new ArrayList<Object>(initialSize * 2);
        for (Map.Entry<Data, Data> entry : entries) {
            this.entries.add(entry.getKey());
            this.entries.add(entry.getValue());
        }
    }

    public void add(Data key, Data value) {
        ensureEntriesCreated();
        this.entries.add(key);
        this.entries.add(value);
    }

    public void add(Data key, Object value, SerializationService serializationService) {
        ensureEntriesCreated();
        this.entries.add(key);
        this.entries.add(value);
        this.serializationService = serializationService;
    }

    public List<Map.Entry<Data, Data>> entries() {
        final List<Map.Entry<Data, Data>> dataEntries = new ArrayList<Map.Entry<Data, Data>>(size());
        putAllToList(dataEntries);
        return dataEntries;
    }

    public Data getKey(int index) {
        return (Data) entries.get(index * 2);
    }

    public Data getValue(int index) {
        final Object oValue = entries.get((2 * index) + 1);
        return serializationService == null ? (Data) oValue : serializationService.toData(oValue);
    }

    public Object getObjectValue(int index) {
        return entries.get((2 * index) + 1);
    }

    public int size() {
        return (entries == null ? 0 : entries.size() / 2);
    }

    public boolean isEmpty() {
        return (entries == null || entries.isEmpty());
    }

    public void clear() {
        if (entries != null) {
            entries.clear();
        }
    }

    public void putAllToList(Collection<Map.Entry<Data, Data>> targetList) {
        for (int i=0, j=size(); i<j; ++i) {
            targetList.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(getKey(i), getValue(i)));            
        }
    }

    public <K, V> void putAllToMapDeserialized(SerializationService serializationService, Map<K, V> map) {
        if (entries == null) {
            return;
        }
        
        for (int i=0,j=entries.size(); i<j; ++i) {
            final K key = serializationService.toObject(entries.get(i));
            ++i;
            final V value = serializationService.toObject(entries.get(i));
            map.put(key, value);
        }
    }

    /**
     * The {@link List} returned has the keys and values stored 1 after the other (key1, value1, key2, value2, etc.). The keys are 
     * guaranteed to be {@link Data} instances. However, the value might be of any type.
     */
    public void putAllToListDataKeysObjectValue(List<Object> values) {
        if (entries == null) {
            return;
        }
        
        values.addAll(entries);
    }

    private void ensureEntriesCreated() {
        if (entries == null) {
            entries = new ArrayList<Object>();
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.ENTRIES;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        int size = size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            out.writeData(getKey(i));
            out.writeData(getValue(i));
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        entries = new ArrayList<Object>(size * 2);
        for (int i = 0; i < size; i++) {
            entries.add(in.readData());
            entries.add(in.readData());
        }
    }
}
