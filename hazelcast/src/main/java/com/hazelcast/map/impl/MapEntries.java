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

    private final class Entry {
        final Data key;
        final Object value;
        private final SerializationService serializationService;

        /**
         * @param key
         * @param value
         */
        public Entry(Data key, Data value) {
            this(key, value, null);
        }

        /**
         * @param key
         * @param value
         * @param serializationService
         */
        public Entry(Data key, Object value, SerializationService serializationService) {
            super();
            this.key = key;
            this.value = value;
            this.serializationService = serializationService;
        }

        public Data getValueAsData() {
            return serializationService != null ? serializationService.toData(value) : (Data) value;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return "Entry [key=" + this.key + ", value=" + this.value + ", serializationService="
                    + this.serializationService + "]";
        }
    }

    private List<Entry> entries;

    public MapEntries() {
    }

    public MapEntries(int initialSize) {
        entries = new ArrayList<Entry>(initialSize);
    }

    public MapEntries(List<Map.Entry<Data, Data>> entries) {
        int initialSize = entries.size();
        this.entries = new ArrayList<MapEntries.Entry>(initialSize);
        for (Map.Entry<Data, Data> entry : entries) {
            this.entries.add(new Entry(entry.getKey(), entry.getValue()));
        }
    }

    public void add(Data key, Data value) {
        ensureEntriesCreated();
        this.entries.add(new Entry(key, value));
    }

    public void add(Data key, Object value, SerializationService serializationService) {
        ensureEntriesCreated();
        this.entries.add(new Entry(key, value, serializationService));
    }

    public List<Map.Entry<Data, Data>> entries() {
        final List<Map.Entry<Data, Data>> dataEntries = new ArrayList<Map.Entry<Data, Data>>(entries.size());
        putAllToList(dataEntries);
        return dataEntries;
    }

    public Data getKey(int index) {
        return entries.get(index).key;
    }

    public Data getValue(int index) {
        return entries.get(index).getValueAsData();
    }

    public Object getObjectValue(int index) {
        return entries.get(index).value;
    }

    public int size() {
        return (entries == null ? 0 : entries.size());
    }

    public boolean isEmpty() {
        return (entries == null || entries.size() == 0);
    }

    public void clear() {
        if (entries != null) {
            entries.clear();
        }
    }

    public void putAllToList(Collection<Map.Entry<Data, Data>> targetList) {
        if (entries == null) {
            return;
        }
        
        for (Entry entry : entries) {
            targetList.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(entry.key, entry.getValueAsData()));
        }
    }

    public <K, V> void putAllToMapDeserialized(SerializationService serializationService, Map<K, V> map) {
        if (entries == null) {
            return;
        }
        
        for (Entry entry : entries) {
            final K key = serializationService.toObject(entry.key);
            final V value = serializationService.toObject(entry.value);
            map.put(key, value);
        }
    }

    public void putAllToMap(Map<Data, Object> map) {
        if (entries == null) {
            return;
        }
        
        for (Entry entry : entries) {
            map.put(entry.key, entry.value);
        }
    }

    private void ensureEntriesCreated() {
        if (entries == null) {
            entries = new ArrayList<MapEntries.Entry>();
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
            final Entry entry = entries.get(i);
            out.writeData(entry.key);
            out.writeData(entry.getValueAsData());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        entries = new ArrayList<Entry>(size);
        for (int i = 0; i < size; i++) {
            entries.add(new Entry(in.readData(), in.readData()));
        }
    }
}
