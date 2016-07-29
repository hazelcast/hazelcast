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
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link MapEntries} which reduces litter by not wrapping each key/value pair in a {@link java.util.Map.Entry}.
 */
public class MapEntriesImpl implements MapEntries {

    private List<Data> keys;
    private List<Data> values;

    public MapEntriesImpl() {
    }

    public MapEntriesImpl(int initialSize) {
        keys = new ArrayList<Data>(initialSize);
        values = new ArrayList<Data>(initialSize);
    }

    @Override
    public void add(Data key, Data value) {
        ensureEntriesCreated();
        keys.add(key);
        values.add(value);
    }

    @Override
    public List<Map.Entry<Data, Data>> entries() {
        ArrayList<Map.Entry<Data, Data>> entries = new ArrayList<Map.Entry<Data, Data>>(keys.size());
        putAllToList(entries);
        return entries;
    }

    @Override
    public Data getKey(int index) {
        return keys.get(index);
    }

    @Override
    public Data getValue(int index) {
        return values.get(index);
    }

    @Override
    public int size() {
        return (keys == null ? 0 : keys.size());
    }

    @Override
    public boolean isEmpty() {
        return (keys == null || keys.isEmpty());
    }

    @Override
    public void clear() {
        if (keys != null) {
            keys.clear();
            values.clear();
        }
    }

    @Override
    public void putAllToList(Collection<Map.Entry<Data, Data>> targetList) {
        if (keys == null) {
            return;
        }
        Iterator<Data> keyIterator = keys.iterator();
        Iterator<Data> valueIterator = values.iterator();
        while (keyIterator.hasNext()) {
            targetList.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(keyIterator.next(), valueIterator.next()));
        }
    }

    @Override
    public <K, V> void putAllToMap(SerializationService serializationService, Map<K, V> map) {
        if (keys == null) {
            return;
        }
        Iterator<Data> keyIterator = keys.iterator();
        Iterator<Data> valueIterator = values.iterator();
        while (keyIterator.hasNext()) {
            K key = serializationService.toObject(keyIterator.next());
            V value = serializationService.toObject(valueIterator.next());
            map.put(key, value);
        }
    }

    private void ensureEntriesCreated() {
        if (keys == null) {
            keys = new ArrayList<Data>();
            values = new ArrayList<Data>();
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.MAP_ENTRIES;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        int size = size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            out.writeData(keys.get(i));
            out.writeData(values.get(i));
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        keys = new ArrayList<Data>(size);
        values = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            keys.add(in.readData());
            values.add(in.readData());
        }
    }
}
