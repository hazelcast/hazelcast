/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * MapEntries is a collection of {@link Data} instances for keys and values of a {@link java.util.Map.Entry}.
 */
public final class MapEntries implements IdentifiedDataSerializable {

    private List<Data> keys;
    private List<Data> values;

    public MapEntries() {
    }

    public MapEntries(int initialSize) {
        keys = new ArrayList<Data>(initialSize);
        values = new ArrayList<Data>(initialSize);
    }

    public MapEntries(List<Map.Entry<Data, Data>> entries) {
        int initialSize = entries.size();
        keys = new ArrayList<Data>(initialSize);
        values = new ArrayList<Data>(initialSize);
        for (Map.Entry<Data, Data> entry : entries) {
            keys.add(entry.getKey());
            values.add(entry.getValue());
        }
    }

    public void add(Data key, Data value) {
        ensureEntriesCreated();
        keys.add(key);
        values.add(value);
    }

    public List<Map.Entry<Data, Data>> entries() {
        ArrayList<Map.Entry<Data, Data>> entries = new ArrayList<Map.Entry<Data, Data>>(keys.size());
        putAllToList(entries);
        return entries;
    }

    public Data getKey(int index) {
        return keys.get(index);
    }

    public Data getValue(int index) {
        return values.get(index);
    }

    public int size() {
        return (keys == null ? 0 : keys.size());
    }

    public boolean isEmpty() {
        return (keys == null || keys.size() == 0);
    }

    public void clear() {
        if (keys != null) {
            keys.clear();
            values.clear();
        }
    }

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
        return MapDataSerializerHook.ENTRIES;
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
