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
import java.util.List;
import java.util.Map;

/**
 * Legacy implementation of {@link MapEntries} for the old client {@link com.hazelcast.core.IMap#putAll(Map)} implementation,
 * which avoids copying of the whole data set, when the data is already a {@link Collection}.
 *
 * Will be de-serialized as {@link MapEntriesImpl}.
 */
public final class MapEntriesLegacyImpl implements MapEntries {

    private List<Map.Entry<Data, Data>> entries;

    public MapEntriesLegacyImpl(Collection<Map.Entry<Data, Data>> entries) {
        this.entries = new ArrayList<Map.Entry<Data, Data>>(entries);
    }

    @Override
    public void add(Data key, Data value) {
        ensureEntriesCreated();
        entries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, value));
    }

    @Override
    public List<Map.Entry<Data, Data>> entries() {
        return entries;
    }

    @Override
    public Data getKey(int index) {
        return entries.get(index).getKey();
    }

    @Override
    public Data getValue(int index) {
        return entries.get(index).getValue();
    }

    @Override
    public int size() {
        return (entries == null ? 0 : entries.size());
    }

    @Override
    public boolean isEmpty() {
        return (entries == null || entries.isEmpty());
    }

    @Override
    public void clear() {
        if (entries != null) {
            entries.clear();
        }
    }

    @Override
    public void putAllToList(Collection<Map.Entry<Data, Data>> targetList) {
        if (entries == null) {
            return;
        }
        targetList.addAll(entries);
    }

    @Override
    public <K, V> void putAllToMap(SerializationService serializationService, Map<K, V> map) {
        if (entries == null) {
            return;
        }
        for (Map.Entry<Data, Data> entry : entries) {
            K key = serializationService.toObject(entry.getKey());
            V value = serializationService.toObject(entry.getValue());
            map.put(key, value);
        }
    }

    private void ensureEntriesCreated() {
        if (entries == null) {
            entries = new ArrayList<Map.Entry<Data, Data>>();
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
        if (size > 0) {
            for (Map.Entry<Data, Data> entry : entries) {
                out.writeData(entry.getKey());
                out.writeData(entry.getValue());
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        throw new UnsupportedOperationException("This class should be de-serialized as MapEntriesImpl");
    }
}
