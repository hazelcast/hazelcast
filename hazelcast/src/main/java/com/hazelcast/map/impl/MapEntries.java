/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * MapEntries is a collection of {@link java.util.Map.Entry} instances.
 */
public final class MapEntries implements IdentifiedDataSerializable, Iterable<Map.Entry<Data, Data>> {

    private List<Map.Entry<Data, Data>> entries;

    public MapEntries() {
    }

    public MapEntries(Collection<Map.Entry<Data, Data>> entries) {
        this.entries = new ArrayList<Map.Entry<Data, Data>>(entries);
    }

    public Collection<Map.Entry<Data, Data>> entries() {
        ensureEntriesCreated();
        return entries;
    }

    public void add(Map.Entry<Data, Data> entry) {
        ensureEntriesCreated();
        entries.add(entry);
    }

    public void add(Data key, Data value) {
        ensureEntriesCreated();
        entries.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, value));
    }

    public int size() {
        return entries == null ? 0 : entries.size();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Iterator<Map.Entry<Data, Data>> iterator() {
        ensureEntriesCreated();
        return entries.iterator();
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
        int size = entries == null ? 0 : entries.size();
        out.writeInt(size);

        if (size > 0) {
            for (Map.Entry<Data, Data> o : entries) {
                out.writeData(o.getKey());
                out.writeData(o.getValue());
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();

        entries = new ArrayList<Map.Entry<Data, Data>>(size);

        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            Data value = in.readData();
            Map.Entry<Data, Data> entry = new AbstractMap.SimpleImmutableEntry<Data, Data>(key, value);
            entries.add(entry);
        }
    }
}
