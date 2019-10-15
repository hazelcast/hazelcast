/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An implementation of a portable implementing entry set for client results on entrySet requests
 */
public class ReplicatedMapEntries implements Portable {

    private List<Data> keys;
    private List<Data> values;

    public ReplicatedMapEntries() {
    }

    public ReplicatedMapEntries(int initialSize) {
        keys = new ArrayList<>(initialSize);
        values = new ArrayList<>(initialSize);
    }

    public ReplicatedMapEntries(List<Map.Entry<Data, Data>> entries) {
        int initialSize = entries.size();
        keys = new ArrayList<>(initialSize);
        values = new ArrayList<>(initialSize);
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
        ArrayList<Map.Entry<Data, Data>> entries = new ArrayList<>(keys.size());
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

    private void putAllToList(Collection<Map.Entry<Data, Data>> targetList) {
        if (keys == null) {
            return;
        }
        Iterator<Data> keyIterator = keys.iterator();
        Iterator<Data> valueIterator = values.iterator();
        while (keyIterator.hasNext()) {
            targetList.add(new AbstractMap.SimpleImmutableEntry<>(keyIterator.next(), valueIterator.next()));
        }
    }

    private void ensureEntriesCreated() {
        if (keys == null) {
            keys = new ArrayList<>();
            values = new ArrayList<>();
        }
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.MAP_ENTRIES;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        int size = size();
        writer.writeInt("size", size);
        ObjectDataOutput out = writer.getRawDataOutput();
        for (int i = 0; i < size; i++) {
            out.writeData(keys.get(i));
            out.writeData(values.get(i));
        }
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        int size = reader.readInt("size");
        keys = new ArrayList<>(size);
        values = new ArrayList<>(size);
        ObjectDataInput in = reader.getRawDataInput();
        for (int i = 0; i < size; i++) {
            keys.add(in.readData());
            values.add(in.readData());
        }
    }
}
