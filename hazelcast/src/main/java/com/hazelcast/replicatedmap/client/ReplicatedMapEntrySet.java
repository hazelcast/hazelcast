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

package com.hazelcast.replicatedmap.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of a portable implementing entry set for client results on entrySet requests
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ReplicatedMapEntrySet<K, V>
        implements Portable {

    private final Set<Map.Entry<K, V>> entrySet;

    ReplicatedMapEntrySet() {
        entrySet = new HashSet<Map.Entry<K, V>>();
    }

    public ReplicatedMapEntrySet(Set<Map.Entry<K, V>> entrySet) {
        this.entrySet = entrySet;
    }

    public Set<Map.Entry<K, V>> getEntrySet() {
        return entrySet;
    }

    @Override
    public void writePortable(PortableWriter writer)
            throws IOException {
        writer.writeInt("size", entrySet.size());
        ObjectDataOutput out = writer.getRawDataOutput();
        for (Map.Entry<K, V> entry : entrySet) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public void readPortable(PortableReader reader)
            throws IOException {
        int size = reader.readInt("size");
        ObjectDataInput in = reader.getRawDataInput();
        for (int i = 0; i < size; i++) {
            K key = (K) in.readObject();
            V value = (V) in.readObject();
            entrySet.add(new AbstractMap.SimpleImmutableEntry(key, value));
        }
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.MAP_ENTRY_SET;
    }

}
