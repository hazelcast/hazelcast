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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Simple HashMap adapter class to implement DataSerializable serialization semantics
 * to not loose hands on serialization while sending intermediate results.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class HashMapAdapter<K, V>
        extends HashMap<K, V>
        implements IdentifiedDataSerializable {

    public HashMapAdapter(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    public HashMapAdapter(int initialCapacity) {
        super(initialCapacity);
    }

    public HashMapAdapter() {
        super();
    }

    public HashMapAdapter(Map<? extends K, ? extends V> m) {
        super(m);
    }

    @Override
    public int getFactoryId() {
        return MapReduceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapReduceDataSerializerHook.HASH_MAP_ADAPTER;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {

        Set<Map.Entry<K, V>> entrySet = entrySet();
        out.writeInt(entrySet.size());
        for (Map.Entry<K, V> entry : entrySet) {
            out.writeObject(entry.getKey());
            out.writeObject(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {

        int size = in.readInt();
        Map<K, V> map = new HashMap<K, V>(size);
        for (int i = 0; i < size; i++) {
            K key = in.readObject();
            V value = in.readObject();
            map.put(key, value);
        }
        putAll(map);
    }
}
