/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readMap;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeMap;

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

    public HashMapAdapter() {
    }

    public HashMapAdapter(int initialCapacity, float loadFactor) {
        super(initialCapacity, loadFactor);
    }

    @Override
    public int getFactoryId() {
        return ScheduledExecutorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.HASH_MAP_ADAPTER;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {

        writeMap(this, out);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {

        Map<K, V> map = readMap(in);
        putAll(map);
    }
}
