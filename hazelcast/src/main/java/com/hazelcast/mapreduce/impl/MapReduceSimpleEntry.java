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

import com.hazelcast.nio.serialization.Data;

import java.util.Map;

/**
 * This class is used to prevent extensive GC pressure while mapping values
 *
 * @param <K> type of key
 * @param <V> type of value
 */
class MapReduceSimpleEntry<K, V>
        implements Map.Entry<K, V> {

    private Data keyData;
    private K key;
    private V value;

    public MapReduceSimpleEntry() {
        this(null, null);
    }

    public MapReduceSimpleEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public Data getKeyData() {
        return keyData;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        V oldValue = this.value;
        this.value = value;
        return oldValue;
    }

    public void setKeyData(Data keyData) {
        this.keyData = keyData;
    }

    public K setKey(K key) {
        K oldKey = this.key;
        this.key = key;
        return oldKey;
    }

}
