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

package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;

import java.util.Collections;

/**
 * Internal implementation of an map entry with changeable value to prevent
 * to much object allocation while supplying
 *
 * @param <K> key type
 * @param <V> value type
 */
final class SimpleEntry<K, V>
        extends QueryableEntry<K, V> {

    private K key;
    private V value;

    public SimpleEntry() {
        this.extractors = new Extractors(Collections.<MapAttributeConfig>emptyList());
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public Data getKeyData() {
        // not used in map-reduce
        throw new UnsupportedOperationException();
    }

    @Override
    public Data getValueData() {
        // not used in map-reduce
        throw new UnsupportedOperationException();
    }

    @Override
    protected Object getTargetObject(boolean key) {
        return key ? this.key : this.value;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V newValue) {
        V oldValue = this.value;
        this.value = newValue;
        return oldValue;
    }

    void setKey(K key) {
        this.key = key;
    }

}
