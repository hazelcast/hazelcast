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

package com.hazelcast.jet.io.tuple;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class Tuple2<K, V> implements Tuple<K, V> {
    protected K key;
    protected V value;

    public Tuple2() {

    }

    public Tuple2(K k, V v) {
        this.key = k;
        this.value = v;
    }

    @Override
    public K getKey(int index) {
        return key;
    }

    @Override
    public V getValue(int index) {
        return value;
    }

    @Override
    public int keySize() {
        return 1;
    }

    @Override
    public int valueSize() {
        return 1;
    }

    @Override
    public void setKey(int index, K key) {
        this.key = key;
    }

    @Override
    public void setValue(int index, V value) {
        this.value = value;
    }

    @Override
    public K[] cloneKeys() {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public V[] cloneValues() {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readObject();
        value = in.readObject();
    }
}
