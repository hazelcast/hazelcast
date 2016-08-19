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

package com.hazelcast.jet.io;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class Pair<K, V> {
    protected K key;
    protected V value;

    public Pair() {
    }

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public <T> T get(int index) {
        final Object result = index == 0 ? key
                : index == 1 ? value
                : error("Attempt to access component at index " + index);
        return (T) result;
    }

    public void set(int index, Object value) {
        if (index == 0) {
            this.key = (K) value;
        } else if (index == 1) {
            this.value = (V) value;
        } else {
            error("Attempt to set a component at index " + index);
        }
    }

    public int size() {
        return 2;
    }

    public Object[] toArray() {
        throw new IllegalStateException("Not supported");
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(key);
        out.writeObject(value);
    }

    public void readData(ObjectDataInput in) throws IOException {
        key = in.readObject();
        value = in.readObject();
    }

    private static Object error(String msg) {
        throw new IllegalArgumentException(msg);
    }

    @Override
    public String toString() {
        return "Pair{"
                + "key=" + key
                + ", value=" + value
                + '}';
    }
}
