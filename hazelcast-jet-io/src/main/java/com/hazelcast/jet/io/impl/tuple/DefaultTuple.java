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

package com.hazelcast.jet.io.impl.tuple;

import com.hazelcast.jet.io.spi.tuple.Tuple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DefaultTuple<K, V> implements Tuple<K, V> {
    protected Object[] data;

    protected int keySize;
    protected int valueSize;

    public DefaultTuple() {

    }

    public DefaultTuple(K key, V value) {
        checkNotNull(key);
        checkNotNull(value);

        this.data = new Object[2];

        this.keySize = 1;
        this.valueSize = 1;

        this.data[0] = key;
        this.data[1] = value;
    }

    public DefaultTuple(K[] keys, V value) {
        checkNotNull(keys);
        checkNotNull(value);

        this.data = new Object[keys.length + 1];
        this.keySize = keys.length;
        this.valueSize = 1;
        System.arraycopy(keys, 0, this.data, 0, keys.length);
        this.data[this.keySize] = value;
    }

    public DefaultTuple(K key, V[] values) {
        checkNotNull(key);
        checkNotNull(values);

        this.data = new Object[1 + values.length];
        this.keySize = 1;
        this.valueSize = values.length;
        data[0] = key;
        System.arraycopy(values, 0, this.data, this.keySize, values.length);
    }

    public DefaultTuple(K[] keys, V[] values) {
        checkNotNull(keys);
        checkNotNull(values);

        this.data = new Object[keys.length + values.length];
        this.keySize = keys.length;
        this.valueSize = values.length;
        System.arraycopy(keys, 0, this.data, 0, keys.length);
        System.arraycopy(values, 0, this.data, this.keySize, values.length);
    }

    @Override
    @SuppressWarnings("unchecked")
    public K getKey(int index) {
        checkKeyIndex(index);
        return (K) data[index];
    }

    @Override
    @SuppressWarnings("unchecked")
    public V getValue(int index) {
        checkValueIndex(index);
        return (V) data[this.keySize + index];
    }

    @Override
    public int keySize() {
        return this.keySize;
    }

    @Override
    public int valueSize() {
        return this.valueSize;
    }

    @Override
    public void setKey(int index, K value) {
        checkKeyIndex(index);
        this.data[index] = value;
    }

    @Override
    public void setValue(int index, V value) {
        checkValueIndex(index);
        this.data[this.keySize + index] = value;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K[] cloneKeys() {
        K[] tmp = (K[]) new Object[this.keySize];
        System.arraycopy(this.data, 0, tmp, 0, this.keySize);
        return tmp;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V[] cloneValues() {
        V[] tmp = (V[]) new Object[this.valueSize];
        System.arraycopy(this.data, this.keySize, tmp, 0, this.valueSize);
        return tmp;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(this.data);
        out.writeInt(this.keySize);
        out.writeInt(this.valueSize);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.data = in.readObject();
        this.keySize = in.readInt();
        this.valueSize = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultTuple<?, ?> tuple = (DefaultTuple<?, ?>) o;

        if (keySize != tuple.keySize) {
            return false;
        }

        if (valueSize != tuple.valueSize) {
            return false;
        }

        return Arrays.equals(data, tuple.data);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(data);
        result = 31 * result + keySize;
        result = 31 * result + valueSize;
        return result;
    }

    protected void checkKeyIndex(int index) {
        if ((index < 0) || (index >= this.keySize)) {
            throw new IllegalStateException("Invalid index for tupleKey");
        }
    }

    protected void checkValueIndex(int index) {
        if ((index < 0) || (index >= this.valueSize)) {
            throw new IllegalStateException("Invalid index for tupleValue");
        }
    }
}
