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

package com.hazelcast.jet.impl.data.tuple;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.data.tuple.Tuple;
import com.hazelcast.jet.spi.strategy.CalculationStrategy;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DefaultTuple<K, V> implements Tuple<K, V> {
    private Object[] data;

    private int keySize;
    private int valueSize;
    private int partitionId;

    private CalculationStrategy calculationStrategy;

    public DefaultTuple() {

    }

    DefaultTuple(K key, V value) {
        this(key, value, -1, null);
    }

    DefaultTuple(K key, V value, int partitionId, CalculationStrategy calculationStrategy) {
        checkNotNull(key);
        checkNotNull(value);

        this.data = new Object[2];

        this.keySize = 1;
        this.valueSize = 1;

        this.data[0] = key;
        this.data[1] = value;
        this.partitionId = partitionId;
        this.calculationStrategy = calculationStrategy;
    }

    DefaultTuple(K key, V[] values) {
        this(key, values, -1, null);
    }

    DefaultTuple(K key, V[] values, int partitionId, CalculationStrategy calculationStrategy) {
        checkNotNull(key);
        checkNotNull(values);

        this.data = new Object[1 + values.length];
        this.keySize = 1;
        this.valueSize = values.length;
        this.data[0] = key;
        System.arraycopy(values, 0, data, this.keySize, values.length);
        this.partitionId = partitionId;
        this.calculationStrategy = calculationStrategy;
    }

    DefaultTuple(K[] key, V[] values) {
        this(key, values, -1, null);
    }

    DefaultTuple(K[] key, V[] values, int partitionId, CalculationStrategy calculationStrategy) {
        checkNotNull(key);
        checkNotNull(values);

        this.data = new Object[key.length + values.length];

        this.keySize = key.length;
        this.valueSize = values.length;

        System.arraycopy(key, 0, this.data, 0, key.length);
        System.arraycopy(values, 0, this.data, this.keySize, values.length);

        this.partitionId = partitionId;
        this.calculationStrategy = calculationStrategy;
    }

    @Override
    public Data getKeyData(NodeEngine nodeEngine) {
        checkNotNull(this.calculationStrategy);
        return getKeyData(this.calculationStrategy, nodeEngine);
    }

    @Override
    public Data getValueData(NodeEngine nodeEngine) {
        checkNotNull(this.calculationStrategy);
        return getValueData(this.calculationStrategy, nodeEngine);
    }

    @Override
    public Data getKeyData(CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        return toData(this.keySize, 0, calculationStrategy, nodeEngine);
    }

    @Override
    public Data getValueData(CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        return toData(this.valueSize, this.keySize, calculationStrategy, nodeEngine);
    }

    @Override
    public Data getKeyData(int index, CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        checkKeyIndex(index);
        return nodeEngine.getSerializationService().toData(this.data[index], calculationStrategy.getPartitioningStrategy());
    }

    @Override
    public Data getValueData(int index, CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        checkValueIndex(index);
        SerializationService serializationService = nodeEngine.getSerializationService();
        PartitioningStrategy partitioningStrategy = calculationStrategy.getPartitioningStrategy();
        return serializationService.toData(this.data[this.keySize + index], partitioningStrategy);
    }

    @Override
    public K[] cloneKeys() {
        K[] tmp = (K[]) new Object[this.keySize];
        System.arraycopy(this.data, 0, tmp, 0, this.keySize);
        return tmp;
    }

    @Override
    public V[] cloneValues() {
        V[] tmp = (V[]) new Object[this.valueSize];
        System.arraycopy(this.data, this.keySize, tmp, 0, this.valueSize);
        return tmp;
    }

    @Override
    public K getKey(int index) {
        checkKeyIndex(index);
        return (K) data[index];
    }

    @Override
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
        this.partitionId = -1;
        this.data[index] = value;
    }

    @Override
    public void setValue(int index, V value) {
        checkValueIndex(index);
        this.data[this.keySize + index] = value;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(this.data);
        out.writeInt(this.keySize);
        out.writeInt(this.valueSize);
        out.writeInt(this.partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.data = in.readObject();
        this.keySize = in.readInt();
        this.valueSize = in.readInt();
        this.partitionId = in.readInt();
    }

    @Override
    public CalculationStrategy getCalculationStrategy() {
        return this.calculationStrategy;
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

    private Data toData(int size, int offset, CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        if (size == 1) {
            return nodeEngine.getSerializationService().toData(this.data[offset], calculationStrategy.getPartitioningStrategy());
        } else {
            BufferObjectDataOutput output = ((InternalSerializationService) nodeEngine.getSerializationService())
                    .createObjectDataOutput();

            for (int i = 0; i < size - 1; i++) {
                try {
                    output.writeObject(this.data[offset + i]);
                } catch (IOException e) {
                    throw JetUtil.reThrow(e);
                }
            }

            return new HeapData(output.toByteArray());
        }
    }

    private void checkKeyIndex(int index) {
        if ((index < 0) || (index >= this.keySize)) {
            throw new IllegalStateException("Invalid index for tupleKey");
        }
    }

    private void checkValueIndex(int index) {
        if ((index < 0) || (index >= this.valueSize)) {
            throw new IllegalStateException("Invalid index for tupleValue");
        }
    }
}
