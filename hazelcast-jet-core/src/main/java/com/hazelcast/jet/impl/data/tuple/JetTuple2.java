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
import com.hazelcast.jet.spi.data.tuple.JetTuple;
import com.hazelcast.jet.spi.strategy.CalculationStrategy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class JetTuple2<K, V> implements JetTuple<K, V> {
    private K key;
    private V value;

    private int keySize;
    private int valueSize;
    private int partitionId;

    private CalculationStrategy calculationStrategy;

    public JetTuple2() {
        this.keySize = 1;
        this.valueSize = 1;
    }

    public JetTuple2(K key, V value) {
        this(key, value, -1, null);
    }

    JetTuple2(K key, V value, int partitionId, CalculationStrategy calculationStrategy) {
        checkNotNull(key);
        checkNotNull(value);

        this.keySize = 1;
        this.valueSize = 1;

        this.key = key;
        this.value = value;
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
        return toData(this.key, calculationStrategy.getPartitioningStrategy(), nodeEngine);
    }

    @Override
    public Data getValueData(CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        return toData(this.value, calculationStrategy.getPartitioningStrategy(), nodeEngine);
    }

    @Override
    public Data getKeyData(int index, CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        checkKeyIndex(index);
        return nodeEngine.getSerializationService().toData(key, calculationStrategy.getPartitioningStrategy());
    }

    @Override
    public Data getValueData(int index, CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        checkValueIndex(index);
        return nodeEngine.getSerializationService().toData(value, calculationStrategy.getPartitioningStrategy());
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
    public K getKey(int index) {
        checkKeyIndex(index);
        return key;
    }

    @Override
    public V getValue(int index) {
        checkValueIndex(index);
        return value;
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
        this.key = value;
    }

    @Override
    public void setValue(int index, V value) {
        checkValueIndex(index);
        this.value = value;
    }

    @Override
    public int getPartitionId() {
        return this.partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(this.key);
        out.writeObject(this.value);
        out.writeInt(this.keySize);
        out.writeInt(this.valueSize);
        out.writeInt(this.partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.key = in.readObject();
        this.value = in.readObject();
        this.keySize = in.readInt();
        this.valueSize = in.readInt();
        this.partitionId = in.readInt();
    }

    @Override
    public CalculationStrategy getCalculationStrategy() {
        return calculationStrategy;
    }

    private Data toData(Object obj, PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        return nodeEngine.getSerializationService().toData(obj, partitioningStrategy);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JetTuple2<?, ?> tuple2 = (JetTuple2<?, ?>) o;

        if (!key.equals(tuple2.key)) {
            return false;
        }

        return value.equals(tuple2.value);

    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
