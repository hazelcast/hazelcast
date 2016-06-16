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
import com.hazelcast.jet.data.tuple.JetTuple;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.io.tuple.DefaultTuple;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.serialization.SerializationService;

import java.io.IOException;
import java.util.Arrays;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DefaultJetTuple<K, V> extends DefaultTuple<K, V> implements JetTuple<K, V> {
    private int partitionId;

    private CalculationStrategy calculationStrategy;

    public DefaultJetTuple(K key,
                           V value,
                           int partitionId,
                           CalculationStrategy calculationStrategy) {
        super(key, value);
        this.partitionId = partitionId;
        this.calculationStrategy = calculationStrategy;
    }

    public DefaultJetTuple(K key,
                           V[] values,
                           int partitionId,
                           CalculationStrategy calculationStrategy) {
        super(key, values);

        this.partitionId = partitionId;
        this.calculationStrategy = calculationStrategy;
    }

    public DefaultJetTuple(K key,
                           V[] values) {
        this(key, values, -1, null);
    }

    public DefaultJetTuple(K[] key,
                           V[] values) {
        this(key, values, -1, null);
    }

    public DefaultJetTuple(K[] key,
                           V[] values,
                           int partitionId,
                           CalculationStrategy calculationStrategy) {
        super(key, values);
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
    public Data getValueData(CalculationStrategy calculationStrategy,
                             NodeEngine nodeEngine) {
        return toData(this.valueSize, this.keySize, calculationStrategy, nodeEngine);
    }

    @Override
    public Data getKeyData(int index,
                           CalculationStrategy calculationStrategy,
                           NodeEngine nodeEngine) {
        checkKeyIndex(index);

        return nodeEngine.getSerializationService().toData(
                this.data[index],
                calculationStrategy.getPartitioningStrategy()
        );
    }

    @Override
    public Data getValueData(int index,
                             CalculationStrategy calculationStrategy,
                             NodeEngine nodeEngine) {
        checkValueIndex(index);
        SerializationService serializationService = nodeEngine.getSerializationService();
        PartitioningStrategy partitioningStrategy = calculationStrategy.getPartitioningStrategy();
        return serializationService.toData(this.data[this.keySize + index], partitioningStrategy);
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(this.partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        this.partitionId = in.readInt();
    }

    @Override
    public CalculationStrategy getCalculationStrategy() {
        return this.calculationStrategy;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(data);
        result = 31 * result + keySize;
        result = 31 * result + valueSize;
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DefaultJetTuple<?, ?> tuple = (DefaultJetTuple<?, ?>) o;

        if (keySize != tuple.keySize) {
            return false;
        }

        if (valueSize != tuple.valueSize) {
            return false;
        }

        return Arrays.equals(data, tuple.data);
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
}
