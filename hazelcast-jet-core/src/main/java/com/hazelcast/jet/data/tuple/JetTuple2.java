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

package com.hazelcast.jet.data.tuple;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class JetTuple2<K, V> extends Tuple2<K, V> implements JetTuple<K, V> {
    private int partitionId;
    private final CalculationStrategy calculationStrategy;

    public JetTuple2(K key, V value) {
        this(key, value, -1, null);
    }

    JetTuple2(K key,
              V value,
              int partitionId,
              CalculationStrategy calculationStrategy) {
        super(key, value);
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
        assert index == 0;
        return nodeEngine.getSerializationService().toData(key, calculationStrategy.getPartitioningStrategy());
    }

    @Override
    public Data getValueData(int index, CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        assert index == 0;
        return nodeEngine.getSerializationService().toData(value, calculationStrategy.getPartitioningStrategy());
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
        return calculationStrategy;
    }

    private Data toData(Object obj, PartitioningStrategy partitioningStrategy, NodeEngine nodeEngine) {
        return nodeEngine.getSerializationService().toData(obj, partitioningStrategy);
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }
}
