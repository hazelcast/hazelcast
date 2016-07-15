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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Jet Tuple implementation
 *
 * @param <T0> type of component 0
 * @param <T1> type of component 1
 */
public class JetTuple2<T0, T1> extends Tuple2<T0, T1> implements JetTuple {
    private int partitionId;
    private final CalculationStrategy calculationStrategy;

    /**
     * Creates a tuple with the given components, partition ID -1, and no CalculationStrategy.
     */
    public JetTuple2(T0 c0, T1 c1) {
        this(c0, c1, -1, null);
    }

    /**
     * Creates a tuple with the given components, partition ID and CalculationStrategy.
     */
    public JetTuple2(T0 c0, T1 c1, int partitionId, CalculationStrategy calculationStrategy) {
        super(c0, c1);
        this.partitionId = partitionId;
        this.calculationStrategy = calculationStrategy;
    }

    @Override
    public Data getComponentData(NodeEngine nodeEngine) {
        checkNotNull(calculationStrategy);
        return getComponentData(calculationStrategy, nodeEngine);
    }

    @Override
    public Data getComponentData(CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        BufferObjectDataOutput output =
                ((InternalSerializationService) nodeEngine.getSerializationService()).createObjectDataOutput();
        try {
            output.writeObject(c0);
            output.writeObject(c1);
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }
        return new HeapData(output.toByteArray());
    }

    @Override
    public Data getComponentData(int index, CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        return nodeEngine.getSerializationService().toData(get(index), calculationStrategy.getPartitioningStrategy());
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

    @Override
    public int getPartitionId() {
        return partitionId;
    }
}
