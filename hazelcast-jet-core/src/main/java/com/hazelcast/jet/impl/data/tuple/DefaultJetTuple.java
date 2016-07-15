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

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

public class DefaultJetTuple extends DefaultTuple implements JetTuple {
    private int partitionId;

    private CalculationStrategy calculationStrategy;

    public DefaultJetTuple(Object c0, Object c1, int partitionId, CalculationStrategy calculationStrategy) {
        super(c0, c1);
        this.partitionId = partitionId;
        this.calculationStrategy = calculationStrategy;
    }

    public DefaultJetTuple(Object[] components, int partitionId, CalculationStrategy calculationStrategy) {
        super(components);
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
        return toData(store.length, 0, calculationStrategy, nodeEngine);
    }

    @Override
    public Data getComponentData(int index, CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        return nodeEngine.getSerializationService().toData(
                this.store[index], calculationStrategy.getPartitioningStrategy());
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(partitionId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        partitionId = in.readInt();
    }

    @Override
    public CalculationStrategy getCalculationStrategy() {
        return this.calculationStrategy;
    }

    private Data toData(int size, int offset, CalculationStrategy calculationStrategy, NodeEngine nodeEngine) {
        if (size == 1) {
            return nodeEngine.getSerializationService().toData(
                    store[offset], calculationStrategy.getPartitioningStrategy());
        }
        BufferObjectDataOutput output =
                ((InternalSerializationService) nodeEngine.getSerializationService()).createObjectDataOutput();
        for (int i = 0; i < size; i++) {
            try {
                output.writeObject(store[offset + i]);
            } catch (IOException e) {
                throw JetUtil.reThrow(e);
            }
        }
        return new HeapData(output.toByteArray());
    }
}
