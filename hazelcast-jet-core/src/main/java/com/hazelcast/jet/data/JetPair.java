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

package com.hazelcast.jet.data;

import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Jet Pair implementation
 *
 * @param <T0> type of component 0
 * @param <T1> type of component 1
 */
public class JetPair<T0, T1> extends Pair<T0, T1> {
    private int partitionId;

    /**
     * Creates a pair with the given components, partition ID -1, and no CalculationStrategy.
     */
    public JetPair(T0 c0, T1 c1) {
        this(c0, c1, -1);
    }

    /**
     * Creates a pair with the given components, partition ID and CalculationStrategy.
     */
    public JetPair(T0 c0, T1 c1, int partitionId) {
        super(c0, c1);
        this.partitionId = partitionId;
    }

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

    public int getPartitionId() {
        return partitionId;
    }
}
