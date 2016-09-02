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

package com.hazelcast.jet.runtime;

import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import java.io.IOException;

/**
 * Jet Pair implementation
 *
 * @param <K> type of component 0
 * @param <V> type of component 1
 */
public class JetPair<K, V> extends Pair<K, V> {
    private int partitionId;

    /**
     * Creates a pair with the given components, partition ID -1, and no CalculationStrategy.
     */
    public JetPair(K key, V value) {
        this(key, value, -1);
    }

    /**
     * Creates a pair with the given components, partition ID and CalculationStrategy.
     */
    public JetPair(K key, V value, int partitionId) {
        super(key, value);
        this.partitionId = partitionId;
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

    /**
     * Retrieves the component at the specified index as a {@code Data} instance.
     *
     * @param index               component index: 0 for key, 1 for value
     * @param calculationStrategy used to calculate the partition hash, which is a part of the {@code Data} format
     * @param serService          Hazelcast serialization service which will serialize the component
     * @return the Data instance representing the component's value
     */
    public Data getComponentData(int index, CalculationStrategy calculationStrategy, SerializationService serService) {
        return serService.toData(get(index), calculationStrategy.getPartitioningStrategy());
    }

    /**
     * Returns the partition ID of this pair.
     */
    public int getPartitionId() {
        return partitionId;
    }
}
