/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.aggregation.impl;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public final class IntegerSumAggregator<I> extends AbstractAggregator<I, Integer, Long>
        implements IdentifiedDataSerializable {

    private long sum;

    public IntegerSumAggregator() {
        super();
    }

    public IntegerSumAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulateExtracted(I entry, Integer value) {
        sum += value;
    }

    @Override
    public void combine(Aggregator aggregator) {
        IntegerSumAggregator integerSumAggregator = (IntegerSumAggregator) aggregator;
        this.sum += integerSumAggregator.sum;
    }

    @Override
    public Long aggregate() {
        return sum;
    }

    @Override
    public int getFactoryId() {
        return AggregatorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return AggregatorDataSerializerHook.INT_SUM;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attributePath);
        out.writeLong(sum);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.attributePath = in.readUTF();
        this.sum = in.readLong();
    }

}
