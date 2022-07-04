/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import java.math.BigInteger;

public final class BigIntegerSumAggregator<I> extends AbstractAggregator<I, BigInteger, BigInteger>
        implements IdentifiedDataSerializable {

    private BigInteger sum = BigInteger.ZERO;

    public BigIntegerSumAggregator() {
        super();
    }

    public BigIntegerSumAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulateExtracted(I entry, BigInteger value) {
        sum = sum.add(value);
    }

    @Override
    public void combine(Aggregator aggregator) {
        BigIntegerSumAggregator longSumAggregator = (BigIntegerSumAggregator) aggregator;
        sum = sum.add(longSumAggregator.sum);
    }

    @Override
    public BigInteger aggregate() {
        return sum;
    }

    @Override
    public int getFactoryId() {
        return AggregatorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return AggregatorDataSerializerHook.BIG_INT_SUM;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(attributePath);
        out.writeObject(sum);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.attributePath = in.readString();
        this.sum = in.readObject();
    }

}
