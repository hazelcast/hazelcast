/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

public final class MaxAggregator<I, R extends Comparable> extends AbstractAggregator<I, R, R>
        implements IdentifiedDataSerializable {

    private R max;

    public MaxAggregator() {
        super();
    }

    public MaxAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulateExtracted(R value) {
        if (isCurrentlyLessThan(value)) {
            max = value;
        }
    }

    private boolean isCurrentlyLessThan(R otherValue) {
        if (otherValue == null) {
            return false;
        }
        return max == null || max.compareTo(otherValue) < 0;
    }

    @Override
    public void combine(Aggregator aggregator) {
        MaxAggregator maxAggregator = (MaxAggregator) aggregator;
        R valueFromOtherAggregator = (R) maxAggregator.max;
        if (isCurrentlyLessThan(valueFromOtherAggregator)) {
            this.max = valueFromOtherAggregator;
        }
    }

    @Override
    public R aggregate() {
        return max;
    }

    @Override
    public int getFactoryId() {
        return AggregatorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return AggregatorDataSerializerHook.MAX;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attributePath);
        out.writeObject(max);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.attributePath = in.readUTF();
        this.max = in.readObject();
    }
}
