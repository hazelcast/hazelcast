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

public final class MinAggregator<I, R extends Comparable> extends AbstractAggregator<I, R, R>
        implements IdentifiedDataSerializable {

    private R min;

    public MinAggregator() {
        super();
    }

    public MinAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulateExtracted(I entry, R value) {
        if (isCurrentlyGreaterThan(value)) {
            min = value;
        }
    }

    private boolean isCurrentlyGreaterThan(R otherValue) {
        if (otherValue == null) {
            return false;
        }
        return min == null || min.compareTo(otherValue) > 0;
    }

    @Override
    public void combine(Aggregator aggregator) {
        MinAggregator maxAggregator = (MinAggregator) aggregator;
        R valueFromOtherAggregator = (R) maxAggregator.min;
        if (isCurrentlyGreaterThan(valueFromOtherAggregator)) {
            this.min = valueFromOtherAggregator;
        }
    }

    @Override
    public R aggregate() {
        return min;
    }

    @Override
    public int getFactoryId() {
        return AggregatorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return AggregatorDataSerializerHook.MIN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attributePath);
        out.writeObject(min);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.attributePath = in.readUTF();
        this.min = in.readObject();
    }
}
