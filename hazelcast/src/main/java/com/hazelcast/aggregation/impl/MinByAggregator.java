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

public final class MinByAggregator<I> extends AbstractAggregator<I, Comparable, I>
        implements IdentifiedDataSerializable {

    private Comparable minValue;
    private I minEntry;

    public MinByAggregator() {
        super();
    }

    public MinByAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulateExtracted(I entry, Comparable value) {
        if (isCurrentlyGreaterThan(value)) {
            minValue = value;
            minEntry = entry;
        }
    }

    private boolean isCurrentlyGreaterThan(Comparable otherValue) {
        if (otherValue == null) {
            return false;
        }
        return minValue == null || minValue.compareTo(otherValue) > 0;
    }

    @Override
    public void combine(Aggregator aggregator) {
        MinByAggregator<I> minAggregator = (MinByAggregator<I>) aggregator;
        Comparable valueFromOtherAggregator = minAggregator.minValue;
        if (isCurrentlyGreaterThan(valueFromOtherAggregator)) {
            this.minValue = valueFromOtherAggregator;
            this.minEntry = minAggregator.minEntry;
        }
    }

    @Override
    public I aggregate() {
        return minEntry;
    }

    @Override
    public int getFactoryId() {
        return AggregatorDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return AggregatorDataSerializerHook.MIN_BY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attributePath);
        out.writeObject(minValue);
        out.writeObject(minEntry);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.attributePath = in.readUTF();
        this.minValue = in.readObject();
        this.minEntry = in.readObject();
    }
}
