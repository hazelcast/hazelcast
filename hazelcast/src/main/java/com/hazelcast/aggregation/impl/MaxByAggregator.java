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
import com.hazelcast.query.impl.Comparables;

import java.io.IOException;
import java.util.Objects;

public final class MaxByAggregator<I> extends AbstractAggregator<I, Comparable, I> implements IdentifiedDataSerializable {

    private Comparable maxValue;
    private I maxEntry;

    public MaxByAggregator() {
        super();
    }

    public MaxByAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulateExtracted(I entry, Comparable value) {
        if (isCurrentlyLessThan(value)) {
            maxValue = value;
            maxEntry = entry;
        }
    }

    private boolean isCurrentlyLessThan(Comparable otherValue) {
        if (otherValue == null) {
            return false;
        }
        return maxValue == null || Comparables.compare(maxValue, otherValue) < 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void combine(Aggregator aggregator) {
        MaxByAggregator<I> maxAggregator = (MaxByAggregator<I>) aggregator;
        Comparable valueFromOtherAggregator = maxAggregator.maxValue;
        if (isCurrentlyLessThan(valueFromOtherAggregator)) {
            this.maxValue = valueFromOtherAggregator;
            this.maxEntry = maxAggregator.maxEntry;
        }
    }

    @Override
    public I aggregate() {
        return maxEntry;
    }

    @Override
    public int getFactoryId() {
        return AggregatorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return AggregatorDataSerializerHook.MAX_BY;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(attributePath);
        out.writeObject(maxValue);
        out.writeObject(maxEntry);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.attributePath = in.readString();
        this.maxValue = in.readObject();
        this.maxEntry = in.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        MaxByAggregator<?> that = (MaxByAggregator<?>) o;
        return Objects.equals(maxValue, that.maxValue) && Objects.equals(maxEntry, that.maxEntry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), maxValue, maxEntry);
    }
}
