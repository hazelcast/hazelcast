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
    public void accumulateExtracted(I entry, R value) {
        if (isCurrentlyLessThan(value)) {
            max = value;
        }
    }

    private boolean isCurrentlyLessThan(R otherValue) {
        if (otherValue == null) {
            return false;
        }
        return max == null || Comparables.compare(max, otherValue) < 0;
    }

    @SuppressWarnings("unchecked")
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
    public int getClassId() {
        return AggregatorDataSerializerHook.MAX;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(attributePath);
        out.writeObject(max);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.attributePath = in.readString();
        this.max = in.readObject();
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
        MaxAggregator<?, ?> that = (MaxAggregator<?, ?>) o;
        return Objects.equals(max, that.max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), max);
    }
}
