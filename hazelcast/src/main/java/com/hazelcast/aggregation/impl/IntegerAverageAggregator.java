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
import com.hazelcast.query.impl.Numbers;

import java.io.IOException;
import java.util.Objects;

public final class IntegerAverageAggregator<I> extends AbstractAggregator<I, Number, Double>
        implements IdentifiedDataSerializable {

    private long sum;

    private long count;

    public IntegerAverageAggregator() {
        super();
    }

    public IntegerAverageAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulateExtracted(I entry, Number value) {
        count++;
        sum += Numbers.asIntExactly(value);
    }

    @Override
    public void combine(Aggregator aggregator) {
        IntegerAverageAggregator integerAverageAggregator = (IntegerAverageAggregator) aggregator;
        this.sum += integerAverageAggregator.sum;
        this.count += integerAverageAggregator.count;
    }

    @Override
    public Double aggregate() {
        if (count == 0) {
            return null;
        }
        return ((double) sum / (double) count);
    }

    @Override
    public int getFactoryId() {
        return AggregatorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return AggregatorDataSerializerHook.INT_AVG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(attributePath);
        out.writeLong(sum);
        out.writeLong(count);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.attributePath = in.readString();
        this.sum = in.readLong();
        this.count = in.readLong();
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
        IntegerAverageAggregator<?> that = (IntegerAverageAggregator<?>) o;
        return sum == that.sum && count == that.count;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sum, count);
    }
}
