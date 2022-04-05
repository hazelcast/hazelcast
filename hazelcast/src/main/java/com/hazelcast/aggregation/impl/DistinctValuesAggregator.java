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
import com.hazelcast.internal.util.MapUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

@SuppressFBWarnings("SE_BAD_FIELD")
public final class DistinctValuesAggregator<I, R>
        extends AbstractAggregator<I, R, Set<R>>
        implements IdentifiedDataSerializable {

    private CanonicalizingHashSet<R> values = new CanonicalizingHashSet<>();

    public DistinctValuesAggregator() {
        super();
    }

    public DistinctValuesAggregator(String attributePath) {
        super(attributePath);
    }

    @Override
    public void accumulateExtracted(I entry, R value) {
        values.addInternal(value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void combine(Aggregator aggregator) {
        DistinctValuesAggregator distinctValuesAggregator = (DistinctValuesAggregator) aggregator;
        this.values.addAllInternal(distinctValuesAggregator.values);
    }

    @Override
    public CanonicalizingHashSet<R> aggregate() {
        return values;
    }

    @Override
    public int getFactoryId() {
        return AggregatorDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return AggregatorDataSerializerHook.DISTINCT_VALUES;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(attributePath);
        out.writeInt(values.size());
        for (Object value : values) {
            out.writeObject(value);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.attributePath = in.readString();
        int count = in.readInt();
        this.values = new CanonicalizingHashSet<R>(MapUtil.calculateInitialCapacity(count));
        for (int i = 0; i < count; i++) {
            R value = in.readObject();
            values.addInternal(value);
        }
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
        DistinctValuesAggregator<?, ?> that = (DistinctValuesAggregator<?, ?>) o;
        return values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), values);
    }
}
