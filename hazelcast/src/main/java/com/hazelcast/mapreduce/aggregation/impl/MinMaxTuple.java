/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Tuple type for min/max aggregations.
 *
 * @param <T> value type
 */
public class MinMaxTuple<T extends Comparable>
        implements IdentifiedDataSerializable {

    private T minimum;
    private T maximum;

    MinMaxTuple() {
    }

    public MinMaxTuple(T minimum, T maximum) {
        this.minimum = minimum;
        this.maximum = maximum;
    }

    public T getMinimum() {
        return minimum;
    }

    public T getMaximum() {
        return maximum;
    }

    void update(MinMaxTuple<T> value) {
        update(value.minimum);
        update(value.maximum);
    }

    void update(T value) {
        if ((minimum == null)
                || (value != null && minimum.compareTo(value) > 0))  {
            minimum = value;
        }

        if ((maximum == null)
                || (value != null && maximum.compareTo(value) < 0))  {
            maximum = value;
        }
    }

    @Override
    public int getFactoryId() {
        return AggregationsDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return AggregationsDataSerializerHook.MINMAX_TUPLE;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {

        out.writeObject(minimum);
        out.writeObject(maximum);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {

        minimum = in.readObject();
        maximum = in.readObject();
    }
}
