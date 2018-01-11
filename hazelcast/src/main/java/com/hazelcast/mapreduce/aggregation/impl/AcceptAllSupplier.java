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

package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.mapreduce.aggregation.PropertyExtractor;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;

/**
 * A standard implementation of the {@link com.hazelcast.mapreduce.aggregation.Supplier}
 * interface which accepts all input values and may apply a given
 * {@link com.hazelcast.mapreduce.aggregation.PropertyExtractor} on those.
 *
 * @param <KeyIn>    the input key type
 * @param <ValueIn>  the input value type
 * @param <ValueOut> the output value type
 */
public class AcceptAllSupplier<KeyIn, ValueIn, ValueOut>
        extends Supplier<KeyIn, ValueIn, ValueOut>
        implements IdentifiedDataSerializable {

    private PropertyExtractor<ValueIn, ValueOut> propertyExtractor;

    AcceptAllSupplier() {
    }

    public AcceptAllSupplier(PropertyExtractor<ValueIn, ValueOut> propertyExtractor) {
        this.propertyExtractor = propertyExtractor;
    }

    @Override
    public ValueOut apply(Map.Entry<KeyIn, ValueIn> entry) {
        ValueIn value = entry.getValue();
        return propertyExtractor != null ? propertyExtractor.extract(value) : (ValueOut) value;
    }

    @Override
    public int getFactoryId() {
        return AggregationsDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return AggregationsDataSerializerHook.ACCEPT_ALL_SUPPLIER;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {

        out.writeObject(propertyExtractor);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {

        propertyExtractor = in.readObject();
    }
}
