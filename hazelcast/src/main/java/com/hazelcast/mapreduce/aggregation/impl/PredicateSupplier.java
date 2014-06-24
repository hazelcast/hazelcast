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

import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.Map;

/**
 * The default supplier for {@link com.hazelcast.query.Predicate}s, used
 * to filter and optionally transform data (using the given
 * {@link com.hazelcast.mapreduce.aggregation.Supplier}).
 *
 * @param <KeyIn>    the input key type
 * @param <ValueIn>  the input value type
 * @param <ValueOut> the output value type
 */
public class PredicateSupplier<KeyIn, ValueIn, ValueOut>
        extends Supplier<KeyIn, ValueIn, ValueOut>
        implements IdentifiedDataSerializable {

    private Predicate<KeyIn, ValueIn> predicate;
    private Supplier<KeyIn, ValueIn, ValueOut> chainedSupplier;

    PredicateSupplier() {
    }

    public PredicateSupplier(Predicate<KeyIn, ValueIn> predicate) {
        this(predicate, null);
    }

    public PredicateSupplier(Predicate<KeyIn, ValueIn> predicate, Supplier<KeyIn, ValueIn, ValueOut> chainedSupplier) {
        this.predicate = predicate;
        this.chainedSupplier = chainedSupplier;
    }

    @Override
    public ValueOut apply(Map.Entry<KeyIn, ValueIn> entry) {
        if (predicate.apply(entry)) {
            ValueIn value = entry.getValue();
            if (value != null) {
                return chainedSupplier != null ? chainedSupplier.apply(entry) : (ValueOut) value;
            }
        }
        return null;
    }

    @Override
    public int getFactoryId() {
        return AggregationsDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return AggregationsDataSerializerHook.PREDICATE_SUPPLIER;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {

        out.writeObject(predicate);
        out.writeObject(chainedSupplier);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {

        predicate = in.readObject();
        chainedSupplier = in.readObject();
    }
}
