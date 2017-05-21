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

package com.hazelcast.mapreduce.aggregation.impl;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.aggregation.AbstractSupplyingMapper;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.BinaryInterface;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * The default mapper implementation for most (but not DistinctValues) aggregations.
 *
 * @param <Key>      the input key type
 * @param <ValueIn>  the input value type
 * @param <ValueOut> the output value type
 */
@SuppressFBWarnings("SE_NO_SERIALVERSIONID")
@BinaryInterface
class SupplierConsumingMapper<Key, ValueIn, ValueOut>
        extends AbstractSupplyingMapper<Key, ValueIn, Key, ValueOut> implements IdentifiedDataSerializable {

    SupplierConsumingMapper() {
    }

    SupplierConsumingMapper(Supplier<Key, ValueIn, ValueOut> supplier) {
        super(supplier);
    }

    @Override
    protected void mapSupplied(Key key, ValueOut supplied, Context<Key, ValueOut> context) {
        if (supplied != null) {
            context.emit(key, supplied);
        }
    }

    @Override
    public int getFactoryId() {
        return AggregationsDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return AggregationsDataSerializerHook.SUPPLIER_CONSUMING_MAPPER;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {

        out.writeObject(supplier);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {

        supplier = in.readObject();
    }

}
