/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.aggregation;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.aggregation.impl.SimpleEntry;
import com.hazelcast.mapreduce.impl.task.DefaultContext;

/**
 * Base class for mapper implementations used in aggregations.
 *
 * @param <KeyIn>    the input key type
 * @param <ValueIn>  the input value type
 * @param <KeyOut>   the output key type
 * @param <ValueOut> the output value type
 */
public abstract class AbstractSupplyingMapper<KeyIn, ValueIn, KeyOut, ValueOut>
        implements Mapper<KeyIn, ValueIn, KeyOut, ValueOut> {

    protected Supplier<KeyIn, ValueIn, ValueOut> supplier;

    private transient SimpleEntry<KeyIn, ValueIn> entry = new SimpleEntry<KeyIn, ValueIn>();

    public AbstractSupplyingMapper() {
    }

    public AbstractSupplyingMapper(Supplier<KeyIn, ValueIn, ValueOut> supplier) {
        this.supplier = supplier;
    }

    @Override
    public final void map(KeyIn key, ValueIn value, Context<KeyOut, ValueOut> context) {
        entry.setKey(key);
        entry.setValue(value);
        entry.setSerializationService(((DefaultContext) context).getSerializationService());
        ValueOut supplied = supplier.apply(entry);
        mapSupplied(key, supplied, context);
    }

    /**
     * This method is called for each {@link #map} after the {@link Supplier} has been applied to the key-value pair.
     *
     * @param key      key to map
     * @param supplied supplied value
     * @param context  Context to be used for emitting values
     */
    protected abstract void mapSupplied(KeyIn key, ValueOut supplied, Context<KeyOut, ValueOut> context);
}
