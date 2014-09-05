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

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;

/**
 * The default mapper implementation for most (but not DistinctValues) aggregations.
 *
 * @param <Key>      the input key type
 * @param <ValueIn>  the input value type
 * @param <ValueOut> the output value type
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings("SE_NO_SERIALVERSIONID")
class SupplierConsumingMapper<Key, ValueIn, ValueOut>
        implements Mapper<Key, ValueIn, Key, ValueOut>, IdentifiedDataSerializable {

    private transient SimpleEntry<Key, ValueIn> entry = new SimpleEntry<Key, ValueIn>();

    private Supplier<Key, ValueIn, ValueOut> supplier;

    SupplierConsumingMapper() {
    }

    SupplierConsumingMapper(Supplier<Key, ValueIn, ValueOut> supplier) {
        this.supplier = supplier;
    }

    @Override
    public void map(Key key, ValueIn value, Context<Key, ValueOut> context) {
        entry.key = key;
        entry.value = value;
        ValueOut valueOut = supplier.apply(entry);
        if (valueOut != null) {
            context.emit(key, valueOut);
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

    /**
     * Internal implementation of an map entry with changeable value to prevent
     * to much object allocation while supplying
     *
     * @param <K> key type
     * @param <V> value type
     */
    private static final class SimpleEntry<K, V>
            implements Map.Entry<K, V> {

        private K key;
        private V value;

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public V setValue(V value) {
            throw new UnsupportedOperationException();
        }
    }
}
