/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.AttributeType;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.ReflectionHelper;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

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
        QueryableEntry queryableEntry;
        if (entry instanceof QueryableEntry) {
            queryableEntry = (QueryableEntry) entry;
        } else {
            queryableEntry = new SimpleQueryableEntry(entry);
        }

        if (predicate.apply(queryableEntry)) {
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

    private static final class SimpleQueryableEntry implements QueryableEntry {

        final Map.Entry entry;

        private SimpleQueryableEntry(Map.Entry entry) {
            this.entry = entry;
        }

        @Override
        public Object getValue() {
            return entry.getValue();
        }

        @Override
        public Object setValue(Object value) {
            return entry.setValue(value);
        }

        @Override
        public Object getKey() {
            return entry.getKey();
        }

        @Override
        public Comparable getAttribute(String attributeName) throws QueryException {
            if (KEY_ATTRIBUTE_NAME.equals(attributeName)) {
                return (Comparable) getKey();
            } else if (THIS_ATTRIBUTE_NAME.equals(attributeName)) {
                return (Comparable) getValue();
            }

            try {
                return ReflectionHelper.extractValue(entry.getValue(), attributeName);
            } catch (Exception e) {
                throw new QueryException(e);
            }
        }

        @Override
        public AttributeType getAttributeType(String attributeName) {
            if (KEY_ATTRIBUTE_NAME.equals(attributeName)) {
                return ReflectionHelper.getAttributeType(getKey().getClass());
            } else if (THIS_ATTRIBUTE_NAME.equals(attributeName)) {
                return ReflectionHelper.getAttributeType(getValue().getClass());
            }

            return ReflectionHelper.getAttributeType(entry.getValue(), attributeName);
        }

        @Override
        public Data getKeyData() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Data getValueData() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Data getIndexKey() {
            throw new UnsupportedOperationException();
        }
    }
}
