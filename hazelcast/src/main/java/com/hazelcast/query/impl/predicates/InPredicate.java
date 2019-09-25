/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.query.impl.Comparables;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;

/**
 * In Predicate
 */
@BinaryInterface
public class InPredicate extends AbstractIndexAwarePredicate {

    private static final long serialVersionUID = 1L;

    Comparable[] values;
    private transient volatile Set<Comparable> convertedInValues;

    public InPredicate() {
    }

    public InPredicate(String attribute, Comparable... values) {
        super(attribute);

        if (values == null) {
            throw new NullPointerException("Array can't be null");
        }
        this.values = values;
    }

    @Override
    protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
        if (attributeValue == null) {
            return false;
        }
        attributeValue = (Comparable) convertEnumValue(attributeValue);
        Set<Comparable> set = convertedInValues;
        if (set == null) {
            set = createHashSet(values.length);
            for (Comparable value : values) {
                Comparable converted = convert(attributeValue, value);
                set.add(Comparables.canonicalizeForHashLookup(converted));
            }
            convertedInValues = set;
        }
        return set.contains(Comparables.canonicalizeForHashLookup(attributeValue));
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = matchIndex(queryContext, QueryContext.IndexMatchHint.PREFER_UNORDERED);
        if (index != null) {
            return index.getRecords(values);
        } else {
            return null;
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeInt(values.length);
        for (Object value : values) {
            out.writeObject(value);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        int len = in.readInt();
        values = new Comparable[len];
        for (int i = 0; i < len; i++) {
            values[i] = in.readObject();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(attributeName);
        sb.append(" IN (");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(values[i]);
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.IN_PREDICATE;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        if (!(o instanceof InPredicate)) {
            return false;
        }

        InPredicate that = (InPredicate) o;
        if (!that.canEqual(this)) {
            return false;
        }

        return Arrays.equals(values, that.values);
    }

    @Override
    public boolean canEqual(Object other) {
        return (other instanceof InPredicate);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (Arrays.hashCode(values));
        return result;
    }

}
