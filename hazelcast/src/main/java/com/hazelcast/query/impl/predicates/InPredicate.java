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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.internal.serialization.BinaryInterface;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Comparables;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static com.hazelcast.query.impl.predicates.PredicateUtils.isNull;

/**
 * In Predicate
 */
@BinaryInterface
public class InPredicate extends AbstractIndexAwarePredicate implements VisitablePredicate {

    private static final long serialVersionUID = 1L;

    Comparable[] values;
    private transient volatile Set<Comparable> convertedInValues;
    private transient volatile Boolean valuesContainNull;

    public InPredicate() {
    }

    public InPredicate(String attribute, Comparable... values) {
        super(attribute);

        if (values == null) {
            throw new NullPointerException("Array can't be null");
        }
        this.values = values;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public Comparable[] getValues() {
        return values;
    }

    @Override
    public Predicate accept(Visitor visitor, Indexes indexes) {
        return visitor.visit(this, indexes);
    }

    @Override
    protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
        Set<Comparable> set = convertedInValues;

        if (attributeValue == null && set == null) {
            Boolean valuesContainNull = this.valuesContainNull;
            if (valuesContainNull != null) {
                return valuesContainNull;
            }

            // Conversion of the values given to the predicate is possible only
            // if the passed attribute value is non-null, otherwise we are
            // unable to infer a proper converter. Postpone the conversion and
            // detect the presence of nulls.

            for (Comparable value : values) {
                if (isNull(value)) {
                    this.valuesContainNull = true;
                    return true;
                }
            }
            this.valuesContainNull = false;
            return false;
        }

        attributeValue = (Comparable) convertEnumValue(attributeValue);
        if (set == null) {
            set = createHashSet(values.length);
            for (Comparable value : values) {
                Comparable converted = convert(attributeValue, value);
                if (isNull(converted)) {
                    // Convert all kind of nulls to plain Java null, so we can
                    // match all of them using set.contains(...).
                    converted = null;
                }
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
