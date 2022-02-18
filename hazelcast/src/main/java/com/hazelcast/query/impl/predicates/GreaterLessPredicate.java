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
import com.hazelcast.query.impl.Comparison;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.Set;

/**
 * Greater Less Predicate
 */
@BinaryInterface
public final class GreaterLessPredicate extends AbstractIndexAwarePredicate implements NegatablePredicate, RangePredicate {

    private static final long serialVersionUID = 1L;

    protected Comparable value;
    boolean equal;
    boolean less;

    public GreaterLessPredicate() {
    }

    public GreaterLessPredicate(String attribute, Comparable value, boolean equal, boolean less) {
        super(attribute);

        if (value == null) {
            throw new NullPointerException("Arguments can't be null");
        }

        this.value = value;
        this.equal = equal;
        this.less = less;
    }

    @Override
    protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
        if (attributeValue == null) {
            return false;
        }
        Comparable givenValue = convert(attributeValue, value);
        attributeValue = (Comparable) convertEnumValue(attributeValue);
        int result = Comparables.compare(attributeValue, givenValue);
        return equal && result == 0 || (less ? (result < 0) : (result > 0));
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = matchIndex(queryContext, QueryContext.IndexMatchHint.PREFER_ORDERED);
        if (index == null) {
            return null;
        }
        final Comparison comparison;
        if (less) {
            comparison = equal ? Comparison.LESS_OR_EQUAL : Comparison.LESS;
        } else {
            comparison = equal ? Comparison.GREATER_OR_EQUAL : Comparison.GREATER;
        }
        return index.getRecords(comparison, value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        value = in.readObject();
        equal = in.readBoolean();
        less = in.readBoolean();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(value);
        out.writeBoolean(equal);
        out.writeBoolean(less);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append(attributeName);
        sb.append(less ? "<" : ">");
        if (equal) {
            sb.append("=");
        }
        sb.append(value);
        return sb.toString();
    }

    @Override
    public Predicate negate() {
        return new GreaterLessPredicate(attributeName, value, !equal, !less);
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.GREATERLESS_PREDICATE;
    }

    @SuppressWarnings({"checkstyle:npathcomplexity"})
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!super.equals(o)) {
            return false;
        }
        if (!(o instanceof GreaterLessPredicate)) {
            return false;
        }

        GreaterLessPredicate that = (GreaterLessPredicate) o;
        if (!that.canEqual(this)) {
            return false;
        }

        if (equal != that.equal) {
            return false;
        }
        if (less != that.less) {
            return false;
        }
        return value != null ? value.equals(that.value) : that.value == null;
    }

    @Override
    public boolean canEqual(Object other) {
        return (other instanceof GreaterLessPredicate);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (value != null ? value.hashCode() : 0);
        result = 31 * result + (equal ? 1 : 0);
        result = 31 * result + (less ? 1 : 0);
        return result;
    }

    @Override
    public String getAttribute() {
        return attributeName;
    }

    @Override
    public Comparable getFrom() {
        return less ? null : value;
    }

    @Override
    public boolean isFromInclusive() {
        return !less && equal;
    }

    @Override
    public Comparable getTo() {
        return less ? value : null;
    }

    @Override
    public boolean isToInclusive() {
        return less && equal;
    }

}
