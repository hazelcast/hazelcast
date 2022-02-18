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

import java.io.IOException;
import java.util.Set;

/**
 * Between Predicate
 */
@BinaryInterface
public class BetweenPredicate extends AbstractIndexAwarePredicate implements VisitablePredicate, RangePredicate {

    private static final long serialVersionUID = 1L;

    Comparable to;
    Comparable from;

    public BetweenPredicate() {
    }

    public BetweenPredicate(String attribute, Comparable from, Comparable to) {
        super(attribute);
        if (from == null || to == null) {
            throw new NullPointerException("Arguments can't be null");
        }
        this.from = from;
        this.to = to;
    }

    @Override
    protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
        if (attributeValue == null) {
            return false;
        }
        Comparable fromConvertedValue = convert(attributeValue, from);
        Comparable toConvertedValue = convert(attributeValue, to);
        if (fromConvertedValue == null || toConvertedValue == null) {
            return false;
        }
        attributeValue = (Comparable) convertEnumValue(attributeValue);
        return Comparables.compare(attributeValue, fromConvertedValue) >= 0
                && Comparables.compare(attributeValue, toConvertedValue) <= 0;
    }

    @Override
    public Set<QueryableEntry> filter(QueryContext queryContext) {
        Index index = matchIndex(queryContext, QueryContext.IndexMatchHint.PREFER_ORDERED);
        if (index == null) {
            return null;
        }
        return index.getRecords(from, true, to, true);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(to);
        out.writeObject(from);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        to = in.readObject();
        from = in.readObject();
    }

    @Override
    public String toString() {
        return attributeName + " BETWEEN " + from + " AND " + to;
    }

    @Override
    public int getClassId() {
        return PredicateDataSerializerHook.BETWEEN_PREDICATE;
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
        if (!(o instanceof BetweenPredicate)) {
            return false;
        }

        BetweenPredicate that = (BetweenPredicate) o;
        if (!that.canEqual(this)) {
            return false;
        }

        if (to != null ? !to.equals(that.to) : that.to != null) {
            return false;
        }
        return from != null ? from.equals(that.from) : that.from == null;
    }

    @Override
    public boolean canEqual(Object other) {
        return (other instanceof BetweenPredicate);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (to != null ? to.hashCode() : 0);
        result = 31 * result + (from != null ? from.hashCode() : 0);
        return result;
    }

    @Override
    public Predicate accept(Visitor visitor, Indexes indexes) {
        return visitor.visit(this, indexes);
    }

    @Override
    public String getAttribute() {
        return attributeName;
    }

    @Override
    public Comparable getFrom() {
        return from;
    }

    @Override
    public boolean isFromInclusive() {
        return true;
    }

    @Override
    public Comparable getTo() {
        return to;
    }

    @Override
    public boolean isToInclusive() {
        return true;
    }

}
