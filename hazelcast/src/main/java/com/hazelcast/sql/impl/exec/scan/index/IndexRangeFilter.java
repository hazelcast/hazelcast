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

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.AbstractIndex;
import com.hazelcast.query.impl.Comparison;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;

/**
 * Filter the is used for range requests. Could have either lower bound, upper bound or both.
 */
@SuppressWarnings("rawtypes")
public class IndexRangeFilter implements IndexFilter, IdentifiedDataSerializable {
    /** Lower bound, null if no bound. */
    private IndexFilterValue from;

    /** Lower bound inclusiveness. */
    private boolean fromInclusive;

    /** Upper bound, null if no bound. */
    private IndexFilterValue to;

    /** Upper bound inclusiveness. */
    private boolean toInclusive;

    public IndexRangeFilter() {
        // No-op.
    }

    public IndexRangeFilter(IndexFilterValue from, boolean fromInclusive, IndexFilterValue to, boolean toInclusive) {
        this.from = from;
        this.fromInclusive = fromInclusive;
        this.to = to;
        this.toInclusive = toInclusive;
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    @Override
    public Iterator<QueryableEntry> getEntries(InternalIndex index, boolean descending, ExpressionEvalContext evalContext) {
        if (from != null) {
            if (to != null) {
                // Lower and upper bounds
                Comparable fromValue = from.getValue(evalContext);

                if (isNull(fromValue)) {
                    return Collections.emptyIterator();
                }

                Comparable toValue = to.getValue(evalContext);

                if (isNull(toValue)) {
                    return Collections.emptyIterator();
                }

                return index.getSqlRecordIterator(fromValue, fromInclusive, toValue, toInclusive, descending);
            } else {
                // Lower bound only
                Comparable fromValue = from.getValue(evalContext);
                Comparison fromComparison = fromInclusive ? Comparison.GREATER_OR_EQUAL : Comparison.GREATER;

                if (isNull(fromValue)) {
                    return Collections.emptyIterator();
                }

                return index.getSqlRecordIterator(fromComparison, fromValue, descending);
            }
        } else {
            assert to != null;

            // Upper bound only
            Comparable toValue = to.getValue(evalContext);
            Comparison toComparison = toInclusive ? Comparison.LESS_OR_EQUAL : Comparison.LESS;

            if (isNull(toValue)) {
                return Collections.emptyIterator();
            }

            return index.getSqlRecordIterator(toComparison, toValue, descending);
        }
    }

    /**
     * Check if the value is null, and hence its usage with any comparison operator cannot produce any row.
     *
     * @param value value
     * @return {@code} true if the value is null
     */
    private static boolean isNull(Object value) {
        // AbstractIndex.NULL is only returned for the "IS NULL" filter.
        // For composite index the AbstractIndex.NULL is wrapped into a composite object.
        // For non-composite index, we never produce the range filter for "IS NULL".
        // Hence, AbstractIndex.NULL is not possible here.
        assert value != AbstractIndex.NULL;

        return value == null;
    }

    @Override
    public Comparable getComparable(ExpressionEvalContext evalContext) {
        return from != null ? from.getValue(evalContext) : to.getValue(evalContext);
    }

    public IndexFilterValue getFrom() {
        return from;
    }

    public boolean isFromInclusive() {
        return fromInclusive;
    }

    public IndexFilterValue getTo() {
        return to;
    }

    public boolean isToInclusive() {
        return toInclusive;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.INDEX_FILTER_RANGE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(from);
        out.writeBoolean(fromInclusive);
        out.writeObject(to);
        out.writeBoolean(toInclusive);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        from = in.readObject();
        fromInclusive = in.readBoolean();
        to = in.readObject();
        toInclusive = in.readBoolean();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexRangeFilter that = (IndexRangeFilter) o;

        return Objects.equals(from, that.from) && fromInclusive == that.fromInclusive
            && Objects.equals(to, that.to) && toInclusive == that.toInclusive;
    }

    @Override
    public int hashCode() {
        int result = from != null ? from.hashCode() : 0;

        result = 31 * result + (fromInclusive ? 1 : 0);
        result = 31 * result + (to != null ? to.hashCode() : 0);
        result = 31 * result + (toInclusive ? 1 : 0);

        return result;
    }

    @Override
    public String toString() {
        return "IndexRangeFilter {from=" + from + ", fromInclusive=" + fromInclusive
            + ", to=" + to + ", toInclusive=" + toInclusive + '}';
    }
}
