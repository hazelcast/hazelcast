/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.io.IOException;
import java.util.Objects;

/**
 * Filter the is used for range requests. Could have either lower bound, upper bound or both.
 */
@SuppressWarnings("rawtypes")
public class IndexRangeFilter implements IndexFilter, IdentifiedDataSerializable {
    /**
     * Lower bound, null if no bound.
     */
    private IndexFilterValue from;

    /**
     * Lower bound inclusiveness.
     */
    private boolean fromInclusive;

    /**
     * Upper bound, null if no bound.
     */
    private IndexFilterValue to;

    /**
     * Upper bound inclusiveness.
     */
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

    @Override
    public Comparable getComparable(ExpressionEvalContext evalContext) {
        return from != null ? from.getValue(evalContext) : to.getValue(evalContext);
    }

    @Override
    public boolean isCooperative() {
        boolean ret = true;
        if (from != null) {
            ret = from.isCooperative();
        }
        if (to != null) {
            ret &= to.isCooperative();
        }
        return ret;
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
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.INDEX_FILTER_RANGE;
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
