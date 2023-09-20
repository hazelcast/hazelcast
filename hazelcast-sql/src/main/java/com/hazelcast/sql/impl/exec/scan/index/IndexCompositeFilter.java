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

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.jet.sql.impl.JetSqlSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Filter that is composed of several equality and range filters.
 * <p>
 * It is guaranteed that every children filter will return a result set that is not intersecting with the result set of
 * any other children filter.
 * <p>
 * Actual children filters could be either {@link IndexEqualsFilter} or {@link IndexRangeFilter} depending on the index
 * configuration and the filter.
 * <p>
 * Consider the expression {@code WHERE a=1 AND (b=2 OR b=3)}.
 * <ul>
 *     <li>{@code SORTED{a,b}} - there will be EQUALS(1,2) and EQUALS(1,3) filters</li>
 *     <li>{@code SORTED{a,b,c}} - there will be RANGE(1,2,INF) and RANGE(1,3,INF) filters</li>
 * </ul>>
 */
@SuppressWarnings("rawtypes")
public class IndexCompositeFilter implements IndexFilter, IdentifiedDataSerializable {

    private List<IndexFilter> filters;

    public IndexCompositeFilter() {
        // No-op.
    }

    public IndexCompositeFilter(IndexFilter... filters) {
        assert filters != null;

        this.filters = Arrays.asList(filters);
    }

    public IndexCompositeFilter(List<IndexFilter> filters) {
        this.filters = filters;
    }

    @Override
    public Comparable getComparable(ExpressionEvalContext evalContext) {
        throw new UnsupportedOperationException("Should not be called");
    }

    @Override
    public boolean isCooperative() {
        for (IndexFilter f : filters) {
            if (!f.isCooperative()) {
                return false;
            }
        }
        return true;
    }

    public List<IndexFilter> getFilters() {
        return filters;
    }

    @Override
    public int getFactoryId() {
        return JetSqlSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return JetSqlSerializerHook.INDEX_FILTER_IN;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        SerializationUtil.writeList(filters, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        filters = SerializationUtil.readList(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexCompositeFilter that = (IndexCompositeFilter) o;

        return filters.equals(that.filters);
    }

    @Override
    public int hashCode() {
        return filters.hashCode();
    }

    @Override
    public String toString() {
        return "IndexCompositeFilter {filters=" + filters + '}';
    }
}
