/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.impl.SerializationUtil;
import com.hazelcast.internal.util.AbstractCompositeIterator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Filter that is composed of several equality filters.
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
public class IndexInFilter implements IndexFilter, IdentifiedDataSerializable {

    private List<IndexFilter> filters;

    public IndexInFilter() {
        // No-op.
    }

    public IndexInFilter(IndexFilter... filters) {
        assert filters != null;

        this.filters = Arrays.asList(filters);
    }

    public IndexInFilter(List<IndexFilter> filters) {
        this.filters = filters;
    }

    @Override
    public Iterator<QueryableEntry> getEntries(InternalIndex index, ExpressionEvalContext evalContext) {
        Map<Comparable, IndexFilter> canonicalFilters = new HashMap<>();

        for (IndexFilter filter : filters) {
            Comparable filterComparable = filter.getComparable(evalContext);

            if (filterComparable == null) {
                // One of disjunctive components produced NULL, ignore it.
                // E.g. {WHERE a=NULL OR a=2} => {WHERE a=2}
                continue;
            }

            // Avoid duplicates. E.g. {WHERE a=? OR a=?} for parameters {1, 1}.
            filterComparable = index.canonicalizeQueryArgumentScalar(filterComparable);

            canonicalFilters.put(filterComparable, filter);
        }

        if (canonicalFilters.isEmpty()) {
            // There are no non-NULL values, the result set is empty.
            return Collections.emptyIterator();
        }

        return new LazyIterator(index, evalContext, canonicalFilters.values());
    }

    @Override
    public Comparable getComparable(ExpressionEvalContext evalContext) {
        throw new UnsupportedOperationException("Should not be called");
    }

    public List<IndexFilter> getFilters() {
        return filters;
    }

    @Override
    public int getFactoryId() {
        return SqlDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SqlDataSerializerHook.INDEX_FILTER_IN;
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

        IndexInFilter that = (IndexInFilter) o;

        return filters.equals(that.filters);
    }

    @Override
    public int hashCode() {
        return filters.hashCode();
    }

    @Override
    public String toString() {
        return "IndexInFilter {filters=" + filters + '}';
    }

    private static final class LazyIterator extends AbstractCompositeIterator<QueryableEntry> {

        private final InternalIndex index;
        private final ExpressionEvalContext evalContext;
        private final Iterator<IndexFilter> filterIterator;

        private LazyIterator(InternalIndex index, ExpressionEvalContext evalContext, Collection<IndexFilter> filters) {
            this.index = index;
            this.evalContext = evalContext;

            filterIterator = filters.iterator();
        }

        @Override
        protected Iterator<QueryableEntry> nextIterator() {
            while (filterIterator.hasNext()) {
                IndexFilter filter = filterIterator.next();

                Iterator<QueryableEntry> iterator = filter.getEntries(index, evalContext);

                if (iterator.hasNext()) {
                    return iterator;
                }
            }

            return null;
        }
    }
}
