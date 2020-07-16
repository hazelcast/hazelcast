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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.query.impl.Comparison;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.exec.scan.KeyValueIterator;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Iterator for index-based partitioned map access.
 */
@SuppressWarnings("rawtypes")
// TODO: Proper conversions of value
public class MapIndexScanExecIterator implements KeyValueIterator {

    private final MapContainer map;
    private final String indexName;
    private final List<IndexFilter> indexFilters;
    private final ExpressionEvalContext evalContext;

    private final Iterator<QueryableEntry> iterator;

    private Data currentKey;
    private Object currentValue;
    private Data nextKey;
    private Object nextValue;

    public MapIndexScanExecIterator(
        MapContainer map,
        String indexName,
        List<IndexFilter> indexFilters,
        ExpressionEvalContext evalContext
    ) {
        this.map = map;
        this.indexName = indexName;
        this.indexFilters = indexFilters;
        this.evalContext = evalContext;

        iterator = getIndexEntries();

        advance0();
    }

    @Override
    public boolean tryAdvance() {
        if (!done()) {
            currentKey = nextKey;
            currentValue = nextValue;

            advance0();

            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean done() {
        return nextKey == null;
    }

    @Override
    public Object getKey() {
        return currentKey;
    }

    @Override
    public Object getValue() {
        return currentValue;
    }

    private void advance0() {
        if (iterator.hasNext()) {
            QueryableEntry<?, ?> entry = iterator.next();

            nextKey = entry.getKeyData();
            nextValue = entry.getValue();
        } else {
            nextKey = null;
            nextValue = null;
        }
    }

    private Iterator<QueryableEntry> getIndexEntries() {
        MapContainer mapContainer = map.getMapServiceContext().getMapContainer(map.getName());
        InternalIndex index = mapContainer.getIndexes().getIndex(indexName);

        // TODO: Check index existence earlier?
        if (index == null) {
            // TODO: Proper error
            throw QueryException.error("Index doesn't exist: " + indexName);
        }

        IndexFilter lastFilter = lastFilter();

        if (lastFilter == null) {
            return processScan(index);
        }

        switch (lastFilter.getType()) {
            case EQUALS:
                return processEquals(index);

            case IN:
                return processIn(index);

            default:
                assert lastFilter.getType() == IndexFilterType.RANGE;

                return processRange(index);
        }
    }

    private Iterator<QueryableEntry> processScan(InternalIndex index) {
        assert indexFilters.isEmpty();

        return index.getRecordIterator();
    }

    private Iterator<QueryableEntry> processEquals(InternalIndex index) {
        if (indexFilters.size() > 1) {
            // TODO
            throw new UnsupportedOperationException("Implement me!");
        }

        IndexFilter lastFilter = lastFilter();

        assert lastFilter != null;

        Comparable value = (Comparable) lastFilter.getFrom().eval(null, evalContext);

        if (value == null) {
            return Collections.emptyIterator();
        } else {
            return index.getRecordIterator(value);
        }
    }

    private Iterator<QueryableEntry> processIn(InternalIndex index) {
        if (indexFilters.size() > 1) {
            // TODO
            throw new UnsupportedOperationException("Implement me!");
        }

        IndexFilter lastFilter = lastFilter();

        assert lastFilter != null;

        Comparable[] values = (Comparable[]) lastFilter.getFrom().eval(null, evalContext);

        return index.getRecordIterator(values);
    }

    private Iterator<QueryableEntry> processRange(InternalIndex index) {
        if (indexFilters.size() > 1) {
            // TODO
            throw new UnsupportedOperationException("Implement me!");
        }

        IndexFilter lastFilter = lastFilter();

        assert lastFilter != null;

        Comparable from = lastFilter.getFrom() != null ? (Comparable) lastFilter.getFrom().eval(null, evalContext) : null;
        Comparison fromComparison = lastFilter.isFromInclusive() ? Comparison.GREATER_OR_EQUAL : Comparison.GREATER;

        Comparable to = lastFilter.getTo() != null ? (Comparable) lastFilter.getTo().eval(null, evalContext) : null;
        Comparison toComparison = lastFilter.isToInclusive() ? Comparison.LESS_OR_EQUAL : Comparison.LESS;

        if (from != null && to == null) {
            // Left bound only
            return index.getRecordIterator(fromComparison, from);
        } else if (from == null && to != null) {
            // Right bound only
            return index.getRecordIterator(toComparison, to);
        } else {
            // Both left and right bounds
            return index.getRecordIterator(from, lastFilter.isFromInclusive(), to, lastFilter.isToInclusive());
        }
    }

    private IndexFilter lastFilter() {
        return indexFilters.isEmpty() ? null : indexFilters.get(indexFilters.size() - 1);
    }
}
