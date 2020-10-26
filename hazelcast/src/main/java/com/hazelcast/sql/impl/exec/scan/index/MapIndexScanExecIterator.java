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
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.exec.scan.KeyValueIterator;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.schema.map.MapTableUtils;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Iterator;
import java.util.List;

/**
 * Iterator for index-based partitioned map access.
 */
@SuppressWarnings("rawtypes")
public class MapIndexScanExecIterator implements KeyValueIterator {

    private final Iterator<QueryableEntry> iterator;

    private Data currentKey;
    private Object currentValue;
    private Data nextKey;
    private Object nextValue;

    public MapIndexScanExecIterator(
        String mapName,
        InternalIndex index,
        int expectedComponentCount,
        IndexFilter indexFilter,
        List<QueryDataType> expectedConverterTypes,
        ExpressionEvalContext evalContext
    ) {
        iterator = getIndexEntries(
            mapName,
            index,
            indexFilter,
            evalContext,
            expectedComponentCount,
            expectedConverterTypes
        );

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

    private Iterator<QueryableEntry> getIndexEntries(
        String mapName,
        InternalIndex index,
        IndexFilter indexFilter,
        ExpressionEvalContext evalContext,
        int expectedComponentCount,
        List<QueryDataType> expectedConverterTypes
    ) {
        if (indexFilter == null) {
            // No filter => this is a full scan (e.g. for HD)
            return index.getSqlRecordIterator();
        }

        int actualComponentCount = index.getComponents().length;

        if (actualComponentCount != expectedComponentCount) {
            throw QueryException.error(SqlErrorCode.INDEX_INVALID, "Cannot use the index \"" + index.getName()
                + "\" of the IMap \"" + mapName + "\" because it has " + actualComponentCount + " component(s), but "
                + expectedComponentCount + " expected").markInvalidate();
        }

        // Validate component types
        List<QueryDataType> currentConverterTypes = MapTableUtils.indexConverterToSqlTypes(index.getConverter());

        validateConverterTypes(index, mapName, expectedConverterTypes, currentConverterTypes);

        // Query the index
        return indexFilter.getEntries(index, evalContext);
    }

    private void validateConverterTypes(
        InternalIndex index,
        String mapName,
        List<QueryDataType> expectedConverterTypes,
        List<QueryDataType> actualConverterTypes
    ) {
        for (int i = 0; i < Math.min(expectedConverterTypes.size(), actualConverterTypes.size()); i++) {
            QueryDataType expected = expectedConverterTypes.get(i);
            QueryDataType actual = actualConverterTypes.get(i);

            if (!expected.equals(actual)) {
                String component = index.getComponents()[i];

                throw QueryException.error(SqlErrorCode.INDEX_INVALID, "Cannot use the index \"" + index.getName()
                    + "\" of the IMap \"" + mapName + "\" because it has component \"" + component + "\" of type "
                    + actual.getTypeFamily() + ", but " + expected.getTypeFamily() + " was expected").markInvalidate();
            }
        }

        if (expectedConverterTypes.size() > actualConverterTypes.size()) {
            QueryDataType expected = expectedConverterTypes.get(actualConverterTypes.size());
            String component = index.getComponents()[actualConverterTypes.size()];

            throw QueryException.error(SqlErrorCode.INDEX_INVALID, "Cannot use the index \"" + index.getName()
                + "\" of the IMap \"" + mapName + "\" because it does not have suitable converter for component \""
                + component + "\" (expected " + expected.getTypeFamily() + ")").markInvalidate();
        }
    }
}
