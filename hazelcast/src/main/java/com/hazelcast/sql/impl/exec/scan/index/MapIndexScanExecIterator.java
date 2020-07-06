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
import com.hazelcast.sql.impl.exec.scan.KeyValueIterator;

import java.util.Iterator;
import java.util.Set;

/**
 * Iterator for index-based partitioned map access.
 */
@SuppressWarnings("rawtypes")
public class MapIndexScanExecIterator implements KeyValueIterator {

    private final MapContainer map;
    private final String indexName;
    private final IndexFilter indexFilter;

    private final Iterator<QueryableEntry> iterator;

    private Data currentKey;
    private Object currentValue;
    private Data nextKey;
    private Object nextValue;

    public MapIndexScanExecIterator(MapContainer map, String indexName, IndexFilter indexFilter) {
        this.map = map;
        this.indexName = indexName;
        this.indexFilter = indexFilter;

        iterator = getIndexEntries().iterator();
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

    private Set<QueryableEntry> getIndexEntries() {
        // TODO: Either obtain the index in advance, or check for its existence here.
        MapContainer mapContainer = map.getMapServiceContext().getMapContainer(map.getName());
        InternalIndex index = mapContainer.getIndexes().getIndex(indexName);

        // TODO: Unsafe conversion. Need to check whether the entry on planning stage?
        Comparable value = (Comparable) indexFilter.getValue();

        // TODO: Return an iterator here. No sets!
        Set<QueryableEntry> res;

        switch (indexFilter.getType()) {
            case GREATER_THAN:
                res = index.getRecords(Comparison.GREATER, value);

                break;

            case GREATER_THAN_OR_EQUAL:
                res = index.getRecords(Comparison.GREATER_OR_EQUAL, value);

                break;

            case LESS_THAN:
                res = index.getRecords(Comparison.LESS, value);

                break;

            case LESS_THAN_OR_EQUAL:
                res = index.getRecords(Comparison.LESS_OR_EQUAL, value);

                break;

            default:
                assert indexFilter.getType() == IndexFilterType.EQUALS;

                res = index.getRecords(value);
        }

        return res;
    }
}
