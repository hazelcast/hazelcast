/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.function.Supplier;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Lazy Multiple result set for Predicates.
 */
public class LazyFastMultiResultSet extends AbstractSet<QueryableEntry> implements LazyMultiResultSet {

    private final List<Supplier<Map<Data, QueryableEntry>>> resultSuppliers
            = new ArrayList<Supplier<Map<Data, QueryableEntry>>>();
    private final List<Map<Data, QueryableEntry>> resultSets = new ArrayList<Map<Data, QueryableEntry>>();
    private boolean initialized;
    private int size;
    private int estimatedSize;

    @Override
    public void addResultSetSupplier(Supplier<Map<Data, QueryableEntry>> resultSetSupplier, int resultSetSize) {
        resultSuppliers.add(resultSetSupplier);
        estimatedSize += resultSetSize;
    }

    @Override
    public void init() {
        if (!initialized) {
            int recordSize = 0;
            for (Supplier<Map<Data, QueryableEntry>> orgResult : resultSuppliers) {
                Map<Data, QueryableEntry> resultSet = orgResult.get();
                resultSets.add(resultSet);
                recordSize += resultSet.size();
            }
            size = recordSize;
            initialized = true;
        }
    }

    @Override
    public boolean contains(Object o) {
        init();
        QueryableEntry entry = (QueryableEntry) o;
        for (Map<Data, QueryableEntry> resultSet : resultSets) {
            if (resultSet.containsKey(entry.getKeyData())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<QueryableEntry> iterator() {
        init();
        return new It();
    }

    class It implements Iterator<QueryableEntry> {
        int currentIndex;
        Iterator<QueryableEntry> currentIterator;

        @Override
        public boolean hasNext() {
            if (resultSets.size() == 0) {
                return false;
            }
            if (currentIterator != null && currentIterator.hasNext()) {
                return true;
            }
            while (currentIndex < resultSets.size()) {
                currentIterator = resultSets.get(currentIndex++).values().iterator();
                if (currentIterator.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public QueryableEntry next() {
            if (resultSets.size() == 0) {
                return null;
            }
            if (currentIterator != null && currentIterator.hasNext()) {
                return currentIterator.next();
            }
            while (currentIndex < resultSets.size()) {
                currentIterator = resultSets.get(currentIndex++).values().iterator();
                if (currentIterator.hasNext()) {
                    return currentIterator.next();
                }
            }
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean add(QueryableEntry obj) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        if (initialized) {
            return size;
        } else {
            return estimatedSize;
        }
    }

}
