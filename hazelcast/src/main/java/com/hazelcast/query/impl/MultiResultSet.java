/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 *  Multiple result set for Predicates.
 */
public class MultiResultSet extends AbstractSet<QueryableEntry> {

    private Set<Object> index;
    private final List<ConcurrentMap<Data, QueryableEntry>> resultSets
            = new ArrayList<ConcurrentMap<Data, QueryableEntry>>();

    public MultiResultSet() {
    }

    public void addResultSet(ConcurrentMap<Data, QueryableEntry> resultSet) {
        resultSets.add(resultSet);
    }

    @Override
    public boolean contains(Object o) {
        QueryableEntry entry = (QueryableEntry) o;
        if (index != null) {
            return checkFromIndex(entry);
        } else {
            //todo: what is the point of this condition? Is it some kind of optimization?
            if (resultSets.size() > 3) {
                index = new HashSet<Object>();
                for (ConcurrentMap<Data, QueryableEntry> result : resultSets) {
                    for (QueryableEntry queryableEntry : result.values()) {
                        index.add(queryableEntry.getIndexKey());
                    }
                }
                return checkFromIndex(entry);
            } else {
                for (ConcurrentMap<Data, QueryableEntry> resultSet : resultSets) {
                    if (resultSet.containsKey(entry.getIndexKey())) {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    private boolean checkFromIndex(QueryableEntry entry) {
        return index.contains(entry.getIndexKey());
    }

    @Override
    public Iterator<QueryableEntry> iterator() {
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
            return currentIterator.next();
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
        int size = 0;
        for (ConcurrentMap<Data, QueryableEntry> resultSet : resultSets) {
            size += resultSet.size();
        }
        return size;
    }
}
