/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Map;
import java.util.Set;

/**
 * Multiple result set for Predicates.
 */
public class FastMultiResultSet extends AbstractSet<QueryableEntryImpl> implements MultiResultSet {

    private Set<Object> index;
    private final List<Map<Data, QueryableEntryImpl>> resultSets
            = new ArrayList<Map<Data, QueryableEntryImpl>>();

    public FastMultiResultSet() {
    }

    public void addResultSet(Map<Data, QueryableEntryImpl> resultSet) {
        resultSets.add(resultSet);
    }

    @Override
    public boolean contains(Object o) {
        QueryableEntryImpl entry = (QueryableEntryImpl) o;
        if (index != null) {
            return checkFromIndex(entry);
        } else {
            //todo: what is the point of this condition? Is it some kind of optimization?
            if (resultSets.size() > 3) {
                index = new HashSet<Object>();
                for (Map<Data, QueryableEntryImpl> result : resultSets) {
                    for (QueryableEntryImpl queryableEntry : result.values()) {
                        index.add(queryableEntry.getKeyData());
                    }
                }
                return checkFromIndex(entry);
            } else {
                for (Map<Data, QueryableEntryImpl> resultSet : resultSets) {
                    if (resultSet.containsKey(entry.getKeyData())) {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    private boolean checkFromIndex(QueryableEntryImpl entry) {
        return index.contains(entry.getKeyData());
    }

    @Override
    public Iterator<QueryableEntryImpl> iterator() {
        return new It();
    }

    class It implements Iterator<QueryableEntryImpl> {
        int currentIndex;
        Iterator<QueryableEntryImpl> currentIterator;

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
        public QueryableEntryImpl next() {
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
    public boolean add(QueryableEntryImpl obj) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        int size = 0;
        for (Map<Data, QueryableEntryImpl> resultSet : resultSets) {
            size += resultSet.size();
        }
        return size;
    }
}
