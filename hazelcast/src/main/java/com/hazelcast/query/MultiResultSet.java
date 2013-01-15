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

package com.hazelcast.query;

import com.hazelcast.core.MapEntry;
import com.hazelcast.impl.Record;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

public class MultiResultSet extends AbstractSet<MapEntry> {
    private final List<Collection<Record>> resultSets = new ArrayList<Collection<Record>>();
    private final Set<Long> indexValues = new HashSet<Long>();
    private final ConcurrentMap<Long, Long> recordValues;

    MultiResultSet(ConcurrentMap<Long, Long> recordValues) {
        this.recordValues = recordValues;
    }

    public void addResultSet(Long indexValue, Collection<Record> resultSet) {
        resultSets.add(resultSet);
        indexValues.add(indexValue);
    }

    @Override
    public Iterator<MapEntry> iterator() {
        return new It();
    }

    @Override
    public boolean contains(Object mapEntry) {
        Long indexValue = recordValues.get(((Record) mapEntry).getId());
        return indexValue != null && indexValues.contains(indexValue);
    }

    class It implements Iterator<MapEntry> {
        int currentIndex = 0;
        Iterator<Record> currentIterator;

        public boolean hasNext() {
            if (resultSets.size() == 0) return false;
            if (currentIterator != null && currentIterator.hasNext()) {
                return true;
            }
            while (currentIndex < resultSets.size()) {
                currentIterator = resultSets.get(currentIndex++).iterator();
                if (currentIterator.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        public MapEntry next() {
            if (resultSets.size() == 0) return null;
            return currentIterator.next();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean add(MapEntry obj) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        int size = 0;
        for (Collection<Record> resultSet : resultSets) {
            size += resultSet.size();
        }
        return size;
    }
}