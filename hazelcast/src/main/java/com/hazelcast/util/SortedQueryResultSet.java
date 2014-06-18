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

package com.hazelcast.util;

import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

/**
 * Collection class for results of query operations
 */
public class SortedQueryResultSet extends AbstractSet<Map.Entry> {

    private final TreeSet<Map.Entry> entries;
    private final IterationType iterationType;
    private final int pageSize;

    public SortedQueryResultSet(final Comparator comparator, IterationType iterationType, int pageSize) {
        this.entries = new TreeSet<Map.Entry>(SortingUtil.newComparator(comparator, iterationType));
        this.iterationType = iterationType;
        this.pageSize = pageSize;
    }

    @Override
    public boolean add(Map.Entry entry) {
        if (entries.add(entry)) {
            if (entries.size() > pageSize) {
                entries.pollLast();
            }
            return true;
        }
        return false;
    }

    @Override
    public Iterator iterator() {
        return new SortedIterator();
    }

    /**
     *
     * @return Map.Entry last entry in set
     */
    public Map.Entry last() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.last();
    }

    //CHECKSTYLE:OFF
    private class SortedIterator implements Iterator {
    //CHECKSTYLE:ON

        final Iterator<Map.Entry> iter = entries.iterator();

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Object next() {
            Map.Entry entry = iter.next();
            if (iterationType == IterationType.VALUE) {
                return entry.getValue();
            } else if (iterationType == IterationType.KEY) {
                return entry.getKey();
            } else {
                return entry;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public int size() {
        return entries.size();
    }

}
