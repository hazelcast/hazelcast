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

import java.util.*;

/**
 * @ali 08/12/13
 */
public class SortedQueryResultSet extends AbstractSet<Map.Entry> {

    private final TreeSet<Map.Entry> entries;
    private final IterationType iterationType;
    private final int pageSize;

    public SortedQueryResultSet(Comparator comparator, IterationType iterationType, int pageSize) {
        this.entries = new TreeSet<Map.Entry>(new WrapperComparator(comparator));
        this.iterationType = iterationType;
        this.pageSize = pageSize;
    }

    public boolean add(Map.Entry entry) {
        if (entries.add(entry)) {
            if (entries.size() > pageSize) {
                entries.pollLast();
            }
            return true;
        }
        return false;
    }

    public Iterator iterator() {
        return new SortedIterator();
    }

    public Object last() {
        return entries.last().getValue();
    }

    private class SortedIterator implements Iterator {

        final Iterator<Map.Entry> iter = entries.iterator();

        public boolean hasNext() {
            return iter.hasNext();
        }

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

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public int size() {
        return entries.size();
    }

    private class WrapperComparator implements Comparator<Map.Entry> {

        private final Comparator inner;

        private WrapperComparator(Comparator inner) {
            this.inner = inner;
        }

        public int compare(Map.Entry o1, Map.Entry o2) {
            Object value1 = o1.getValue();
            Object value2 = o2.getValue();
            if (inner != null) {
                return inner.compare(value1, value2);
            } else if (value1 instanceof Comparable && value2 instanceof Comparable) {
                return ((Comparable) value1).compareTo(value2);
            }
            return value1.hashCode() - value2.hashCode();
        }
    }
}
