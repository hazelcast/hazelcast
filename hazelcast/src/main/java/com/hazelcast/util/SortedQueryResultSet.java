/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Collection class for results of query operations
 */
public class SortedQueryResultSet extends AbstractSet<Map.Entry> {

    private final List<Map.Entry> entries;
    private final IterationType iterationType;

    public SortedQueryResultSet() {
        this(null, null);
    }

    public SortedQueryResultSet(List<Map.Entry> entries, IterationType iterationType) {
        this.entries = entries;
        this.iterationType = iterationType;
    }

    @Override
    public Iterator iterator() {
        if (entries == null) {
            return new EmptyIterator();
        }
        return new SortedIterator();
    }

    @Override
    public int size() {
        if (entries == null) {
            return 0;
        }
        return entries.size();
    }

    private class SortedIterator implements Iterator {

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

    private static class EmptyIterator implements Iterator {

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Object next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
