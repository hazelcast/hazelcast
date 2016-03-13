/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.util.IterationType;

import java.util.AbstractSet;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
            return Collections.EMPTY_LIST.iterator();
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

        private final Iterator<Map.Entry> iterator = entries.iterator();

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Object next() {
            Map.Entry entry = iterator.next();
            switch (iterationType) {
                case KEY:
                    return entry.getKey();
                case VALUE:
                    return entry.getValue();
                case ENTRY:
                    return entry;
                default:
                    throw new IllegalStateException("Unrecognized iterationType:" + iterationType);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
