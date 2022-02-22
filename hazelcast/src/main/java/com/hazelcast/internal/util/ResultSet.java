/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 *
 * A result set of {@link Map.Entry} objects that can be iterated according to {@link IterationType}
 *
 */
public class ResultSet extends AbstractSet<Map.Entry> {

    private final List<Map.Entry> entries;
    private final IterationType iterationType;

    public ResultSet(List<? extends Map.Entry> entries, IterationType iterationType) {
        this.entries = (List<Map.Entry>) entries;
        this.iterationType = iterationType;
    }

    public ResultSet() {
        this(null, null);
    }

    @Override
    public Iterator iterator() {
        if (entries == null) {
            return Collections.EMPTY_LIST.iterator();
        }
        return new ResultIterator();
    }

    @Override
    public int size() {
        if (entries == null) {
            return 0;
        }
        return entries.size();
    }

    private class ResultIterator implements Iterator {

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

    @Override
    public boolean addAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate filter) {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(Collection<?> coll) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
