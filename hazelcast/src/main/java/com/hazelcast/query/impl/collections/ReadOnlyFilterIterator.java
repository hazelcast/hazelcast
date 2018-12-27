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

package com.hazelcast.query.impl.collections;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ReadOnlyFilterIterator implements Iterator<QueryableEntry> {

    private final Iterator<QueryableEntry> delegate;
    private final List<Predicate> filters;

    private QueryableEntry currentEntry;

    ReadOnlyFilterIterator(final Iterator<QueryableEntry> iterator, final List<Predicate> filters) {
        this.delegate = iterator;
        this.filters = filters;
    }

    @Override
    public boolean hasNext() {
        if (currentEntry != null) {
            return true;
        }

        while (delegate.hasNext()) {
            QueryableEntry entry = delegate.next();
            if (checkFilters(entry)) {
                currentEntry = entry;
                return true;
            }
        }

        return false;
    }

    private boolean checkFilters(QueryableEntry entry) {
        for (Predicate predicate : filters) {
            if (!predicate.apply(entry)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public QueryableEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        QueryableEntry result = currentEntry;
        currentEntry = null;
        return result;
    }
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
