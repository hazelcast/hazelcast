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

package com.hazelcast.query.impl.resultset;

import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;

import java.io.IOException;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * Filtered Result set for Predicates.
 */
public class FilteredResultSet extends AbstractSet<QueryableEntry> {
    private final Set<QueryableEntry> entries;
    private final Predicate predicate;

    public FilteredResultSet(Set<QueryableEntry> entries, Predicate predicate) {
        this.entries = isNotNull(entries, "entries");
        this.predicate = isNotNull(predicate, "predicate");
    }

    public byte[] toByteArray(ObjectDataOutput out) throws IOException {
        for (QueryableEntry entry : entries) {
            if (!predicate.apply(entry)) {
                break;

            }
            entry.getKeyData().writeData(out);
        }
        return out.toByteArray();
    }

    @Override
    public boolean contains(Object o) {
        if (!entries.contains(o)) {
            return false;
        }

        if (!predicate.apply((Map.Entry) o)) {
            return false;

        }

        return true;
    }

    @Override
    public Iterator<QueryableEntry> iterator() {
        return new It();
    }

    class It implements Iterator<QueryableEntry> {

        QueryableEntry currentEntry;
        final Iterator<QueryableEntry> it = entries.iterator();

        @Override
        public boolean hasNext() {
            if (currentEntry != null) {
                return true;
            }

            while (it.hasNext()) {
                QueryableEntry entry = it.next();

                if (checkNoIndexPredicates(entry)) {
                    currentEntry = entry;
                    return true;
                }
            }

            return false;
        }

        private boolean checkNoIndexPredicates(QueryableEntry currentEntry) {

            if (!predicate.apply(currentEntry)) {
                return false;
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

    @Override
    public int size() {
        return entries.size();
    }
}
