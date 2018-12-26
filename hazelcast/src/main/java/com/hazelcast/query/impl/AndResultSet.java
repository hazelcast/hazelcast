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

package com.hazelcast.query.impl;

import com.hazelcast.query.Predicate;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * And Result set for Predicates.
 */
public class AndResultSet extends AbstractSet<QueryableEntry> implements LazyResultSet {

    private static final int SIZE_UNINITIALIZED = -1;

    private final Set<QueryableEntry> setSmallest;
    private final List<Predicate> lsNoIndexPredicates;
    private int size;

    public AndResultSet(Set<QueryableEntry> setSmallest, List<Predicate> lsNoIndexPredicates) {
        this.setSmallest = isNotNull(setSmallest, "setSmallest");
        this.lsNoIndexPredicates = lsNoIndexPredicates;
        this.size = SIZE_UNINITIALIZED;
    }

    @Override
    public boolean contains(Object o) {
        if (!setSmallest.contains(o)) {
            return false;
        }

        if (lsNoIndexPredicates != null) {
            for (Predicate noIndexPredicate : lsNoIndexPredicates) {
                if (!noIndexPredicate.apply((Map.Entry) o)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Iterator<QueryableEntry> iterator() {
        return new It();
    }

    class It implements Iterator<QueryableEntry> {

        QueryableEntry currentEntry;
        final Iterator<QueryableEntry> it = setSmallest.iterator();

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
            if (lsNoIndexPredicates == null) {
                return true;
            }

            for (Predicate noIndexPredicate : lsNoIndexPredicates) {
                if (!noIndexPredicate.apply(currentEntry)) {
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

    @Override
    public int size() {
        return estimatedSize();
    }

    /**
     * @return returns estimated size without calculating the full
     * result set in full-result scan.
     */
    @Override
    public int estimatedSize() {
        if (size == SIZE_UNINITIALIZED) {
            return setSmallest.size();
        } else {
            return size;
        }
    }

    @Override
    public void init() {
        if(size == SIZE_UNINITIALIZED) {
            if (setSmallest instanceof LazyResultSet) {
                ((LazyResultSet) setSmallest).init();
            }
            //Apply predicates & calculate real size
            int calculatedSize = 0;
            for (Iterator<QueryableEntry> it = iterator(); it.hasNext(); it.next()) {
                calculatedSize++;
            }
            size = calculatedSize;
        }
    }

}
