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

package com.hazelcast.query.impl;

import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;

import java.io.IOException;
import java.util.*;

public class AndResultSet extends AbstractSet<QueryableEntry> {
    private final Set<QueryableEntry> setSmallest;
    private final List<Set<QueryableEntry>> otherIndexedResults;
    private final List<Predicate> lsNoIndexPredicates;

    public AndResultSet(Set<QueryableEntry> setSmallest, List<Set<QueryableEntry>> otherIndexedResults, List<Predicate> lsNoIndexPredicates) {
        this.setSmallest = setSmallest;
        this.otherIndexedResults = otherIndexedResults;
        this.lsNoIndexPredicates = lsNoIndexPredicates;
    }

    public byte[] toByteArray(ObjectDataOutput out) throws IOException {
        for (QueryableEntry entry : setSmallest) {
            if (otherIndexedResults != null) {
                for (Set<QueryableEntry> otherIndexedResult : otherIndexedResults) {
                    if (!otherIndexedResult.contains(entry)) {
                        break;
                    }
                }
            }
            if (lsNoIndexPredicates != null) {
                for (Predicate noIndexPredicate : lsNoIndexPredicates) {
                    if (!noIndexPredicate.apply(entry)) {
                        break;
                    }
                }
            }
            entry.getKeyData().writeData(out);
        }
        return out.toByteArray();
    }

    @Override
    public boolean contains(Object o) {
        if (!setSmallest.contains(o)) return false;
        if (otherIndexedResults != null) {
            for (Set<QueryableEntry> otherIndexedResult : otherIndexedResults) {
                if (!otherIndexedResult.contains(o)) return false;
            }
        }
        if (lsNoIndexPredicates != null) {
            for (Predicate noIndexPredicate : lsNoIndexPredicates) {
                if (!noIndexPredicate.apply((Map.Entry) o)) return false;
            }
        }
        return true;
    }

    @Override
    public Iterator<QueryableEntry> iterator() {
        return new It();
    }

    class It implements Iterator<QueryableEntry> {

        QueryableEntry currentEntry = null;
        final Iterator<QueryableEntry> it = setSmallest.iterator();

        public boolean hasNext() {
            if (!it.hasNext()) return false;
            currentEntry = it.next();
            if (otherIndexedResults != null) {
                for (Set<QueryableEntry> otherIndexedResult : otherIndexedResults) {
                    if (!otherIndexedResult.contains(currentEntry)) {
                        return hasNext();
                    }
                }
            }
            if (lsNoIndexPredicates != null) {
                for (Predicate noIndexPredicate : lsNoIndexPredicates) {
                    if (!noIndexPredicate.apply(currentEntry)) {
                        return hasNext();
                    }
                }
            }
            return true;
        }

        public QueryableEntry next() {
            return currentEntry;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public int size() {
        return setSmallest.size();
    }
}
