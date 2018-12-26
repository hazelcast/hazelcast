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

import java.util.AbstractSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.hazelcast.util.SetUtil.createHashSet;

/**
 * Or result set for Predicates.
 */
public class OrResultSet extends AbstractSet<QueryableEntry> implements LazyResultSet {

    private static final int ENTRY_MULTIPLE = 4;
    private static final int ENTRY_MIN_SIZE = 8;

    private final List<Set<QueryableEntry>> indexedResults;
    private Set<QueryableEntry> entries;
    private final int estimatedSize;

    public OrResultSet(List<Set<QueryableEntry>> indexedResults) {
        this.indexedResults = indexedResults;
        int resultSize = 0;
        // Since all results are {@com.hazelcast.query.impl.LazyResultSet}, calling size has no cost
        // and gave more accurate size estimate
        for (Set<QueryableEntry> indexedResult : indexedResults) {
            resultSize += indexedResult.size();
        }
        estimatedSize = resultSize;
    }

    @Override
    public boolean contains(Object o) {
        for (Set<QueryableEntry> otherIndexedResult : indexedResults) {
            if (otherIndexedResult.contains(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<QueryableEntry> iterator() {
        init();
        return entries.iterator();
    }

    @Override
    public int size() {
        return estimatedSize();
    }

    /**
     * @return returns estimated size without allocating the full result set
     */
    @Override
    public int estimatedSize() {
        if (entries == null) {
            return estimatedSize;
        } else {
            return entries.size();
        }
    }

    @Override
    public void init() {
        if (entries == null) {
            if (indexedResults.isEmpty()) {
                entries = Collections.emptySet();
            } else {
                if (indexedResults.size() == 1) {
                    entries = new HashSet<QueryableEntry>(indexedResults.get(0));
                } else {
                    entries = createHashSet(Math.max(ENTRY_MIN_SIZE, indexedResults.size() * ENTRY_MULTIPLE));
                    for (Set<QueryableEntry> result : indexedResults) {
                        entries.addAll(result);
                    }
                }
            }
        }
    }
}
