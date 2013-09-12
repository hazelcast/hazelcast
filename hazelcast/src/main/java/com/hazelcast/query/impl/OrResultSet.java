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

import java.util.*;

public class OrResultSet extends AbstractSet<QueryableEntry> {

    private final List<Set<QueryableEntry>> indexedResults;
    private Set<QueryableEntry> entries = null;

    public OrResultSet(List<Set<QueryableEntry>> indexedResults) {
        this.indexedResults = indexedResults;
    }

    @Override
    public boolean contains(Object o) {
        for (Set<QueryableEntry> otherIndexedResult : indexedResults) {
            if (otherIndexedResult.contains(o)) return true;
        }
        return false;
    }

    @Override
    public Iterator<QueryableEntry> iterator() {
        if (entries == null) {
            if (!indexedResults.isEmpty()) {
                if (indexedResults.size() == 1) {
                    entries = new HashSet<QueryableEntry>(indexedResults.get(0));
                } else {
                    entries = new HashSet<QueryableEntry>();
                    for (Set<QueryableEntry> result : indexedResults) {
                        entries.addAll(result);
                    }
                }
            } else {
                entries = Collections.emptySet();
            }
        }
        return entries.iterator();
    }

    @Override
    public int size() {
        return indexedResults.isEmpty() ? 0 : indexedResults.get(0).size();
    }
}
