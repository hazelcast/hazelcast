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

import java.util.*;

public class OrResultSet extends AbstractSet<QueryableEntry> {
    private final List<Set<QueryableEntry>> lsIndexedResults;
    Set<QueryableEntry> entries = null;

    public OrResultSet(List<Set<QueryableEntry>> lsIndexedResults) {
        this.lsIndexedResults = lsIndexedResults;
    }

    public byte[] toByteArray(ObjectDataOutput out) {
        return null;
    }

    @Override
    public boolean contains(Object o) {
        for (Set<QueryableEntry> otherIndexedResult : lsIndexedResults) {
            if (otherIndexedResult.contains(o)) return true;
        }
        return false;
    }

    @Override
    public Iterator<QueryableEntry> iterator() {
        if (entries != null) {
            return entries.iterator();
        }
        entries = new HashSet<QueryableEntry>(lsIndexedResults.get(0));
        for (int i = 1; i < lsIndexedResults.size(); i++) {
            entries.addAll(lsIndexedResults.get(i));
        }
        return entries.iterator();
    }

    @Override
    public int size() {
        return lsIndexedResults.get(0).size();
    }
}
