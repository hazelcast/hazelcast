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

import java.util.Collection;

/**
 * Holder class for a collection of queried entries and the index from which new items can be fetched.
 * It can be used when fetching query results in chunks (e.g. efficient paging).
 */
public class QueryableEntriesSegment {

    private final Collection<QueryableEntry> entries;
    private final int nextTableIndexToReadFrom;

    public QueryableEntriesSegment(Collection<QueryableEntry> entries, int nextTableIndexToReadFrom) {
        this.entries = entries;
        this.nextTableIndexToReadFrom = nextTableIndexToReadFrom;
    }

    public Collection<QueryableEntry> getEntries() {
        return entries;
    }

    public int getNextTableIndexToReadFrom() {
        return nextTableIndexToReadFrom;
    }
}
