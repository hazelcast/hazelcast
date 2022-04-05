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

package com.hazelcast.query.impl;

import com.hazelcast.internal.iteration.IterationPointer;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Collection;

/**
 * Holder class for a collection of queried entries and the index from which new items can be fetched.
 * It can be used when fetching query results in chunks (e.g. efficient paging).
 */
public class QueryableEntriesSegment {

    private final Collection<QueryableEntry> entries;
    private final IterationPointer[] pointers;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is an internal class")
    public QueryableEntriesSegment(Collection<QueryableEntry> entries, IterationPointer[] pointers) {
        this.entries = entries;
        this.pointers = pointers;
    }

    public Collection<QueryableEntry> getEntries() {
        return entries;
    }

    /**
     * Returns the iteration pointers representing the current iteration state.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "This is an internal class")
    public IterationPointer[] getPointers() {
        return pointers;
    }
}
