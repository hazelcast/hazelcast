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

import com.hazelcast.query.impl.collections.LazySet;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.query.impl.predicates.PredicateUtils.estimatedSizeOf;

/**
 * Or result set for Predicates.
 */
public class OrResultSet extends LazySet<QueryableEntry> {

    private final List<Set<QueryableEntry>> indexedResults;
    private final int estimatedSize;

    public OrResultSet(List<Set<QueryableEntry>> indexedResults) {
        this.indexedResults = indexedResults;
        int size = 0;
        // Results may be {@com.hazelcast.query.impl.collections.LazySet}, so get estimated size
        for (Set<QueryableEntry> indexedResult : indexedResults) {
            size += estimatedSizeOf(indexedResult);
        }
        estimatedSize = size;
    }

    @Nonnull
    @Override
    protected Set<QueryableEntry> initialize() {
        if (indexedResults.isEmpty()) {
            return Collections.emptySet();
        }
        //Since combining all predicate results required (duplicate removal), we're paying copy cost again
        //TODO : check what can be done to prevent second copy cost
        Set<QueryableEntry> results = new HashSet<QueryableEntry>();
        for (Set<QueryableEntry> result : indexedResults) {
            results.addAll(result);
        }
        return Collections.unmodifiableSet(results);
    }

    @Override
    public int estimatedSize() {
        return estimatedSize;
    }
}
