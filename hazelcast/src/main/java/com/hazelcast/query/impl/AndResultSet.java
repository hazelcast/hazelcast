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
import com.hazelcast.query.impl.collections.LazySet;
import com.hazelcast.query.impl.collections.ReadOnlyFilterableCollectionDelegate;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.hazelcast.query.impl.predicates.PredicateUtils.estimatedSizeOf;

/**
 * And Result set for Predicates.
 */
public class AndResultSet extends LazySet<QueryableEntry> {

    @Nonnull
    private final Set<QueryableEntry> smallestResult;
    private final List<? extends Predicate> otherPredicates;

    public AndResultSet(@Nonnull Set<QueryableEntry> smallestResult, List<? extends Predicate> otherPredicates) {
        this.smallestResult = smallestResult;
        this.otherPredicates = otherPredicates;
    }

    @Nonnull
    @Override
    protected Set<QueryableEntry> initialize() {
        if (otherPredicates == null || otherPredicates.isEmpty()) {
            return Collections.unmodifiableSet(smallestResult);
        } else {
            return new ReadOnlyFilterableCollectionDelegate(smallestResult, otherPredicates);
        }
    }

    @Override
    public int estimatedSize() {
        return estimatedSizeOf(smallestResult);
    }

}
