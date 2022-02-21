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

package com.hazelcast.query.impl.predicates;

import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.AndResultSet;
import com.hazelcast.query.impl.OrResultSet;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Collection;
import java.util.Optional;

import static com.hazelcast.query.impl.AbstractIndex.NULL;

public final class PredicateUtils {
    private PredicateUtils() {
        // No-op.
    }

    /**
     * In case of AndResultSet and OrResultSet calling size() may be very
     * expensive so quicker estimatedSize() is used.
     *
     * @param result result of a predicated search
     * @return size or estimated size
     * @see AndResultSet#estimatedSize()
     * @see OrResultSet#estimatedSize()
     */
    public static int estimatedSizeOf(Collection<QueryableEntry> result) {
        if (result instanceof AndResultSet) {
            return ((AndResultSet) result).estimatedSize();
        } else if (result instanceof OrResultSet) {
            return ((OrResultSet) result).estimatedSize();
        }
        return result.size();
    }

    /**
     * @return {@code true} if the given value is considered as a null-like by
     * predicates and indexes, {@code false} otherwise.
     */
    public static boolean isNull(Comparable value) {
        return value == null || value == NULL;
    }

    /**
     * Unwraps the given potentially {@link Optional optional} value.
     *
     * @param value the potentially optional value to unwrap.
     * @return an unwrapped value, if the given value is optional; the
     * original given value, if it's not optional.
     */
    @SuppressWarnings("unchecked")
    public static <T> T unwrapIfOptional(Object value) {
        return value instanceof Optional ? ((Optional<T>) value).orElse(null) : (T) value;
    }

    public static PagingPredicateImpl unwrapPagingPredicate(Predicate predicate) {
        if (predicate instanceof PagingPredicateImpl) {
            return (PagingPredicateImpl) predicate;
        }

        Predicate unwrappedPredicate = ((PartitionPredicate) predicate).getTarget();
        return (PagingPredicateImpl) unwrappedPredicate;
    }
}
