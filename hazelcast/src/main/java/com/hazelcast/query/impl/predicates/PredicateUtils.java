/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.AndResultSet;
import com.hazelcast.query.impl.OrResultSet;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Collection;

import static com.hazelcast.query.impl.AbstractIndex.NULL;

public final class PredicateUtils {

    private static final int EXPECTED_AVERAGE_COMPONENT_NAME_LENGTH = 16;

    private static final String THIS_DOT = "this.";

    private static final String KEY_HASH = "__key#";

    private static final String KEY_DOT = "__key.";

    private PredicateUtils() {
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
     * @return {@code true} if the given predicate is a {@link RangePredicate},
     * {@code false} otherwise.
     */
    public static boolean isRangePredicate(Predicate predicate) {
        // XXX: NotEqualPredicate is a subclass of EqualPredicate
        return predicate instanceof RangePredicate && !(predicate instanceof NotEqualPredicate);
    }

    /**
     * @return {@code true} if the given predicate is an {@link EqualPredicate},
     * {@code false} otherwise.
     */
    public static boolean isEqualPredicate(Predicate predicate) {
        // XXX: NotEqualPredicate is a subclass of EqualPredicate
        return predicate instanceof EqualPredicate && !(predicate instanceof NotEqualPredicate);
    }

    /**
     * Produces canonical attribute representation in the following way:
     *
     * <ol>
     * <li>Strips an unnecessary "this." qualifier from the passed attribute.
     * <li>Replaces "__key#" qualifier with "__key.".
     * </ol>
     *
     * @param attribute the attribute to canonicalize.
     * @return the canonicalized attribute representation.
     * @see #constructCanonicalCompositeIndexName
     */
    public static String canonicalizeAttribute(String attribute) {
        if (attribute.startsWith(THIS_DOT)) {
            return attribute.substring(THIS_DOT.length());
        }
        if (attribute.startsWith(KEY_HASH)) {
            return KEY_DOT + attribute.substring(KEY_HASH.length());
        }
        return attribute;
    }

    /**
     * Constructs a canonical index name from the given index components.
     * <p>
     * A canonical name is a comma-separated list of index components with a
     * single space character going after every comma.
     * <p>
     * It's a caller's responsibility to canonicalize individual components
     * as specified by {@link #canonicalizeAttribute}.
     *
     * @param components the index components to construct the canonical index
     *                   name from.
     * @return the constructed canonical index name.
     */
    public static String constructCanonicalCompositeIndexName(String[] components) {
        assert components.length > 1;

        StringBuilder builder = new StringBuilder(components.length * EXPECTED_AVERAGE_COMPONENT_NAME_LENGTH);
        for (String component : components) {
            if (builder.length() > 0) {
                builder.append(", ");
            }
            builder.append(component);
        }
        return builder.toString();
    }

}
