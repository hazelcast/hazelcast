/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.query.impl.AndResultSet;
import com.hazelcast.query.impl.OrResultSet;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.hazelcast.query.impl.AbstractIndex.NULL;

public final class PredicateUtils {

    private static final int EXPECTED_AVERAGE_COMPONENT_NAME_LENGTH = 16;

    private static final int MAX_INDEX_COMPONENTS = 255;

    private static final Pattern THIS_PATTERN = Pattern.compile("^this\\.");

    private static final Pattern COMMA_PATTERN = Pattern.compile("\\s*,\\s*");

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

    /**
     * Produces canonical attribute representation by stripping an unnecessary
     * "this." qualifier from the passed attribute, if any.
     *
     * @param attribute the attribute to canonicalize.
     * @return the canonicalized attribute representation.
     * @see #constructCanonicalCompositeIndexName
     */
    public static String canonicalizeAttribute(String attribute) {
        return THIS_PATTERN.matcher(attribute).replaceFirst("");
    }

    /**
     * Parses the given index name into components.
     *
     * @param name the index name to parse.
     * @return the parsed components or {@code null} if the given index name
     * doesn't describe a composite index components.
     * @throws IllegalArgumentException if the given index name is empty.
     * @throws IllegalArgumentException if the given index name contains empty
     *                                  components.
     * @throws IllegalArgumentException if the given index name contains
     *                                  duplicate components.
     * @throws IllegalArgumentException if the given index name has more than
     *                                  255 components.
     * @see #constructCanonicalCompositeIndexName
     */
    public static String[] parseOutCompositeIndexComponents(String name) {
        String[] components = COMMA_PATTERN.split(name, -1);

        if (components.length == 1) {
            return null;
        }

        if (components.length > MAX_INDEX_COMPONENTS) {
            throw new IllegalArgumentException("Too many composite index attributes: " + name);
        }

        Set<String> seenComponents = new HashSet<>(components.length);
        for (int i = 0; i < components.length; ++i) {
            String component = PredicateUtils.canonicalizeAttribute(components[i]);
            components[i] = component;

            if (component.isEmpty()) {
                throw new IllegalArgumentException("Empty composite index attribute: " + name);
            }
            if (!seenComponents.add(component)) {
                throw new IllegalArgumentException("Duplicate composite index attribute: " + name);
            }
        }

        return components;
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
