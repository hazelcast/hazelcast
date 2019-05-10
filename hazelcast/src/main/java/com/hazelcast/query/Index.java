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

package com.hazelcast.query;

import com.hazelcast.core.TypeConverter;

import java.util.Set;

/**
 * Represents an index built on top of the attribute of the map entries.
 */
public interface Index {
    /**
     * @return the canonical name of this index: for single-attribute
     * non-composite indexes, it's the attribute name itself stripping an
     * unnecessary "this." qualifier, if any; for composite indexes, it's a
     * comma-separated list of index components with a single space character
     * going after every comma, any unnecessary "this." qualifiers are stripped.
     */
    String getName();

    /**
     * @return the components of this index for composite indexes, {@code null}
     * for single-attribute non-composite indexes.
     */
    String[] getComponents();

    /**
     * Tells whether this index is ordered or not.
     * <p>
     * Ordered indexes support the fast evaluation of range queries. Unordered
     * indexes are still capable to execute range queries, but the performance
     * would be about the same as the full scan performance.
     *
     * @return {@code true} if this index is ordered, {@code false} otherwise.
     * @see #getRecords(Comparable, boolean, Comparable, boolean)
     */
    boolean isOrdered();

    /**
     * @return the converter associated with this index; or {@code null} if the
     * converter is not known because there were no saves to this index and
     * the attribute type is not inferred yet.
     */
    TypeConverter getConverter();

    /**
     * Produces a result set containing entries whose attribute values are equal
     * to the given value.
     *
     * @param value the value to compare against.
     * @return the produced result set.
     */
    Set<? extends QueryableEntry> getRecords(Comparable value);

    /**
     * Produces a result set containing entries whose attribute values are equal
     * to at least one of the given values.
     *
     * @param values the values to compare against.
     * @return the produced result set.
     */
    Set<? extends QueryableEntry> getRecords(Comparable[] values);

    /**
     * Produces a result set by performing a range query on this index with the
     * range defined by the passed arguments.
     *
     * @param from          the beginning of the range.
     * @param fromInclusive {@code true} if the beginning of the range is
     *                      inclusive, {@code false} otherwise.
     * @param to            the end of the range.
     * @param toInclusive   {@code true} if the end of the range is inclusive,
     *                      {@code false} otherwise.
     * @return the produced result set.
     */
    Set<? extends QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive);
}
