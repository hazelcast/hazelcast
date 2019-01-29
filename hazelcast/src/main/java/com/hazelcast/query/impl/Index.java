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

import com.hazelcast.core.TypeConverter;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.QueryException;

import java.util.Set;

/**
 * Represents an index built on top of the attribute of the map entries.
 */
public interface Index {

    /**
     * @return the name of the attribute for which this index is built.
     */
    String getAttributeName();

    /**
     * Tells whether this index is ordered or not.
     * <p>
     * Ordered indexes support the fast evaluation of range queries. Unordered
     * indexes are still capable to execute range queries, but the performance
     * would be about the same as the full scan performance.
     *
     * @return {@code true} if this index is ordered, {@code false} otherwise.
     * @see #getSubRecords
     * @see #getSubRecordsBetween
     */
    boolean isOrdered();

    /**
     * Saves the given entry into this index.
     *
     * @param entry    the entry to save.
     * @param oldValue the previous old value associated with the entry or
     *                 {@code null} if the entry is new.
     * @throws QueryException if there were errors while extracting the
     *                        attribute value from the entry.
     */
    void saveEntryIndex(QueryableEntry entry, Object oldValue);

    /**
     * Removes the entry having the given key and the value from this index.
     *
     * @param key   the key of the entry to remove.
     * @param value the value of the entry to remove.
     * @throws QueryException if there were errors while extracting the
     *                        attribute value from the entry.
     */
    void removeEntryIndex(Data key, Object value);

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
    Set<QueryableEntry> getRecords(Comparable value);

    /**
     * Produces a result set containing entries whose attribute values are equal
     * to at least one of the given values.
     *
     * @param values the values to compare against.
     * @return the produced result set.
     */
    Set<QueryableEntry> getRecords(Comparable[] values);

    /**
     * Produces a result set by performing a range query on this index.
     * <p>
     * More precisely, this method produces a result set containing entries
     * whose attribute values are greater than or equal to the given
     * {@code from} value and less than or equal to the given {@code to} value.
     *
     * @param from the beginning of the range (inclusive).
     * @param to   the end of the range (inclusive).
     * @return the produced result set.
     */
    Set<QueryableEntry> getSubRecordsBetween(Comparable from, Comparable to);

    /**
     * Produces a result set containing entries whose attribute values are
     * satisfy the comparison of the given type with the given value.
     *
     * @param comparisonType the type of the comparison to perform.
     * @param searchedValue  the value to compare against.
     * @return the produced result set.
     */
    Set<QueryableEntry> getSubRecords(ComparisonType comparisonType, Comparable searchedValue);

    /**
     * Clears out all entries from this index.
     */
    void clear();

    /**
     * Releases all resources hold by this index, e.g. the allocated native
     * memory for the HD index.
     */
    void destroy();

}
