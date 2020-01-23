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

package com.hazelcast.query.impl;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;

import java.util.Set;

/**
 * Represents an index built on top of the attribute of the map entries.
 */
public interface Index {

    /**
     * @return Index name.
     */
    String getName();

    /**
     * @return the components of this index.
     */
    String[] getComponents();

    /**
     * @return Configuration of the index.
     */
    IndexConfig getConfig();

    /**
     * Tells whether this index is ordered or not.
     * <p>
     * Ordered indexes support the fast evaluation of range queries. Unordered
     * indexes are still capable to execute range queries, but the performance
     * would be about the same as the full scan performance.
     *
     * @return {@code true} if this index is ordered, {@code false} otherwise.
     * @see #getRecords(Comparison, Comparable)
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
     * Saves the given entry into this index.
     *
     * @param entry           the entry to save.
     * @param oldValue        the previous old value associated with the entry or
     *                        {@code null} if the entry is new.
     * @param operationSource the operation source.
     * @throws QueryException if there were errors while extracting the
     *                        attribute value from the entry.
     */
    void putEntry(QueryableEntry entry, Object oldValue, OperationSource operationSource);

    /**
     * Removes the entry having the given key and the value from this index.
     *
     * @param key             the key of the entry to remove.
     * @param value           the value of the entry to remove.
     * @param operationSource the operation source.
     * @throws QueryException if there were errors while extracting the
     *                        attribute value from the entry.
     */
    void removeEntry(Data key, Object value, OperationSource operationSource);

    /**
     * @return {@code true} if this index supports querying only with {@link
     * #evaluate} method, {@code false} otherwise.
     */
    boolean isEvaluateOnly();

    /**
     * @return {@code true} if this index can evaluate a predicate of the given
     * predicate class, {@code false} otherwise.
     */
    boolean canEvaluate(Class<? extends Predicate> predicateClass);

    /**
     * Evaluates the given predicate using this index.
     *
     * @param predicate the predicate to evaluate. The predicate is guaranteed
     *                  to be evaluable by this index ({@code canEvaluate}
     *                  returned {@code true} for its class).
     * @return a set containing entries matching the given predicate.
     */
    Set<QueryableEntry> evaluate(Predicate predicate);

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
    Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive);

    /**
     * Produces a result set containing entries whose attribute values are
     * satisfy the comparison of the given type with the given value.
     *
     * @param comparison the type of the comparison to perform.
     * @param value      the value to compare against.
     * @return the produced result set.
     */
    Set<QueryableEntry> getRecords(Comparison comparison, Comparable value);

    /**
     * Clears out all entries from this index.
     */
    void clear();

    /**
     * Releases all resources hold by this index, e.g. the allocated native
     * memory for the HD index.
     */
    void destroy();

    /**
     * Identifies an original source of an index operation.
     * <p>
     * Required for the index stats tracking to ignore index operations
     * initiated internally by Hazelcast. We can't achieve the same behaviour
     * on the pure stats level, e.g. by turning stats off during a partition
     * migration, since global indexes and their stats are shared across
     * partitions.
     */
    enum OperationSource {

        /**
         * Indicates that an index operation was initiated by a user; for
         * instance, as a result of a new map entry insertion.
         */
        USER,

        /**
         * Indicates that an index operation was initiated internally by
         * Hazelcast; for instance, as a result of a partition migration.
         */
        SYSTEM

    }

}
