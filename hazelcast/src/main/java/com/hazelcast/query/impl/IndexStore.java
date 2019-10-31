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

package com.hazelcast.query.impl;

import com.hazelcast.internal.monitor.impl.IndexOperationStats;
import com.hazelcast.nio.serialization.Data;

import java.util.Set;

/**
 * Defines a contract for index stores, so different index stores may be used
 * interchangeably with the same {@link Index} implementation.
 */
public interface IndexStore {

    /**
     * Canonicalizes the given value for the purpose of a hash-based lookup.
     * <p>
     * The method is used while performing InPredicate queries to canonicalize
     * the set of values in question, so additional duplicate-eliminating
     * post-processing step can be avoided.
     * <p>
     * The main difference comparing to {@link BaseIndexStore#canonicalizeScalarForStorage}
     * is that this method is specifically designed to support the
     * canonicalization of transient non-persistent values (think of query
     * arguments), so a more efficient representation may chosen.
     *
     * @param value the value to canonicalize.
     * @return the canonicalized value.
     */
    Comparable canonicalizeQueryArgumentScalar(Comparable value);

    /**
     * Inserts the given entry into this index store under the given value
     * acting as an index key.
     *
     * @param value          the value to insert the entry under.
     * @param entry          the entry to insert.
     * @param operationStats the operation stats to update while performing the
     *                       operation.
     * @see Index#putEntry
     * @see IndexOperationStats#onEntryAdded
     */
    void insert(Object value, QueryableEntry entry, IndexOperationStats operationStats);

    /**
     * Updates the existing entry mapping in this index by remapping it from the
     * given old value to the new given value.
     * <p>
     * The update operation is logically equivalent to removing the old mapping
     * and inserting the new one.
     *
     * @param oldValue       the value to remap the entry from.
     * @param newValue       the new value to remap the entry to.
     * @param entry          the entry to remap.
     * @param operationStats the operation stats to update while performing the
     *                       operation.
     * @see #remove
     * @see #insert
     * @see Index#putEntry
     * @see IndexOperationStats#onEntryRemoved
     * @see IndexOperationStats#onEntryAdded
     */
    void update(Object oldValue, Object newValue, QueryableEntry entry, IndexOperationStats operationStats);

    /**
     * Removes the existing entry mapping in this index.
     *
     * @param value          the value to remove the mapping from.
     * @param indexKey       the value to remove the mapping to.
     * @param operationStats the operation stats to update while performing the
     *                       operation.
     * @see Index#removeEntry
     * @see IndexOperationStats#onEntryRemoved
     */
    void remove(Object value, Data indexKey, IndexOperationStats operationStats);

    /**
     * Clears the contents of this index by purging all its entries.
     */
    void clear();

    /**
     * Destroys this index by releasing all its resources.
     */
    void destroy();

    /**
     * Obtains entries that have indexed attribute value equal to the given
     * value.
     *
     * @param value the value to obtains the entries for.
     * @return the obtained entries.
     * @see Index#getRecords(Comparable)
     */
    Set<QueryableEntry> getRecords(Comparable value);

    /**
     * Obtains entries that have indexed attribute value equal to one of the
     * given set of values.
     *
     * @param values the values to obtains the entries for.
     * @return the obtained entries.
     * @see Index#getRecords(Comparable[])
     */
    Set<QueryableEntry> getRecords(Set<Comparable> values);

    /**
     * Obtains entries that have indexed attribute value satisfying the given
     * comparison with the given value.
     *
     * @param comparison the comparison to perform.
     * @param value      the value to perform the comparison with.
     * @return the obtained entries.
     * @see Index#getRecords(Comparison, Comparable)
     */
    Set<QueryableEntry> getRecords(Comparison comparison, Comparable value);

    /**
     * Obtains entries that have indexed attribute value belonging to the given
     * range.
     *
     * @param from          the beginning of the range.
     * @param fromInclusive {@code true} if the beginning of the range is
     *                      inclusive, {@code false} otherwise.
     * @param to            the end of the range.
     * @param toInclusive   {@code true} if the end of the range is inclusive,
     *                      {@code false} otherwise.
     * @return the obtained entries.
     * @see Index#getRecords(Comparable, boolean, Comparable, boolean)
     */
    Set<QueryableEntry> getRecords(Comparable from, boolean fromInclusive, Comparable to, boolean toInclusive);

}
