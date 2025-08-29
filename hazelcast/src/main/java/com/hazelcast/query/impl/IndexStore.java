/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.monitor.impl.IndexOperationStats;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.query.Predicate;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.Set;

/**
 * Defines a contract for index stores, so different index stores may be used
 * interchangeably with the same {@link Index} implementation.
 */
@SuppressWarnings("rawtypes")
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
     * arguments), so a more efficient representation may be chosen.
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
     * @param entry          the entry from which attribute values should be read.
     * @param entryToStore   the entry that should be stored in this index store;
     *                       it might differ from the passed {@code entry}: for
     *                       instance, {@code entryToStore} might be optimized
     *                       specifically for storage, while {@code entry} is
     *                       always optimized for attribute values extraction.
     * @param operationStats the operation stats to update while performing the
     *                       operation.
     * @see Index#putEntry
     * @see IndexOperationStats#onEntryAdded
     */
    void insert(Object value, CachedQueryEntry entry, QueryableEntry entryToStore, IndexOperationStats operationStats);

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
     * @param entryToStore   the entry that should be stored in this index store;
     *                       it might differ from the passed {@code entry}: for
     *                       instance, {@code entryToStore} might be optimized
     *                       specifically for storage, while {@code entry} is
     *                       always optimized for attribute values extraction.
     * @param operationStats the operation stats to update while performing the
     *                       operation.
     * @see #remove
     * @see #insert
     * @see Index#putEntry
     * @see IndexOperationStats#onEntryRemoved
     * @see IndexOperationStats#onEntryAdded
     */
    void update(Object oldValue, Object newValue, CachedQueryEntry entry, QueryableEntry entryToStore,
                IndexOperationStats operationStats);

    /**
     * Removes the existing entry mapping in this index.
     *
     * @param value          the value to remove the mapping from.
     * @param entry          the entry to remove.
     * @param operationStats the operation stats to update while performing the
     *                       operation.
     * @see Index#removeEntry
     * @see IndexOperationStats#onEntryRemoved
     */
    void remove(Object value, CachedQueryEntry entry, IndexOperationStats operationStats);

    /**
     * Clears the contents of this index by purging all its entries.
     */
    void clear();

    /**
     * Destroys this index by releasing all its resources.
     */
    void destroy();

    /**
     * @return {@code true} if this index store supports querying only with
     * {@link #evaluate} method, {@code false} otherwise.
     */
    boolean isEvaluateOnly();

    /**
     * @return {@code true} if this index store can evaluate a predicate of the
     * given predicate class, {@code false} otherwise.
     */
    boolean canEvaluate(Class<? extends Predicate> predicateClass);

    /**
     * Evaluates the given predicate using this index store.
     *
     * @param predicate the predicate to evaluate. The predicate is guaranteed
     *                  to be evaluable by this index store ({@code canEvaluate}
     *                  returned {@code true} for its class).
     * @return a set containing entries matching the given predicate.
     */
    Set<QueryableEntry> evaluate(Predicate predicate, TypeConverter converter);

    /**
     * @param descending whether the entries should come in the descending order.
     *                   {@code true} means a descending order,
     *                   {@code false} means an ascending order.
     * @return iterator over all index entries
     */
    Iterator<QueryableEntry> getSqlRecordIterator(boolean descending);

    /**
     * @return iterator over index entries that are equal to the given value
     */
    Iterator<QueryableEntry> getSqlRecordIterator(@Nonnull Comparable value);

    /**
     * @param comparison comparison type
     * @param descending whether the entries should come in the descending order.
     *                   {@code true} means a descending order,
     *                   {@code false} means an ascending order.
     * @return iterator over index entries that are matching the given comparisons type and value
     */
    Iterator<QueryableEntry> getSqlRecordIterator(Comparison comparison, Comparable value, boolean descending);

    /**
     * @param from lower bound
     * @param fromInclusive lower bound inclusive flag
     * @param to upper bound
     * @param toInclusive upper bound inclusive flag
     * @param descending whether the entries should come in the descending order.
     *                   {@code true} means a descending order,
     *                   {@code false} means an ascending order.
     * @return iterator over index entries matching the given range
     */
    Iterator<QueryableEntry> getSqlRecordIterator(Comparable from, boolean fromInclusive,
                                                  Comparable to, boolean toInclusive, boolean descending);

    /**
     * Get records for given value. Value can be {@link AbstractIndex#NULL}.
     * @param descending    whether the entries should come in the descending order.
     *                      {@code true} means a descending order,
     *                      {@code false} means an ascending order.
     * @return iterator over index entries that are equal to the given value
     */
    Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(@Nonnull Comparable value, boolean descending);

    default Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(
            @Nonnull Comparable value,
            boolean descending,
            Data lastEntryKeyData
    )  {
        throw new IllegalStateException("Not implemented");
    }

    /**
     * Scan all records, including NULL.
     *
     * @param descending whether the entries should come in the descending order.
     *                   {@code true} means a descending order,
     *                   {@code false} means an ascending order.
     * @return iterator over all index entries
     */
    Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(boolean descending);

    /**
     * Returns records matching comparison with special handling of NULL records.
     * <p>
     * This method uses similar convention of ordering of special values as {@link CompositeValue}
     * as it can be used with composite and non-composite values. The following ordering is assumed:
     * {@code -Inf < NULL < non-null values}. {@code +Inf} should not be passed to this method.
     * <p>
     * With that definition in mind:
     * <ol>
     *     <li>There are no records {@code < NULL}</li>
     *     <li>Trying to get records {@code <= NULL} is invalid</li>
     *     <li>It is possible to get records {@code > NULL} (all non-null records) or {@code >= NULL} (all records).
     *     <li>{@code < non-null value} or {@code <= non-null value} invocation will not return NULLs.</li>
     * </ol>
     * <p>
     * Note that the ordering of NULLs may not always fit SQL query requirements defined by {@code NULLS FIRST}
     * or {@code NULLS LAST}. In such case 2 scans have to be used.
     *
     * @param comparison comparison type
     * @param descending whether the entries should come in the descending order.
     *                   {@code true} means a descending order,
     *                   {@code false} means an ascending order.
     * @return iterator over index entries that are matching the given comparisons type and value
     */
    Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(@Nonnull Comparison comparison,
                                                        @Nonnull Comparable value,
                                                        boolean descending);

    default Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(@Nonnull Comparison comparison,
                                                                @Nonnull Comparable value,
                                                                boolean descending,
                                                                Data lastEntryKeyData) {
        throw new IllegalStateException("Not implemented");
    }

    /**
     * Returns records in given range. Both bounds must be given, however they may be {@link AbstractIndex#NULL}.
     * <p>
     * This method uses the same convention for special values comparing as
     * {@link #getSqlRecordIteratorBatch(Comparison, Comparable, boolean)}.
     * Ranges like the following are allowed, some combinations may lead to empty results:
     * <ol>
     *     <li>{@code non-null..non-null value}</li>
     *     <li>{@code NULL..NULL} - makes sense only when both ends are inclusive but it is better to use
     *         {@link #getSqlRecordIteratorBatch(Comparable, boolean)} in this case</li>
     *     <li>{@code NULL..non-null value} - makes sense only for ascending scan.
     *         Inclusive NULL end will return also NULL values.</li>
     *     <li>{@code non-null value..NULL} - makes sense only for descending scan.
     *         Inclusive NULL end will return also NULL values.</li>
     * </ol>
     *
     * @param from lower bound
     * @param fromInclusive lower bound inclusive flag
     * @param to upper bound
     * @param toInclusive upper bound inclusive flag
     * @param descending whether the entries should come in the descending order.
     *                   {@code true} means a descending order,
     *                   {@code false} means an ascending order.
     * @return iterator over index entries matching the given range
     */
    Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(
            @Nonnull Comparable from,
            boolean fromInclusive,
            @Nonnull Comparable to,
            boolean toInclusive,
            boolean descending
    );

    default Iterator<IndexKeyEntries> getSqlRecordIteratorBatch(
            @Nonnull Comparable from,
            boolean fromInclusive,
            @Nonnull Comparable to,
            boolean toInclusive,
            boolean descending,
            Data lastEntryKeyData
    ) {
        throw new IllegalStateException("Not implemented");
    }

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
