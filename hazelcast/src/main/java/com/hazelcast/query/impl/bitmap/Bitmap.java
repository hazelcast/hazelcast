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

package com.hazelcast.query.impl.bitmap;

import com.hazelcast.core.TypeConverter;
import com.hazelcast.internal.monitor.impl.IndexOperationStats;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.InPredicate;
import com.hazelcast.query.impl.predicates.NotEqualPredicate;
import com.hazelcast.query.impl.predicates.NotPredicate;
import com.hazelcast.query.impl.predicates.OrPredicate;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Provides indexing and querying capabilities for a single attribute of entries
 * of type {@code E}. Each indexed entry is uniquely identified by its unique
 * {@code long} key provided externally.
 * <p>
 * Internally, each bitmap manages a set of sparse bit sets, one for each
 * possible attribute value, and a sparse array to map from unique {@code long}
 * entry keys back to entries.
 *
 * @param <E> the type of entries being indexed.
 */
@SuppressWarnings("rawtypes")
public final class Bitmap<E> {

    private final Map<Object, SparseBitSet> bitSets = new HashMap<>();

    private final SparseArray<E> entries = new SparseArray<>();

    // Note! At the moment bitmap index doesn't support memory statistics,
    // because we cannot produce precise memory estimate.
    // Instead, we provide zero memory consumption estimation.
    private enum ZeroCost { ZERO_COST }

    /**
     * Inserts the given values associated with the given entry having the given
     * unique key.
     *
     * @param values the values to insert.
     * @param key    the unique key of the entry being inserted.
     * @param entry  the entry to insert.
     */
    public void insert(Iterator values, long key, E entry, IndexOperationStats operationStats) {
        while (values.hasNext()) {
            Object value = values.next();
            assert value != null;

            SparseBitSet bitSet = bitSets.get(value);
            if (bitSet == null) {
                bitSet = new SparseBitSet();
                bitSets.put(value, bitSet);
            }
            operationStats.onEntryAdded(ZeroCost.ZERO_COST);
            bitSet.add(key);
        }

        entries.set(key, entry);
    }

    /**
     * Updates the given old values to the given new values associated with the
     * given entry having the given unique key.
     *
     * @param oldValues the old values to replace.
     * @param newValues the new values to replace with.
     * @param key       the unique key of the entry being updated.
     * @param entry     the entry to update.
     */
    public void update(Iterator oldValues, Iterator newValues, long key, E entry, IndexOperationStats operationStats) {
        while (oldValues.hasNext()) {
            Object value = oldValues.next();
            assert value != null;

            SparseBitSet bitSet = bitSets.get(value);
            if (bitSet != null) {
                bitSet.remove(key);
            }
            operationStats.onEntryRemoved(ZeroCost.ZERO_COST);
        }

        while (newValues.hasNext()) {
            Object value = newValues.next();
            assert value != null;

            SparseBitSet bitSet = bitSets.get(value);
            if (bitSet == null) {
                bitSet = new SparseBitSet();
                bitSets.put(value, bitSet);
            }
            operationStats.onEntryAdded(ZeroCost.ZERO_COST);
            bitSet.add(key);
        }

        entries.set(key, entry);
    }

    /**
     * Removes the given values associated with an entry identified by the given
     * unique key.
     *
     * @param values the values to remove.
     * @param key    the unique key of an entry being removed.
     */
    public void remove(Iterator values, long key, IndexOperationStats operationStats) {
        while (values.hasNext()) {
            Object value = values.next();
            assert value != null;

            SparseBitSet bitSet = bitSets.get(value);
            if (bitSet != null) {
                if (bitSet.remove(key)) {
                    bitSets.remove(value);
                }
            }
            operationStats.onEntryRemoved(ZeroCost.ZERO_COST);
        }

        entries.clear(key);
    }

    /**
     * Clears this bitmap.
     */
    public void clear() {
        bitSets.clear();
        entries.clear();
    }

    /**
     * Evaluates the given predicate while converting the predicate arguments
     * using the given converter.
     * <p>
     * The following predicates (and combinations of them) are supported:
     * {@link AndPredicate}, {@link OrPredicate}, {@link NotPredicate}, {@link
     * NotEqualPredicate}, {@link EqualPredicate}, {@link InPredicate}.
     *
     * @param predicate the predicate to evaluate.
     * @param converter the converter to use for the predicate arguments
     *                  conversion.
     * @return an iterator containing entries matching the given predicate.
     */
    public Iterator<E> evaluate(Predicate predicate, TypeConverter converter) {
        return new EntryIterator<>(predicateIterator(predicate, converter), entries.iterator());
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private AscendingLongIterator predicateIterator(Predicate predicate, TypeConverter converter) {
        if (predicate instanceof AndPredicate) {
            Predicate[] predicates = ((AndPredicate) predicate).getPredicates();
            assert predicates.length > 0;
            if (predicates.length == 1) {
                return predicateIterator(predicates[0], converter);
            } else {
                return BitmapAlgorithms.and(predicateIterators(predicates, converter));
            }
        }

        if (predicate instanceof OrPredicate) {
            Predicate[] predicates = ((OrPredicate) predicate).getPredicates();
            assert predicates.length > 0;
            if (predicates.length == 1) {
                return predicateIterator(predicates[0], converter);
            } else {
                return BitmapAlgorithms.or(predicateIterators(predicates, converter));
            }
        }

        if (predicate instanceof NotPredicate) {
            Predicate subPredicate = ((NotPredicate) predicate).getPredicate();
            return BitmapAlgorithms.not(predicateIterator(subPredicate, converter), entries);
        }

        if (predicate instanceof NotEqualPredicate) {
            Comparable value = ((NotEqualPredicate) predicate).getValue();
            return BitmapAlgorithms.not(valueIterator(value, converter), entries);
        }

        if (predicate instanceof EqualPredicate) {
            Comparable value = ((EqualPredicate) predicate).getFrom();
            return valueIterator(value, converter);
        }

        if (predicate instanceof InPredicate) {
            Comparable[] values = ((InPredicate) predicate).getValues();
            return BitmapAlgorithms.or(valueIterators(values, converter));
        }

        throw new IllegalArgumentException("unexpected predicate: " + predicate);
    }

    private AscendingLongIterator[] predicateIterators(Predicate[] predicates, TypeConverter converter) {
        AscendingLongIterator[] iterators = new AscendingLongIterator[predicates.length];
        for (int i = 0; i < predicates.length; ++i) {
            iterators[i] = predicateIterator(predicates[i], converter);
        }
        return iterators;
    }

    private AscendingLongIterator valueIterator(Comparable value, TypeConverter converter) {
        SparseBitSet bitSet = bitSets.get(converter.convert(value));
        return bitSet == null ? AscendingLongIterator.EMPTY : bitSet.iterator();
    }

    private AscendingLongIterator[] valueIterators(Comparable[] values, TypeConverter converter) {
        AscendingLongIterator[] iterators = new AscendingLongIterator[values.length];
        for (int i = 0; i < values.length; ++i) {
            iterators[i] = valueIterator(values[i], converter);
        }
        return iterators;
    }

    /**
     * Maps unique entry keys back to entries.
     */
    private static final class EntryIterator<E> implements Iterator<E> {

        private final AscendingLongIterator iterator;
        private final SparseArray.Iterator<E> universe;

        EntryIterator(AscendingLongIterator iterator, SparseArray.Iterator<E> universe) {
            this.iterator = iterator;
            this.universe = universe;
        }

        @Override
        public boolean hasNext() {
            return iterator.getIndex() != AscendingLongIterator.END;
        }

        @Override
        public E next() {
            long member = iterator.advance();
            long advancedTo = universe.advanceAtLeastTo(member);
            assert advancedTo == member;
            return universe.getValue();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("bitmap iterators are read-only");
        }

    }

}
