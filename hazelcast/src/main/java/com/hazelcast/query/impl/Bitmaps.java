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

import com.hazelcast.core.TypeConverter;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.InPredicate;
import com.hazelcast.query.impl.predicates.NotEqualPredicate;
import com.hazelcast.query.impl.predicates.OrPredicate;
import com.hazelcast.util.collection.Long2ObjectHashMap;
import org.roaringbitmap.longlong.HackedRoaringBitmap;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.PeekableLongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;

public final class Bitmaps<E> {

    private static final Comparator<PeekableLongIterator> MIN_HEAP_COMPARATOR = new Comparator<PeekableLongIterator>() {
        @Override
        public int compare(PeekableLongIterator a, PeekableLongIterator b) {
            return Numbers.compareLongs(a.peekNext(), b.peekNext());
        }
    };

    private final Map<Object, HackedRoaringBitmap> bitmaps = new HashMap<Object, HackedRoaringBitmap>();

    private final Long2ObjectHashMap<E> entries;

    public Bitmaps(double loadFactor) {
        entries = new Long2ObjectHashMap<E>(Long2ObjectHashMap.DEFAULT_INITIAL_CAPACITY, loadFactor);
    }

    public void insert(Iterator values, long key, E entry) {
        while (values.hasNext()) {
            Object value = values.next();
            assert value != null;

            HackedRoaringBitmap bitmap = bitmaps.get(value);
            if (bitmap == null) {
                bitmap = new HackedRoaringBitmap();
                bitmaps.put(value, bitmap);
            }
            bitmap.addLong(key);
        }

        E replaced = entries.put(key, entry);
        assert replaced == null;
    }

    public void update(Iterator oldValues, Iterator newValues, long key, E entry) {
        while (oldValues.hasNext()) {
            Object value = oldValues.next();
            assert value != null;

            Roaring64NavigableMap bitmap = bitmaps.get(value);
            if (bitmap != null) {
                bitmap.removeLong(key);
            }
        }

        while (newValues.hasNext()) {
            Object value = newValues.next();
            assert value != null;

            HackedRoaringBitmap bitmap = bitmaps.get(value);
            if (bitmap == null) {
                bitmap = new HackedRoaringBitmap();
                bitmaps.put(value, bitmap);
            }
            bitmap.addLong(key);
        }

        E replaced = entries.put(key, entry);
        assert replaced != null;
    }

    public void remove(Iterator values, long key) {
        // TODO: how to reclaim potentially freed memory?

        while (values.hasNext()) {
            Object value = values.next();
            assert value != null;

            Roaring64NavigableMap bitmap = bitmaps.get(value);
            if (bitmap != null) {
                bitmap.removeLong(key);
            }
        }

        entries.remove(key);
    }

    public void clear() {
        for (Roaring64NavigableMap bitmap : bitmaps.values()) {
            bitmap.clear();
        }
        bitmaps.clear();
        entries.clear();
    }

    public Iterator<E> evaluate(Predicate predicate, TypeConverter converter) {
        if (predicate.getClass() == NotEqualPredicate.class) {
            // TODO remove this once proper not-equal iterator implemented
            return notEqualEntries(predicate, converter);
        }

        return new EntryIterator(predicate(predicate, converter));
    }

    public Iterator<E> getHaving(Object value) {
        return new EntryIterator(equal(value));
    }

    public Iterator<E> getHavingAnyOf(Set<?> values) {
        return new EntryIterator(or(valueIterators(values.iterator()), values.size()));
    }

    private PeekableLongIterator predicate(Predicate predicate, TypeConverter converter) {
        if (predicate.getClass() == EqualPredicate.class) {
            EqualPredicate equalPredicate = (EqualPredicate) predicate;
            return equal(converter.convert(equalPredicate.getFrom()));
        }

        if (predicate.getClass() == NotEqualPredicate.class) {
            NotEqualPredicate notEqualPredicate = (NotEqualPredicate) predicate;
            return notEqual(converter.convert(notEqualPredicate.getFrom()));
        }

        if (predicate.getClass() == InPredicate.class) {
            InPredicate inPredicate = (InPredicate) predicate;
            Set<Object> values = new HashSet<Object>();
            for (Comparable value : inPredicate.getValues()) {
                values.add(converter.convert(value));
            }
            return values.size() == 1 ? equal(values.iterator().next()) : or(valueIterators(values.iterator()), values.size());
        }

        if (predicate.getClass() == AndPredicate.class) {
            AndPredicate andPredicate = (AndPredicate) predicate;
            Predicate[] predicates = andPredicate.getPredicates();

            if (predicates.length == 1) {
                return predicate(predicates[0], converter);
            } else {
                PeekableLongIterator[] iterators = new PeekableLongIterator[predicates.length];
                int count = 0;
                for (int i = 0; i < iterators.length; ++i) {
                    Predicate subPredicate = predicates[i];
                    if (subPredicate.getClass() != NotEqualPredicate.class) {
                        iterators[count++] = predicate(subPredicate, converter);
                    }
                }

                if (count == 0) {
                    NotEqualPredicate notEqualPredicate = (NotEqualPredicate) predicates[0];
                    PeekableLongIterator iterator = notEqual(converter.convert(notEqualPredicate.getFrom()));
                    for (int i = 1; i < predicates.length; ++i) {
                        notEqualPredicate = (NotEqualPredicate) predicates[i];
                        HackedRoaringBitmap bitmap = bitmaps.get(converter.convert(notEqualPredicate.getFrom()));
                        if (bitmap != null) {
                            iterator = exclude(iterator, bitmap);
                        }
                    }
                    return iterator;
                } else {
                    PeekableLongIterator iterator = and(iterators, count);
                    if (count != predicates.length) {
                        for (Predicate subPredicate : predicates) {
                            if (subPredicate.getClass() != NotEqualPredicate.class) {
                                continue;
                            }

                            NotEqualPredicate notEqualPredicate = (NotEqualPredicate) subPredicate;
                            HackedRoaringBitmap bitmap = bitmaps.get(converter.convert(notEqualPredicate.getFrom()));
                            if (bitmap != null) {
                                iterator = exclude(iterator, bitmap);
                            }
                        }
                    }
                    return iterator;
                }
            }
        }

        if (predicate.getClass() == OrPredicate.class) {
            OrPredicate orPredicate = (OrPredicate) predicate;
            Predicate[] predicates = orPredicate.getPredicates();

            if (predicates.length == 1) {
                return predicate(predicates[0], converter);
            } else {
                return or(predicateIterators(predicates, converter), predicates.length);
            }
        }

        throw new IllegalStateException("unexpected predicate: " + predicate);
    }

    private PeekableLongIterator equal(Object value) {
        HackedRoaringBitmap bitmap = bitmaps.get(value);
        return bitmap == null ? EmptyPeekableLongIterator.INSTANCE : bitmap.peekableLongIterator();
    }

    private PeekableLongIterator notEqual(final Object value) {
        // TODO
        // XXX: Long2ObjectHashMap is used to store the keys and entries, we
        // can't iterate it from lowest keys to highest keys as required by
        // PeekableLongIterator. For now, we are just forming or-iterator, that
        // is hugely inefficient, but may be countered by switching to some kind
        // of a sparse array or another ordered data structure.
        //
        // Good thing currently it is used only for NotEqual inside OrPredicate.

        final HackedRoaringBitmap bitmap = bitmaps.get(value);
        final PeekableLongIterator orIterator = or(valueIterators(bitmaps.keySet().iterator(), value), bitmaps.size());
        if (bitmap == null) {
            return orIterator;
        }

        return exclude(orIterator, bitmap);
    }

    private PeekableLongIterator exclude(final PeekableLongIterator baseIterator, final HackedRoaringBitmap bitmap) {
        return new PeekableLongIterator() {

            private PeekableLongIterator iterator = baseIterator;

            private long next;

            {
                advance();
            }

            @Override
            public void advanceIfNeeded(long minimum) {
                if (next >= minimum) {
                    return;
                }

                iterator.advanceIfNeeded(minimum);
                advance();
            }

            @Override
            public long peekNext() {
                return next;
            }

            @Override
            public PeekableLongIterator clone() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                return iterator != null;
            }

            @Override
            public long next() {
                assert iterator != null;
                long next = this.next;
                advance();
                return next;
            }

            private void advance() {
                while (iterator.hasNext()) {
                    long candidate = iterator.next();
                    if (!bitmap.contains(candidate)) {
                        next = candidate;
                        return;
                    }
                }

                iterator = null;
            }
        };
    }

    private Iterator<E> notEqualEntries(Predicate predicate, TypeConverter converter) {
        NotEqualPredicate notEqualPredicate = (NotEqualPredicate) predicate;
        Object value = converter.convert(notEqualPredicate.getFrom());
        final HackedRoaringBitmap bitmap = bitmaps.get(value);
        if (bitmap == null) {
            return entries.values().iterator();
        }

        return new Iterator<E>() {

            private Long2ObjectHashMap.KeyIterator iterator = entries.keySet().iterator();

            private long next;

            {
                advance();
            }

            @Override
            public boolean hasNext() {
                return iterator != null;
            }

            @Override
            public E next() {
                assert iterator != null;
                long next = this.next;
                advance();
                return entries.get(next);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private void advance() {
                while (iterator.hasNext()) {
                    long candidate = iterator.nextLong();
                    if (!bitmap.contains(candidate)) {
                        next = candidate;
                        return;
                    }
                }

                iterator = null;
            }
        };
    }

    private PeekableLongIterator or(Iterator<PeekableLongIterator> iterators, int expectedSize) {
        final PriorityQueue<PeekableLongIterator> minHeap =
                new PriorityQueue<PeekableLongIterator>(expectedSize, MIN_HEAP_COMPARATOR);

        while (iterators.hasNext()) {
            PeekableLongIterator iterator = iterators.next();
            if (iterator.hasNext()) {
                minHeap.add(iterator);
            }
        }

        return new PeekableLongIterator() {
            @Override
            public void advanceIfNeeded(long minimum) {
                PeekableLongIterator iterator;
                while ((iterator = minHeap.peek()) != null && iterator.peekNext() < minimum) {
                    minHeap.poll();
                    iterator.advanceIfNeeded(minimum);
                    if (iterator.hasNext()) {
                        assert iterator.peekNext() >= minimum;
                        minHeap.add(iterator);
                    }
                }
            }

            @Override
            public long peekNext() {
                assert !minHeap.isEmpty();
                return minHeap.peek().peekNext();
            }

            @Override
            public PeekableLongIterator clone() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                return !minHeap.isEmpty();
            }

            @Override
            public long next() {
                assert !minHeap.isEmpty();
                PeekableLongIterator resultIterator = minHeap.poll();
                assert resultIterator != null;
                long result = resultIterator.next();

                while (true) {
                    PeekableLongIterator iterator = minHeap.peek();
                    if (iterator == null) {
                        break;
                    }

                    if (iterator.peekNext() == result) {
                        minHeap.poll();
                        iterator.next();
                        if (iterator.hasNext()) {
                            minHeap.add(iterator);
                        }
                    } else {
                        break;
                    }
                }

                if (resultIterator.hasNext()) {
                    minHeap.add(resultIterator);
                }

                return result;
            }
        };
    }

    private PeekableLongIterator and(final PeekableLongIterator[] predicateIterators, final int count) {
        return new PeekableLongIterator() {

            private PeekableLongIterator[] iterators = predicateIterators;

            private long next;

            {
                advance();
            }

            @Override
            public void advanceIfNeeded(long minimum) {
                if (iterators == null) {
                    return;
                }
                for (int i = 0; i < count; i++) {
                    iterators[i].advanceIfNeeded(minimum);
                }
                advance();
            }

            @Override
            public long peekNext() {
                assert iterators != null;
                return next;
            }

            @Override
            public PeekableLongIterator clone() {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasNext() {
                return iterators != null;
            }

            @Override
            public long next() {
                assert iterators != null;
                long next = this.next;
                advance();
                return next;
            }

            private void advance() {
                assert iterators != null;
                if (!iterators[0].hasNext()) {
                    iterators = null;
                    return;
                }

                long max = iterators[0].peekNext();
                while (true) {
                    long newMax = max;
                    for (int i = 0; i < count; ++i) {
                        PeekableLongIterator iterator = iterators[i];

                        iterator.advanceIfNeeded(newMax);
                        if (!iterator.hasNext()) {
                            iterators = null;
                            return;
                        }

                        long next = iterator.next();
                        if (next != newMax) {
                            assert next > newMax;
                            newMax = next;
                        }
                    }

                    if (newMax == max) {
                        next = max;
                        return;
                    } else {
                        max = newMax;
                    }
                }
            }

        };
    }

    private Iterator<PeekableLongIterator> valueIterators(final Iterator<?> values) {
        return new Iterator<PeekableLongIterator>() {
            @Override
            public boolean hasNext() {
                return values.hasNext();
            }

            @Override
            public PeekableLongIterator next() {
                return equal(values.next());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Iterator<PeekableLongIterator> valueIterators(final Iterator<?> values, final Object excluding) {
        return new Iterator<PeekableLongIterator>() {

            private Object next;

            @Override
            public boolean hasNext() {
                while (values.hasNext()) {
                    next = values.next();
                    if (!next.equals(excluding)) {
                        return true;
                    }
                }

                return false;
            }

            @Override
            public PeekableLongIterator next() {
                return equal(next);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Iterator<PeekableLongIterator> predicateIterators(final Predicate[] predicates, final TypeConverter converter) {
        return new Iterator<PeekableLongIterator>() {
            private int current = 0;

            @Override
            public boolean hasNext() {
                return current < predicates.length;
            }

            @Override
            public PeekableLongIterator next() {
                return predicate(predicates[current++], converter);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private class EntryIterator implements Iterator<E> {

        private final LongIterator iterator;

        public EntryIterator(LongIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public E next() {
            E entry = entries.get(iterator.next());
            assert entry != null;
            return entry;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private static class EmptyPeekableLongIterator implements PeekableLongIterator {

        private static final PeekableLongIterator INSTANCE = new EmptyPeekableLongIterator();

        @Override
        public void advanceIfNeeded(long minimum) {
            // do nothing
        }

        @Override
        public long peekNext() {
            throw new NoSuchElementException();
        }

        @Override
        public PeekableLongIterator clone() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public long next() {
            throw new NoSuchElementException();
        }

    }

}
