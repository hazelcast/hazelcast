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

import com.hazelcast.util.collection.Long2ObjectHashMap;
import org.roaringbitmap.longlong.HackedRoaringBitmap;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.PeekableLongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.util.Comparator;
import java.util.HashMap;
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

    public Iterator<E> getHaving(Object value) {
        return new EntriesIterator(equal(value));
    }

    public Iterator<E> getHavingAnyOf(Set<?> values) {
        return new EntriesIterator(or(equalIterators(values.iterator()), values.size()));
    }

    private PeekableLongIterator equal(Object value) {
        HackedRoaringBitmap bitmap = bitmaps.get(value);
        return bitmap == null ? EmptyPeekableLongIterator.INSTANCE : bitmap.peekableLongIterator();
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

    private Iterator<PeekableLongIterator> equalIterators(final Iterator<?> values) {
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

    private class EntriesIterator implements Iterator<E> {

        private final LongIterator iterator;

        public EntriesIterator(LongIterator iterator) {
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
