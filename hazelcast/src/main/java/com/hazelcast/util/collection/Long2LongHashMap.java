/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util.collection;

import com.hazelcast.util.QuickMath;
import com.hazelcast.util.function.BiConsumer;
import com.hazelcast.util.function.LongLongConsumer;
import com.hazelcast.util.function.Predicate;
import com.hazelcast.util.function.Supplier;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * A Probing hashmap specialised for long key and value pairs.
 */
public class Long2LongHashMap implements Map<Long, Long> {
    private final Set<Long> keySet;
    private final LongIterator valueIterator = new LongIterator(1);
    private final Collection<Long> values;
    private final Set<Entry<Long, Long>> entrySet;

    private final double loadFactor;
    private final long missingValue;

    private long[] entries;
    private int capacity;
    private int mask;
    private int resizeThreshold;
    private int size;

    public Long2LongHashMap(final long missingValue) {
        this(16, 0.6, missingValue);
    }

    @SuppressWarnings("unchecked")
    public Long2LongHashMap(final int initialCapacity, final double loadFactor, final long missingValue) {
        this.loadFactor = loadFactor;
        this.missingValue = missingValue;
        capacity(QuickMath.nextPowerOfTwo(initialCapacity));
        final LongIterator keyIterator = new LongIterator(0);
        keySet = new MapDelegatingSet<Long>(this, new IteratorSupplier(keyIterator), new Predicate() {
            @Override public boolean test(Object value) {
                return containsValue(value);
            }
        });
        values = new MapDelegatingSet<Long>(this, new Supplier<Iterator<Long>>() {
            @Override public Iterator<Long> get() {
                return valueIterator.reset();
            }
        }, new Predicate() {
            @Override
            public boolean test(Object key) {
                return containsKey(key);
            }
        });
        final EntryIterator entryIterator = new EntryIterator();
        entrySet = new MapDelegatingSet<Entry<Long, Long>>(this, new EntryIteratorSupplier(entryIterator), new
                Predicate() {
            @Override
            public boolean test(Object e) {
                return Long2LongHashMap.this.containsKey(((Entry<Long, Long>) e).getKey());
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    public int size() {
        return size;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    public long get(final long key) {
        final long[] entries = this.entries;
        int index = hash(key);
        long candidateKey;
        while ((candidateKey = entries[index]) != missingValue) {
            if (candidateKey == key) {
                return entries[index + 1];
            }
            index = next(index);
        }
        return missingValue;
    }

    public long put(final long key, final long value) {
        long oldValue = missingValue;
        int index = hash(key);
        long candidateKey;
        while ((candidateKey = entries[index]) != missingValue) {
            if (candidateKey == key) {
                oldValue = entries[index + 1];
                break;
            }
            index = next(index);
        }
        if (oldValue == missingValue) {
            ++size;
            entries[index] = key;
        }
        entries[index + 1] = value;
        checkResize();
        return oldValue;
    }

    private void checkResize() {
        if (size > resizeThreshold) {
            final int newCapacity = capacity << 1;
            if (newCapacity < 0) {
                throw new IllegalStateException("Max capacity reached at size=" + size);
            }
            rehash(newCapacity);
        }
    }

    private void rehash(final int newCapacity) {
        final long[] oldEntries = entries;
        capacity(newCapacity);
        for (int i = 0; i < oldEntries.length; i += 2) {
            final long key = oldEntries[i];
            if (key != missingValue) {
                put(key, oldEntries[i + 1]);
            }
        }
    }

    private int hash(final long key) {
        int hash = (int) key ^ (int) (key >>> 32);
        hash = (hash << 1) - (hash << 8);
        return hash & mask;
    }

    /**
     * Primitive specialised forEach implementation.
     * <p/>
     * NB: Renamed from forEach to avoid overloading on parameter types of lambda
     * expression, which doesn't interplay well with type inference in lambda expressions.
     *
     * @param consumer a callback called for each key/value pair in the map.
     */
    public void longForEach(final LongLongConsumer consumer) {
        final long[] entries = this.entries;
        for (int i = 0; i < entries.length; i += 2) {
            final long key = entries[i];
            if (key != missingValue) {
                consumer.accept(entries[i], entries[i + 1]);
            }
        }
    }

    /**
     * Long primitive specialised containsKey.
     *
     * @param key the key to check.
     * @return true if the map contains key as a key, false otherwise.
     */
    public boolean containsKey(final long key) {
        return get(key) != missingValue;
    }

    public boolean containsValue(final long value) {
        final long[] entries = this.entries;
        for (int i = 1; i < entries.length; i += 2) {
            final long entryValue = entries[i];
            if (entryValue == value) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public void clear() {
        Arrays.fill(entries, missingValue);
        size = 0;
    }

    // ---------------- Boxed Versions Below ----------------

    /**
     * {@inheritDoc}
     */
    public Long get(final Object key) {
        return get((long) (Long) key);
    }

    /**
     * {@inheritDoc}
     */
    public Long put(final Long key, final Long value) {
        return put(key.longValue(), value.longValue());
    }

    /**
     * {@inheritDoc}
     */
    public void forEach(final BiConsumer<? super Long, ? super Long> action) {
        longForEach(new UnboxingBiConsumer(action));
    }

    /**
     * {@inheritDoc}
     */
    public boolean containsKey(final Object key) {
        return containsKey((long) (Long) key);
    }

    /**
     * {@inheritDoc}
     */
    public boolean containsValue(final Object value) {
        return containsValue((long) (Long) value);
    }

    /**
     * {@inheritDoc}
     */
    public void putAll(final Map<? extends Long, ? extends Long> map) {
        for (final Entry<? extends Long, ? extends Long> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * {@inheritDoc}
     */
    public Set<Long> keySet() {
        return keySet;
    }

    /**
     * {@inheritDoc}
     */
    public Collection<Long> values() {
        return values;
    }

    /**
     * {@inheritDoc}
     */
    public Set<Entry<Long, Long>> entrySet() {
        return entrySet;
    }

    /**
     * {@inheritDoc}
     */
    public Long remove(final Object key) {
        return remove((long) (Long) key);
    }

    public long remove(final long key) {
        final long[] entries = this.entries;
        int index = hash(key);
        long candidateKey;
        while ((candidateKey = entries[index]) != missingValue) {
            if (candidateKey == key) {
                final int valueIndex = index + 1;
                final long oldValue = entries[valueIndex];
                entries[index] = missingValue;
                entries[valueIndex] = missingValue;
                size--;
                compactChain(index);
                return oldValue;
            }
            index = next(index);
        }
        return missingValue;
    }

    private void compactChain(int deleteIndex) {
        final long[] entries = this.entries;
        int index = deleteIndex;
        while (true) {
            index = next(index);
            if (entries[index] == missingValue) {
                return;
            }
            final int hash = hash(entries[index]);
            if ((index < hash && (hash <= deleteIndex || deleteIndex <= index))
                    || (hash <= deleteIndex && deleteIndex <= index)) {
                entries[deleteIndex] = entries[index];
                entries[deleteIndex + 1] = entries[index + 1];
                entries[index] = missingValue;
                entries[index + 1] = missingValue;
                deleteIndex = index;
            }
        }
    }

    public long minValue() {
        long min = Long.MAX_VALUE;
        final LongIterator iterator = valueIterator.reset();
        while (iterator.hasNext()) {
            min = Math.min(min, iterator.nextValue());
        }
        return min;
    }

    private static class IteratorSupplier implements Supplier<Iterator<Long>> {
        private final LongIterator keyIterator;

        public IteratorSupplier(LongIterator keyIterator) {
            this.keyIterator = keyIterator;
        }

        @Override
        public Iterator<Long> get() {
            return keyIterator.reset();
        }
    }

    private static class EntryIteratorSupplier implements Supplier<Iterator<Entry<Long, Long>>> {
        private final EntryIterator entryIterator;

        public EntryIteratorSupplier(EntryIterator entryIterator) {
            this.entryIterator = entryIterator;
        }

        @Override
        public Iterator<Entry<Long, Long>> get() {
            return entryIterator.reset();
        }
    }

    private static class UnboxingBiConsumer implements LongLongConsumer {
        private final BiConsumer<? super Long, ? super Long> action;

        public UnboxingBiConsumer(BiConsumer<? super Long, ? super Long> action) {
            this.action = action;
        }

        @Override
        public void accept(long t, long u) {
            action.accept(t, u);
        }
    }

    // ---------------- Utility Classes ----------------

    private abstract class AbstractIterator {
        protected final int startIndex;

        protected int index;

        protected AbstractIterator(final int startIndex) {
            this.startIndex = startIndex;
            index = startIndex;
        }

        public boolean hasNext() {
            while (entries[index] == missingValue) {
                nextIndex();
                if (index == startIndex) {
                    return false;
                }
            }
            return true;
        }

        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        protected void nextIndex() {
            index = next(index);
        }
    }

    private final class LongIterator extends AbstractIterator implements Iterator<Long> {
        private LongIterator(final int startIndex) {
            super(startIndex);
        }

        private LongIterator reset() {
            index = startIndex;
            return this;
        }

        public Long next() {
            return nextValue();
        }

        public long nextValue() {
            final long entry = entries[index];
            nextIndex();
            return entry;
        }
    }

    private final class EntryIterator extends AbstractIterator implements Iterator<Entry<Long, Long>>, Entry<Long,
            Long> {
        private long key;
        private long value;

        private EntryIterator() {
            super(0);
        }

        private EntryIterator reset() {
            index = startIndex;
            return this;
        }

        public Long getKey() {
            return key;
        }

        public Long getValue() {
            return value;
        }

        public Long setValue(final Long value) {
            throw new UnsupportedOperationException();
        }

        public Entry<Long, Long> next() {
            key = entries[index];
            value = entries[index + 1];
            nextIndex();
            return this;
        }
    }

    private int next(final int index) {
        return (index + 2) & mask;
    }

    private void capacity(final int newCapacity) {
        capacity = newCapacity;
        resizeThreshold = (int) (newCapacity * loadFactor);
        mask = (newCapacity * 2) - 1;
        entries = new long[newCapacity * 2];
        Arrays.fill(entries, missingValue);
    }
}
