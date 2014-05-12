/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class SortedHashMap<K, V> extends AbstractMap<K, V> {
    static final int MAXIMUM_CAPACITY = 1 << 30;
    static final int DEFAULT_INITIAL_CAPACITY = 16;
    static final float DEFAULT_LOAD_FACTOR = 0.75f;

    transient volatile Set<K> keySet;
    transient volatile Collection<V> values;

    int size;
    int threshold;
    final float loadFactor;
    final OrderingType orderingType;
    int modCount;

    Entry<K, V>[] table;
    private transient Set<Map.Entry<K, V>> entrySet;

    private transient Entry<K, V> header;

    public enum OrderingType {
        NONE, LRU, LFU, HASH
    }

    public SortedHashMap() {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, OrderingType.NONE);
    }

    public SortedHashMap(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, OrderingType.NONE);
    }

    public SortedHashMap(int initialCapacity, OrderingType orderingType) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, orderingType);
    }

    public SortedHashMap(int initialCapacity, float loadFactor, OrderingType orderingType) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("Illegal initial capacity: "
                    + initialCapacity);
        }
        if (initialCapacity > MAXIMUM_CAPACITY) {
            initialCapacity = MAXIMUM_CAPACITY;
        }
        if (loadFactor <= 0 || Float.isNaN(loadFactor)) {
            throw new IllegalArgumentException("Illegal load factor: "
                    + loadFactor);
        }
        // Find a power of 2 >= initialCapacity
        int capacity = 1;
        while (capacity < initialCapacity) {
            capacity <<= 1;
        }
        this.orderingType = orderingType;
        this.loadFactor = loadFactor;
        threshold = (int) (capacity * loadFactor);
        table = new Entry[capacity];
        header = new Entry<K, V>(-1, null, null, null);
        header.before = header.after = header;
    }

    public V put(K key, V value) {
        int hash = hash(key.hashCode());
        int i = indexFor(hash, table.length);
        for (Entry<K, V> e = table[i]; e != null; e = e.next) {
            Object k;
            if (e.hash == hash && ((k = e.key) == key || key.equals(k))) {
                V oldValue = e.value;
                e.value = value;
                e.recordAccess(this);
                return oldValue;
            }
        }
        modCount++;
        addEntry(hash, key, value, i);
        return null;
    }

    public static void touch(SortedHashMap linkedMap, Object key, OrderingType orderingType) {
        Entry e = linkedMap.getEntry(key);
        if (e != null) {
            e.touch(linkedMap, orderingType);
        }
    }

    public static void moveToTop(SortedHashMap linkedMap, Object key) {
        Entry e = linkedMap.getEntry(key);
        if (e != null) {
            e.moveToTop(linkedMap);
        }
    }

    public static OrderingType getOrderingTypeByName(String orderingType) {
        return OrderingType.valueOf(orderingType.toUpperCase());
    }

    public boolean containsKey(Object key) {
        return getEntry(key) != null;
    }

    public V get(Object key) {
        Entry<K, V> e = getEntry(key);
        if (e == null) {
            return null;
        }
        e.recordAccess(this);
        return e.value;
    }

    Entry<K, V> getEntry(Object k) {
        int hash = hash(k.hashCode());
        int i = indexFor(hash, table.length);
        Entry<K, V> e = table[i];
        while (e != null && !(e.hash == hash && eq(k, e.key))) {
            e = e.next;
        }
        return e;
    }

    static boolean eq(Object x, Object y) {
        return x == y || x.equals(y);
    }

    public void clear() {
        modCount++;
        Entry[] tab = table;
        for (int i = 0; i < tab.length; i++) {
            tab[i] = null;
        }
        size = 0;
        header.before = header.after = header;
    }

    public V remove(Object key) {
        Entry<K, V> e = removeEntryForKey(key);
        return (e == null ? null : e.value);
    }

    Entry<K, V> removeEntryForKey(Object k) {
        int hash = hash(k.hashCode());
        int i = indexFor(hash, table.length);
        Entry<K, V> prev = table[i];
        Entry<K, V> e = prev;
        while (e != null) {
            Entry<K, V> next = e.next;
            if ((e.hash == hash) && (k == e.key || k.equals(e.key))) {
                modCount++;
                size--;
                if (prev == e) {
                    table[i] = next;
                } else {
                    prev.next = next;
                }
                e.recordRemoval(this);
                return e;
            }
            prev = e;
            e = next;
        }
        return e;
    }

    Entry<K, V> removeMapping(Object o) {
        if (!(o instanceof Map.Entry)) {
            return null;
        }
        Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
        Object k = entry.getKey();
        int hash = hash(k.hashCode());
        int i = indexFor(hash, table.length);
        Entry<K, V> prev = table[i];
        Entry<K, V> e = prev;
        while (e != null) {
            Entry<K, V> next = e.next;
            if (e.hash == hash && e.equals(entry)) {
                modCount++;
                size--;
                if (prev == e) {
                    table[i] = next;
                } else {
                    prev.next = next;
                }
                e.recordRemoval(this);
                return e;
            }
            prev = e;
            e = next;
        }
        return e;
    }

    void addEntry(int hash, K key, V value, int bucketIndex) {
        createEntry(hash, key, value, bucketIndex);
        Entry<K, V> eldest = header.after;
        if (removeEldestEntry(eldest)) {
            removeEntryForKey(eldest.key);
        } else {
            if (size >= threshold) {
                resize(2 * table.length);
            }
        }
    }

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return false;
    }

    void createEntry(int hash, K key, V value, int bucketIndex) {
        Entry<K, V> old = table[bucketIndex];
        Entry<K, V> e = new Entry<K, V>(hash, key, value, old);
        table[bucketIndex] = e;
        e.addBefore(header);
        size++;
    }

    void resize(int newCapacity) {
        Entry[] oldTable = table;
        int oldCapacity = oldTable.length;
        if (oldCapacity == MAXIMUM_CAPACITY) {
            threshold = Integer.MAX_VALUE;
            return;
        }
        Entry[] newTable = new Entry[newCapacity];
        transfer(newTable);
        table = newTable;
        threshold = (int) (newCapacity * loadFactor);
    }

    void transfer(Entry[] newTable) {
        int newCapacity = newTable.length;
        for (Entry<K, V> e = header.after; e != header; e = e.after) {
            int index = indexFor(e.hash, newCapacity);
            e.next = newTable[index];
            newTable[index] = e;
        }
    }

    static int hash(int h) {
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    static int indexFor(int h, int length) {
        return h & (length - 1);
    }

    public boolean containsValue(Object value) {
        // Overridden to take advantage of faster iterator
        if (value == null) {
            for (Entry<K, V> e = header.after; e != header; e = e.after) {
                if (e.value == null) {
                    return true;
                }
            }
        } else {
            for (Entry<K, V> e = header.after; e != header; e = e.after) {
                if (value.equals(e.value)) {
                    return true;
                }
            }
        }
        return false;
    }

    static class Entry<K, V> implements Map.Entry<K, V> {
        K key;
        V value;
        Entry<K, V> next;
        Entry<K, V> after;
        Entry<K, V> before;
        long accessCount = 1;
        int hash = -1;

        Entry(int hash, K key, V value, Entry<K, V> next) {
            this.key = key;
            this.value = value;
            this.hash = hash;
            this.next = next;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        public V setValue(V value) {
            this.value = value;
            return value;
        }

        public boolean equals(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry e = (Map.Entry) o;
            Object k1 = getKey();
            Object k2 = e.getKey();
            if (k1 == k2 || (k1 != null && k1.equals(k2))) {
                Object v1 = getValue();
                Object v2 = e.getValue();
                if (v1 == v2 || (v1 != null && v1.equals(v2))) {
                    return true;
                }
            }
            return false;
        }

        public int hashCode() {
            return key.hashCode();
        }

        public String toString() {
            return "Entry key=" + getKey() + ", value=" + getValue();
        }

        private void remove() {
            before.after = after;
            after.before = before;
        }

        private void addBefore(Entry<K, V> existingEntry) {
            after = existingEntry;
            before = existingEntry.before;
            before.after = this;
            after.before = this;
        }

        private void addAfter(Entry<K, V> existingEntry) {
            addBefore(existingEntry.after);
        }

        /**
         * This method is invoked by the superclass whenever the value
         * of a pre-existing entry is read by Map.get or modified by Map.set.
         * If the enclosing Map is access-ordered, it moves the entry
         * to the end of the list; otherwise, it does nothing.
         *
         * @param lm
         */
        void recordAccess(SortedHashMap<K, V> lm) {
            touch(lm, lm.orderingType);
        }

        void touch(SortedHashMap<K, V> lm, OrderingType orderingType) {
            if (orderingType != OrderingType.NONE) {
                accessCount++;
                lm.modCount++;
                if (orderingType == OrderingType.LFU) {
                    moveLFU(lm);
                } else if (orderingType == OrderingType.LRU) {
                    moveLRU(lm);
                } else if (orderingType == OrderingType.HASH) {
                    moveHash(lm);
                } else {
                    throw new RuntimeException("Unknown orderingType:" + lm.orderingType);
                }
            }
        }

        void moveLRU(SortedHashMap lm) {
            remove();
            addBefore(lm.header);
        }

        void moveLFU(SortedHashMap lm) {
            Entry<K, V> nextOne = after;
            boolean shouldMove = false;
            while (nextOne != null && accessCount >= nextOne.accessCount && nextOne != lm.header) {
                shouldMove = true;
                nextOne = nextOne.after;
            }
            if (shouldMove) {
                remove();
                addBefore(nextOne);
            }
        }

        void moveHash(SortedHashMap lm) {
            Entry<K, V> nextOne = after;
            boolean shouldMove = false;
            while (nextOne != null && nextOne != lm.header && value.hashCode() >= nextOne.value.hashCode()) {
                shouldMove = true;
                nextOne = nextOne.after;
            }
            if (shouldMove) {
                remove();
                addBefore(nextOne);
            }
        }

        void moveToTop(SortedHashMap lm) {
            remove();
            addAfter(lm.header);
        }

        void recordRemoval(SortedHashMap<K, V> lm) {
            remove();
        }
    }

    private abstract class LinkedHashIterator<T> implements Iterator<T> {
        Entry<K, V> nextEntry = header.after;
        Entry<K, V> lastReturned;

        int expectedModCount = modCount;

        public boolean hasNext() {
            return nextEntry != header;
        }

        public void remove() {
            if (lastReturned == null) {
                throw new IllegalStateException();
            }
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            SortedHashMap.this.remove(lastReturned.key);
            lastReturned = null;
            expectedModCount = modCount;
        }

        Entry<K, V> nextEntry() {
            if (modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
            if (nextEntry == header) {
                throw new NoSuchElementException();
            }
            Entry<K, V> e = lastReturned = nextEntry;
            nextEntry = e.after;
            return e;
        }
    }

    private class KeyIterator extends LinkedHashIterator<K> {
        public K next() {
            return nextEntry().getKey();
        }
    }

    private class ValueIterator extends LinkedHashIterator<V> {
        public V next() {
            return nextEntry().value;
        }
    }

    private class EntryIterator extends LinkedHashIterator<Map.Entry<K, V>> {
        public Map.Entry<K, V> next() {
            return nextEntry();
        }
    }
    // These Overrides alter the behavior of superclass view iterator() methods

    Iterator<K> newKeyIterator() {
        return new KeyIterator();
    }

    Iterator<V> newValueIterator() {
        return new ValueIterator();
    }

    Iterator<Map.Entry<K, V>> newEntryIterator() {
        return new EntryIterator();
    }


    public Set<K> keySet() {
        Set<K> ks = keySet;
        return (ks != null ? ks : (keySet = new KeySet()));
    }

    private class KeySet extends AbstractSet<K> {
        public Iterator<K> iterator() {
            return newKeyIterator();
        }

        public int size() {
            return size;
        }

        public boolean contains(Object o) {
            return containsKey(o);
        }

        public boolean remove(Object o) {
            return SortedHashMap.this.removeEntryForKey(o) != null;
        }

        public void clear() {
            SortedHashMap.this.clear();
        }
    }

    public Collection<V> values() {
        Collection<V> vs = values;
        return (vs != null ? vs : (values = new Values()));
    }

    private class Values extends AbstractCollection<V> {
        public Iterator<V> iterator() {
            return newValueIterator();
        }

        public int size() {
            return size;
        }

        public boolean contains(Object o) {
            return containsValue(o);
        }

        public void clear() {
            SortedHashMap.this.clear();
        }
    }

    public Set<Map.Entry<K, V>> entrySet() {
        Set<Map.Entry<K, V>> es = entrySet;
        return (es != null ? es : (entrySet = (Set<Map.Entry<K, V>>) new EntrySet()));
    }

    private class EntrySet extends AbstractSet {
        public Iterator iterator() {
            return newEntryIterator();
        }

        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }
            Map.Entry<K, V> e = (Map.Entry<K, V>) o;
            Entry<K, V> candidate = getEntry(e.getKey());
            return candidate != null && candidate.equals(e);
        }

        public boolean remove(Object o) {
            return removeMapping(o) != null;
        }

        public int size() {
            return size;
        }

        public void clear() {
            SortedHashMap.this.clear();
        }
    }
}
