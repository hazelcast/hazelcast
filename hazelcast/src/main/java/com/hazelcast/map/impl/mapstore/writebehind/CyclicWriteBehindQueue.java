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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.internal.util.MutableInteger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.internal.serialization.Data;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Write behind queue impl. backed by a {@link java.util.Deque}.
 * Used when non-write-coalescing mode is on.
 * Intended to use when we need to store all changes on a key.
 */
class CyclicWriteBehindQueue implements WriteBehindQueue<DelayedEntry> {

    private final Deque<DelayedEntry> deque;

    /**
     * Index to fast check existence of a key.
     * Maps: key --> number of keys.
     * <p>
     * Used to determine whether a key is loadable from store. Because there is a possibility that a key
     * is in {@link WriteBehindQueue} but it is not in {@link IMap} due to the eviction.
     * At that point if one tries to get that evicted key, {@link MapLoader} will
     * try to load it from store and that may cause data inconsistencies.
     *
     * @see WriteBehindStore#loadable(Data)
     */
    private final Map<Data, MutableInteger> index;

    CyclicWriteBehindQueue() {
        this.deque = new ArrayDeque<>();
        this.index = new HashMap<>();
    }

    /**
     * Add this collection to the front of the queue.
     *
     * @param collection collection to be added in front of this queue.
     */
    @Override
    public void addFirst(Collection<DelayedEntry> collection) {
        for (DelayedEntry entry : collection) {
            deque.addFirst(entry);
        }
        addCountIndex(collection);
    }

    @Override
    public void addLast(DelayedEntry entry, boolean addWithoutCapacityCheck) {
        deque.addLast(entry);
        addCountIndex(entry);
    }

    @Override
    public DelayedEntry peek() {
        return deque.peek();
    }

    /**
     * Removes the first element of this queue instead of searching for it,
     * implementation of this method is strongly tied with {@link StoreWorker} implementation.
     *
     * @param entry element to be removed.
     * @return <code>true</code> if removed successfully, <code>false</code> otherwise
     */
    @Override
    public boolean removeFirstOccurrence(DelayedEntry entry) {
        DelayedEntry removedEntry = deque.pollFirst();
        if (removedEntry == null) {
            return false;
        }
        decreaseCountIndex(entry);
        return true;
    }

    /**
     * Checks whether an item exist in queue or not.
     *
     * @param entry item to be checked
     * @return <code>true</code> if exists, <code>false</code> otherwise
     */
    @Override
    public boolean contains(DelayedEntry entry) {
        Data key = (Data) entry.getKey();
        return index.containsKey(key);
    }

    @Override
    public int size() {
        return deque.size();
    }

    @Override
    public void clear() {
        deque.clear();
        resetCountIndex();
    }

    /**
     * Removes all available elements from this queue and adds them
     * to the given collection.
     *
     * @param collection all elements to be added to this collection.
     * @return number of removed items from this queue.
     */
    @Override
    public int drainTo(Collection<DelayedEntry> collection) {
        checkNotNull(collection, "collection can not be null");

        Iterator<DelayedEntry> iterator = deque.iterator();
        while (iterator.hasNext()) {
            DelayedEntry e = iterator.next();
            collection.add(e);
            iterator.remove();
        }
        resetCountIndex();
        return collection.size();
    }

    /**
     * Returns unmodifiable list representation of this queue.
     *
     * @return read-only list representation of this queue.
     */
    @Override
    public List<DelayedEntry> asList() {
        return Collections.unmodifiableList(new ArrayList<>(deque));
    }

    @Override
    public void filter(IPredicate<DelayedEntry> predicate, Collection<DelayedEntry> collection) {
        for (DelayedEntry e : deque) {
            if (predicate.test(e)) {
                collection.add(e);
            } else {
                break;
            }
        }
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (this.getClass().isAssignableFrom(clazz)) {
            return (T) this;
        }
        return null;
    }

    private void addCountIndex(DelayedEntry entry) {
        Data key = (Data) entry.getKey();
        Map<Data, MutableInteger> index = this.index;

        MutableInteger count = index.get(key);
        if (count == null) {
            count = new MutableInteger();
        }
        count.value++;
        index.put(key, count);
    }

    private void addCountIndex(Collection<DelayedEntry> collection) {
        for (DelayedEntry entry : collection) {
            addCountIndex(entry);
        }
    }

    private void decreaseCountIndex(DelayedEntry entry) {
        Data key = (Data) entry.getKey();
        Map<Data, MutableInteger> index = this.index;

        MutableInteger count = index.get(key);
        if (count == null) {
            return;
        }
        count.value--;

        if (count.value == 0) {
            index.remove(key);
        } else {
            index.put(key, count);
        }
    }

    private void resetCountIndex() {
        index.clear();
    }

}
