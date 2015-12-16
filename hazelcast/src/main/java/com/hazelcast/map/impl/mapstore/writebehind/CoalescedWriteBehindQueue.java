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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.map.impl.mapstore.writebehind.entry.DelayedEntry;
import com.hazelcast.nio.serialization.Data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.CollectionUtil.isEmpty;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A write-behind queue which supports write coalescing.
 */
class CoalescedWriteBehindQueue implements WriteBehindQueue<DelayedEntry> {

    protected Map<Data, DelayedEntry> map;

    CoalescedWriteBehindQueue() {
        map = new LinkedHashMap<Data, DelayedEntry>();
    }

    @Override
    public void addFirst(Collection<DelayedEntry> collection) {
        if (isEmpty(collection)) {
            return;
        }
        int expectedCapacity = map.size() + collection.size();
        Map<Data, DelayedEntry> newMap = createMapWithExpectedCapacity(expectedCapacity);
        Iterator<DelayedEntry> iterator = collection.iterator();
        while (iterator.hasNext()) {
            DelayedEntry next = iterator.next();
            newMap.put((Data) next.getKey(), next);
        }
        newMap.putAll(map);
        map = newMap;
    }

    @Override
    public void addLast(DelayedEntry delayedEntry) {
        if (delayedEntry == null) {
            return;
        }
        calculateStoreTime(delayedEntry);
        Data key = (Data) delayedEntry.getKey();
        map.put(key, delayedEntry);
    }

    /**
     * Removes the first occurrence of the specified element in this queue
     * when searching it by starting from the head of this queue.
     *
     * @param incoming element to be removed.
     * @return <code>true</code> if removed successfully, <code>false</code> otherwise
     */
    @Override
    public boolean removeFirstOccurrence(DelayedEntry incoming) {
        Data incomingKey = (Data) incoming.getKey();
        Object incomingValue = incoming.getValue();

        DelayedEntry current = map.get(incomingKey);
        if (current == null) {
            return false;
        }

        Object currentValue = current.getValue();
        if (incomingValue == null && currentValue == null
                || incomingValue != null && currentValue != null && incomingValue.equals(currentValue)) {
            map.remove(incomingKey);
            return true;
        }

        return false;
    }

    @Override
    public boolean contains(DelayedEntry entry) {
        //noinspection SuspiciousMethodCalls
        return map.containsKey(entry.getKey());
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public int drainTo(Collection<DelayedEntry> collection) {
        checkNotNull(collection, "collection can not be null");

        Collection<DelayedEntry> delayedEntries = map.values();
        for (DelayedEntry delayedEntry : delayedEntries) {
            collection.add(delayedEntry);
        }
        map.clear();
        return collection.size();
    }

    @Override
    public List<DelayedEntry> asList() {
        Collection<DelayedEntry> values = map.values();
        return Collections.unmodifiableList(new ArrayList<DelayedEntry>(values));
    }


    @Override
    public void getFrontByTime(long time, Collection<DelayedEntry> collection) {
        Collection<DelayedEntry> values = map.values();
        for (DelayedEntry e : values) {
            if (e.getStoreTime() <= time) {
                collection.add(e);
            }
        }
    }

    @Override
    public void getFrontByNumber(int numberOfElements, Collection<DelayedEntry> collection) {
        int count = 0;
        Collection<DelayedEntry> values = map.values();
        for (DelayedEntry e : values) {
            if (count == numberOfElements) {
                break;
            }
            collection.add(e);
            count++;
        }
    }


    /**
     * If this is an existing key in this queue, use previously set store time;
     * since we do not want to shift store time of an existing key on every update.
     */
    private void calculateStoreTime(DelayedEntry delayedEntry) {
        Data key = (Data) delayedEntry.getKey();
        DelayedEntry currentEntry = map.get(key);
        if (currentEntry != null) {
            long currentStoreTime = currentEntry.getStoreTime();
            delayedEntry.setStoreTime(currentStoreTime);
        }
    }

    private static <K, V> Map<K, V> createMapWithExpectedCapacity(int expectedCapacity) {
        final double defaultLoadFactor = 0.75;
        int initialCapacity = (int) (expectedCapacity / defaultLoadFactor) + 1;
        return new LinkedHashMap<K, V>(initialCapacity);
    }

}
