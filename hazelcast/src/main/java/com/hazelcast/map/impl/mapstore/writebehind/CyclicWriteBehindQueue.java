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
import com.hazelcast.util.MutableInteger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Write behind queue impl. backed by a {@link java.util.Deque}.
 * Used when non-write-coalescing mode is on.
 * Intended to use when we need to store all changes on a key.
 */
class CyclicWriteBehindQueue implements WriteBehindQueue<DelayedEntry> {

    protected Sequencer sequencer;
    private final Deque<DelayedEntry> deque;

    /**
     * Index to fast check existence of a key.
     * Maps: key --> number of keys.
     * <p/>
     * Used to determine whether a key is loadable from store. Because there is a possibility that a key
     * is in {@link WriteBehindQueue} but it is not in {@link com.hazelcast.core.IMap} due to the eviction.
     * At that point if one tries to get that evicted key, {@link com.hazelcast.core.MapLoader} will
     * try to load it from store and that may cause data inconsistencies.
     *
     * @see WriteBehindStore#loadable(com.hazelcast.nio.serialization.Data)
     */
    private final Map<Data, MutableInteger> index;

    public CyclicWriteBehindQueue() {
        this.deque = new ArrayDeque<DelayedEntry>();
        this.index = new HashMap<Data, MutableInteger>();
        this.sequencer = new DefaultSequencer();
    }

    @Override
    public void addFirst(Collection<DelayedEntry> collection) {
        for (DelayedEntry entry : collection) {
            deque.addFirst(entry);
        }
        addCountIndex(collection);
    }

    @Override
    public void addLast(DelayedEntry entry) {
        entry.setSequence(sequencer.incrementTail());
        deque.addLast(entry);
        addCountIndex(entry);
    }

    @Override
    public boolean removeFirstOccurrence(DelayedEntry entry) {
        DelayedEntry removedEntry = deque.pollFirst();
        if (removedEntry == null) {
            return false;
        }
        decreaseCountIndex(entry);
        sequencer.incrementHead();
        return true;
    }

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
        sequencer.init();
        deque.clear();
        resetCountIndex();
    }

    @Override
    public Sequencer getSequencer() {
        return sequencer;
    }

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
        sequencer.setHeadSequence(sequencer.tailSequence());
        return collection.size();
    }

    @Override
    public List<DelayedEntry> asList() {
        return Collections.unmodifiableList(new ArrayList<DelayedEntry>(deque));
    }

    @Override
    public void getFrontByTime(long time, Collection<DelayedEntry> collection) {
        Iterator<DelayedEntry> iterator = deque.iterator();
        while (iterator.hasNext()) {
            DelayedEntry e = iterator.next();
            if (e.getStoreTime() <= time) {
                collection.add(e);
            } else {
                break;
            }
        }
    }

    @Override
    public void getFrontByNumber(int numberOfElements, Collection<DelayedEntry> collection) {
        int count = 0;
        Iterator<DelayedEntry> iterator = deque.iterator();
        while (iterator.hasNext()) {
            DelayedEntry e = iterator.next();
            if (count == numberOfElements) {
                break;
            }
            collection.add(e);
            count++;
        }
    }

    @Override
    public void getFrontBySequence(long sequence, Collection<DelayedEntry> collection) {
        Iterator<DelayedEntry> iterator = deque.iterator();
        while (iterator.hasNext()) {
            DelayedEntry e = iterator.next();
            if (e.getSequence() < sequence) {
                collection.add(e);
            } else {
                break;
            }
        }
    }

    @Override
    public void getEndBySequence(long sequence, Collection<DelayedEntry> collection) {
        Iterator<DelayedEntry> iterator = deque.descendingIterator();
        while (iterator.hasNext()) {
            DelayedEntry e = iterator.next();
            if (e.getSequence() > sequence) {
                collection.add(e);
            } else {
                break;
            }
        }
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
