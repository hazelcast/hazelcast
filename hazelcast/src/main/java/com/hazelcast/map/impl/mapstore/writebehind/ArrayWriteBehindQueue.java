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

package com.hazelcast.map.impl.mapstore.writebehind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Write behind queue impl. backed by an array list.
 * Used when non-write-coalescing mode is on.
 * Means this implementation is used if we need to store all changes on a key.
 */
class ArrayWriteBehindQueue implements WriteBehindQueue<DelayedEntry> {

    private static final int INITIAL_CAPACITY = 16;

    protected List<DelayedEntry> list;

    ArrayWriteBehindQueue() {
        list = new ArrayList<DelayedEntry>(INITIAL_CAPACITY);
    }

    ArrayWriteBehindQueue(List<DelayedEntry> list) {
        if (list == null) {
            throw new NullPointerException();
        }
        this.list = list;
    }

    @Override
    public boolean offer(DelayedEntry entry) {
        return list.add(entry);
    }

    @Override
    public DelayedEntry get(DelayedEntry entry) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DelayedEntry getFirst() {
        if (list.isEmpty()) {
            return null;
        }
        return list.get(0);
    }

    @Override
    public void removeFirst() {
        if (list.isEmpty()) {
            return;
        }
        list.remove(0);
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public void clear() {
        list.clear();
    }

    @Override
    public WriteBehindQueue<DelayedEntry> getSnapShot() {
        if (list == null || list.isEmpty()) {
            return WriteBehindQueues.emptyWriteBehindQueue();
        }
        return new ArrayWriteBehindQueue(new ArrayList<DelayedEntry>(list));
    }

    @Override
    public void addFront(Collection<DelayedEntry> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        final List<DelayedEntry> newList = new ArrayList<DelayedEntry>();
        newList.addAll(collection);
        newList.addAll(list);
        list = newList;
    }

    @Override
    public void addEnd(Collection<DelayedEntry> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        for (DelayedEntry e : collection) {
            offer(e);
        }
    }

    @Override
    public void removeAll(Collection<DelayedEntry> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        if (list.isEmpty()) {
            return;
        }
        final int size = collection.size();
        for (int i = 0; i < size; i++) {
            list.remove(0);
        }
    }

    @Override
    public List<DelayedEntry> removeAll() {
        final List<DelayedEntry> list = asList();
        this.list.clear();
        return list;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public List<DelayedEntry> asList() {
        if (list.isEmpty()) {
            Collections.emptyList();
        }
        return new ArrayList<DelayedEntry>(list);
    }

    @Override
    public List<DelayedEntry> filterItems(long now) {
        List<DelayedEntry> delayedEntries = null;
        final List<DelayedEntry> list = this.list;
        for (DelayedEntry e : list) {
            if (delayedEntries == null) {
                delayedEntries = new ArrayList<DelayedEntry>();
            }
            if (e.getStoreTime() <= now) {
                delayedEntries.add(e);
            }
        }
        if (delayedEntries == null) {
            return Collections.emptyList();
        }
        return delayedEntries;
    }

}
