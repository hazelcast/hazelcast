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

package com.hazelcast.map.writebehind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Write behind queue impl. backed by an array list.
 *
 * @param <T> Type of entry to be queued.
 */
class ArrayWriteBehindQueue<T> implements WriteBehindQueue<T> {

    private static final int INITIAL_CAPACITY = 1024;

    private List<T> list;

    ArrayWriteBehindQueue() {
        list = new ArrayList<T>(INITIAL_CAPACITY);
    }

    ArrayWriteBehindQueue(List<T> list) {
        if (list == null) {
            throw new NullPointerException();
        }
        this.list = list;
    }

    @Override
    public boolean offer(T entry) {
        return list.add(entry);
    }

    @Override
    public void removeFirst() {
        if (list.isEmpty()) {
            return;
        }
        REMOVE.incrementAndGet();
        list.remove(0);
    }

    @Override
    public T get(int index) {
        final int size = list.size();
        if (index >= size || index < 0) {
            return null;
        }
        return list.get(index);
    }

    @Override
    public boolean contains(T entry) {
        if (list.isEmpty() || entry == null) {
            return false;
        }
        for (T t : list) {
            if (entry.equals(t)) {
                return true;
            }
        }
        return false;
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
    public WriteBehindQueue<T> getSnapShot() {
        if (list.isEmpty()) {
            return WriteBehindQueues.emptyWriteBehindQueue();
        }
        return new ArrayWriteBehindQueue<T>(new ArrayList<T>(list));
    }

    @Override
    public void addFront(Collection<T> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        final List<T> newList = new ArrayList<T>();
        newList.addAll(collection);
        newList.addAll(list);
        list = newList;
    }

    @Override
    public void addEnd(Collection<T> collection) {
        if (collection == null || collection.isEmpty()) {
            return;
        }
        for (T e : collection) {
            offer(e);
        }
    }

    @Override
    public List<T> fetchAndRemoveAll() {
        final List<T> list = asList();
        clear();
        return list;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public List<T> asList() {
        if (list.isEmpty()) {
            Collections.emptyList();
        }
        return new ArrayList<T>(list);
    }

    @Override
    public void shrink() {
        ((ArrayList) list).trimToSize();
    }
}
