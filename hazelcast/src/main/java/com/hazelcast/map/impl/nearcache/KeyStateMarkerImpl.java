/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.nio.serialization.Data;

import java.util.concurrent.atomic.AtomicIntegerArray;

import static com.hazelcast.map.impl.nearcache.KeyStateMarker.STATE.MARKED;
import static com.hazelcast.map.impl.nearcache.KeyStateMarker.STATE.REMOVED;
import static com.hazelcast.map.impl.nearcache.KeyStateMarker.STATE.UNMARKED;
import static com.hazelcast.util.HashUtil.hashToIndex;

public class KeyStateMarkerImpl implements KeyStateMarker {

    private final AtomicIntegerArray marks;

    public KeyStateMarkerImpl(int markerCount) {
        this.marks = new AtomicIntegerArray(markerCount);
    }

    @Override
    public boolean markIfUnmarked(Object key) {
        return casState(key, UNMARKED, MARKED);
    }

    @Override
    public boolean unmarkIfMarked(Object key) {
        return casState(key, MARKED, UNMARKED);
    }

    @Override
    public boolean removeIfMarked(Object key) {
        return casState(key, MARKED, REMOVED);
    }

    @Override
    public void unmarkForcibly(Object key) {
        int slot = getSlot(key);
        marks.set(slot, UNMARKED.getState());
    }

    @Override
    public void unmarkAllForcibly() {
        int slot = 0;
        do {
            marks.set(slot, UNMARKED.getState());
        } while (++slot < marks.length());
    }

    private boolean casState(Object key, STATE expect, STATE update) {
        int slot = getSlot(key);
        return marks.compareAndSet(slot, expect.getState(), update.getState());
    }

    private int getSlot(Object key) {
        int hash = key instanceof Data ? ((Data) key).getPartitionHash() : key.hashCode();
        return hashToIndex(hash, marks.length());
    }

    // only used for testing purposes
    public AtomicIntegerArray getMarks() {
        return marks;
    }

    @Override
    public String toString() {
        return "KeyStateMarkerImpl{"
                + "markerCount=" + marks.length()
                + ", marks=" + marks
                + '}';
    }
}
