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

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class StreamSet extends AbstractSet {
    final Set keys = new HashSet();
    final BlockingQueue<Map.Entry> q = new LinkedBlockingQueue<Map.Entry>();
    volatile int size = 0;
    boolean ended = false; // guarded by endLock
    final Object endLock = new Object();

    private static final End END = new End();

    private final IterationType iterationType;

    public StreamSet(IterationType iterationType) {
        this.iterationType = iterationType;
    }

    public enum IterationType {
        KEY, VALUE, ENTRY
    }

    public void end() {
        q.offer(END);
        synchronized (endLock) {
            ended = true;
        }
    }

    public synchronized boolean add(Map.Entry entry) {
        if (keys.add(entry)) {
            q.offer(entry);
            size++;
            return true;
        }
        return false;
    }

    @Override
    public Iterator iterator() {
        return new It();
    }

    class It implements Iterator {

        Map.Entry currentEntry;

        public boolean hasNext() {
            try {
                currentEntry = q.take();
            } catch (InterruptedException e) {
                return false;
            }
            return currentEntry != END;
        }

        public Object next() {
            if (iterationType == IterationType.VALUE) {
                return currentEntry.getValue();
            } else if (iterationType == IterationType.KEY) {
                return currentEntry.getKey();
            } else return currentEntry;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class End implements Map.Entry {
        public Object getKey() {
            return null;
        }

        public Object getValue() {
            return null;
        }

        public Object setValue(Object value) {
            return null;
        }
    }

    @Override
    public int size() {
        synchronized (endLock) {
            while (!ended) {
                try {
                    endLock.wait();
                } catch (InterruptedException e) {
                    return size;
                }
            }
            return size;
        }
    }
}
