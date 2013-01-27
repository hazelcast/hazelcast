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

import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueryResultStream<T> extends AbstractSet<QueryableEntry> {
    final BlockingQueue<QueryableEntry> q = new LinkedBlockingQueue<QueryableEntry>();
    volatile int size = 0;
    boolean ended = false; // guarded by endLock
    final Object endLock = new Object();
    private static final QueryableEntry END = new QueryEntry(null, "1", "1", "1");

    private final boolean set;
    private final Set<Object> keys;
    private final boolean data;
    private final IterationType iterationType;

    public QueryResultStream(QueryResultStream.IterationType iterationType, boolean data) {
        this(iterationType, data, (iterationType != IterationType.VALUE));
    }

    public QueryResultStream(QueryResultStream.IterationType iterationType, boolean data, boolean set) {
        this.set = set;
        this.data = data;
        this.iterationType = iterationType;
        if (set) {
            keys = new HashSet<Object>();
        } else {
            keys = null;
        }
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

    public synchronized boolean add(QueryableEntry entry) {
        if (!set || keys.add(entry.getIndexKey())) {
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

        QueryableEntry currentEntry;

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
                return (data) ? currentEntry.getValueData() : currentEntry.getValue();
            } else if (iterationType == IterationType.KEY) {
                return (data) ? currentEntry.getKeyData() : currentEntry.getKey();
            } else return currentEntry;
        }

        public void remove() {
            throw new UnsupportedOperationException();
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
