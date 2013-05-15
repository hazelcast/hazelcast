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

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.query.impl.QueryResultEntryImpl;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueryResultStream extends AbstractSet<QueryResultEntry> {
    final BlockingQueue<QueryResultEntry> q = new LinkedBlockingQueue<QueryResultEntry>();
    volatile int size = 0;
    boolean ended = false; // guarded by endLock
    final Object endLock = new Object();
    private static final QueryResultEntry END = new QueryResultEntryImpl(null, null, null);

    private final SerializationService serializationService;
    private final boolean set;
    private final Set<Object> keys;
    private final boolean data;
    private final IterationType iterationType;

    public QueryResultStream(SerializationService serializationService, IterationType iterationType, boolean data) {
        this(serializationService, iterationType, data, (iterationType != IterationType.VALUE));
    }

    public QueryResultStream(SerializationService serializationService, IterationType iterationType, boolean data, boolean set) {
        this.serializationService = serializationService;
        this.set = set;
        this.data = data;
        this.iterationType = iterationType;
        if (set) {
            keys = new HashSet<Object>();
        } else {
            keys = null;
        }
    }

    public void end() {
        q.offer(END);
        synchronized (endLock) {
            ended = true;
        }
    }

    public synchronized boolean add(QueryResultEntry entry) {
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

        QueryResultEntry currentEntry;

        public boolean hasNext() {
            QueryResultEntry entry;
            try {
                entry = q.peek();
            } catch (Exception e) {
                return false;
            }
            return entry != END && entry != null;
        }

        public Object next() {
            currentEntry = q.poll();
            if(currentEntry == null || currentEntry == END)
                return null;
            if (iterationType == IterationType.VALUE) {
                Data valueData = currentEntry.getValueData();
                return (data) ? valueData : serializationService.toObject(valueData);
            } else if (iterationType == IterationType.KEY) {
                Data keyData = currentEntry.getKeyData();
                return (data) ? keyData : serializationService.toObject(keyData);
            } else {
                Data keyData = currentEntry.getKeyData();
                Data valueData = currentEntry.getValueData();
                return (data) ? new AbstractMap.SimpleImmutableEntry(keyData, valueData) : new AbstractMap.SimpleImmutableEntry(serializationService.toObject(keyData), serializationService.toObject(valueData));
            }
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
