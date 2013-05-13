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

import com.hazelcast.map.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.query.impl.QueryResultEntryImpl;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class QueryDataResultStream extends AbstractSet<QueryResultEntry> implements IdentifiedDataSerializable {
    final BlockingQueue<QueryResultEntry> q = new LinkedBlockingQueue<QueryResultEntry>();
    volatile int size = 0;
    boolean ended = false; // guarded by endLock
    final Object endLock = new Object();
    private static final QueryResultEntry END = new QueryResultEntryImpl(null, null, null);

    private boolean set;
    private Set<Object> keys;
    private IterationType iterationType;

    public QueryDataResultStream() {
    }

    public QueryDataResultStream(IterationType iterationType) {
        this(iterationType, (iterationType != IterationType.VALUE));
    }

    public QueryDataResultStream(IterationType iterationType, boolean set) {
        this.set = set;
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
                return valueData;
            } else if (iterationType == IterationType.KEY) {
                Data keyData = currentEntry.getKeyData();
                return keyData;
            } else {
                Data keyData = currentEntry.getKeyData();
                Data valueData = currentEntry.getValueData();
                return new AbstractMap.SimpleImmutableEntry(keyData, valueData);
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


    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.QUERY_RESULT_STREAM;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(set);
        out.writeUTF(iterationType.name());
        out.writeInt(size);
        for (QueryResultEntry queryResultEntry : q) {
            out.writeObject(queryResultEntry);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        set = in.readBoolean();
        iterationType = IterationType.valueOf(in.readUTF());
        size = in.readInt();
        for (int i = 0; i < size; i++) {
            q.add((QueryResultEntry) in.readObject());
        }
    }
}
