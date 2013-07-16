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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class QueryResultSet extends AbstractSet implements IdentifiedDataSerializable {

    private transient final SerializationService serializationService;

    private final Set<QueryResultEntry> entries = Collections.newSetFromMap(new ConcurrentHashMap<QueryResultEntry, Boolean>());
    private IterationType iterationType;
    private boolean data;

    public QueryResultSet() {
        serializationService = null;
    }

    public QueryResultSet(SerializationService serializationService, IterationType iterationType, boolean data) {
        this.serializationService = serializationService;
        this.data = data;
        this.iterationType = iterationType;
    }

    public boolean add(QueryResultEntry entry) {
        return entries.add(entry);
    }

    public boolean add(Object entry) {
        return entries.add((QueryResultEntry) entry);
    }

    public Iterator iterator() {
        return new QueryResultIterator();
    }

    private class QueryResultIterator implements Iterator {

        final Iterator<QueryResultEntry> iter = entries.iterator();

        public boolean hasNext() {
            return iter.hasNext();
        }

        public Object next() {
            QueryResultEntry entry = iter.next();
            if (iterationType == IterationType.VALUE) {
                Data valueData = entry.getValueData();
                return (data) ? valueData : serializationService.toObject(valueData);
            } else if (iterationType == IterationType.KEY) {
                Data keyData = entry.getKeyData();
                return (data) ? keyData : serializationService.toObject(keyData);
            } else {
                Data keyData = entry.getKeyData();
                Data valueData = entry.getValueData();
                return (data) ? new AbstractMap.SimpleImmutableEntry(keyData, valueData) : new AbstractMap.SimpleImmutableEntry(serializationService.toObject(keyData), serializationService.toObject(valueData));
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.QUERY_RESULT_SET;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(data);
        out.writeUTF(iterationType.toString());
        out.writeInt(entries.size());
        for (QueryResultEntry queryResultEntry : entries) {
            out.writeObject(queryResultEntry);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        data = in.readBoolean();
        iterationType = IterationType.valueOf(in.readUTF());
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            entries.add((QueryResultEntry) in.readObject());
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QueryResultSet{");
        sb.append("entries=").append(entries);
        sb.append(", iterationType=").append(iterationType);
        sb.append(", data=").append(data);
        sb.append('}');
        return sb.toString();
    }
}
