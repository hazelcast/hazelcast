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

package com.hazelcast.query.impl;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.IterationType;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Collection(List) class for result of query operations.
 */
public class QueryResultList extends AbstractList implements IdentifiedDataSerializable {

    private final List<QueryResultEntry> entries = new ArrayList<QueryResultEntry>();
    private final SerializationService serializationService;

    private IterationType iterationType;
    private boolean binary;

    public QueryResultList() {
        serializationService = null;
    }

    public QueryResultList(SerializationService serializationService, IterationType iterationType, boolean binary) {
        this.serializationService = serializationService;
        this.iterationType = iterationType;
        this.binary = binary;
    }

    @SuppressWarnings("unchecked")
    public Iterator<Map.Entry> rawIterator() {
        return new QueryResultIterator(entries.iterator(), IterationType.ENTRY, binary, serializationService);
    }

    public boolean add(QueryResultEntry entry) {
        return entries.add(entry);
    }

    public boolean remove(QueryResultEntry entry) {
        return entries.remove(entry);
    }

    public boolean contains(QueryResultEntry entry) {
        return entries.contains(entry);
    }

    @Override
    public boolean add(Object entry) {
        return entries.add((QueryResultEntry) entry);
    }

    @Override
    public Iterator iterator() {
        return new QueryResultIterator(entries.iterator(), iterationType, binary, serializationService);
    }

    @Override
    public Object get(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return entries.size();
    }

    //todo:
    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    //todo:
    @Override
    public int getId() {
        return MapDataSerializerHook.QUERY_RESULT_SET;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(binary);
        out.writeUTF(iterationType.toString());
        out.writeInt(entries.size());
        for (QueryResultEntry queryResultEntry : entries) {
            out.writeObject(queryResultEntry);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        binary = in.readBoolean();
        iterationType = IterationType.valueOf(in.readUTF());
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            entries.add((QueryResultEntry) in.readObject());
        }
    }

    @Override
    public String toString() {
        return "QueryResultList{"
                + "entries=" + entriesToString()
                + ", iterationType=" + iterationType
                + ", binary=" + binary
                + '}';

    }

    private String entriesToString() {
        final Iterator i = iterator();
        if (!i.hasNext()) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (; ; ) {
            Object e = i.next();
            sb.append(e == this ? "(this Collection)" : e);
            if (!i.hasNext()) {
                return sb.append(']').toString();
            }
            sb.append(", ");
        }
    }
}
