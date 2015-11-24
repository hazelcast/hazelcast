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

package com.hazelcast.map.impl.query;

import static com.hazelcast.util.IterationType.ENTRY;
import static java.util.Collections.newSetFromMap;

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.util.IterationType;

import java.io.IOException;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Collection(Set) class for result of query operations.
 *
 * @deprecated use {@link QueryResultCollection}.
 */
@Deprecated
public class QueryResultSet extends AbstractSet implements IdentifiedDataSerializable {

    private Set<QueryResultRow> rows = newSetFromMap(new ConcurrentHashMap<QueryResultRow, Boolean>());
    private final SerializationService serializationService;
    private IterationType iterationType;
    private boolean binary;

    // needed for serialization
    public QueryResultSet() {
        serializationService = null;
    }

    public QueryResultSet(SerializationService serializationService, IterationType iterationType, boolean binary) {
        this.serializationService = serializationService;
        this.iterationType = iterationType;
        this.binary = binary;
    }

    @SuppressWarnings("unchecked")
    public Iterator<Map.Entry> rawIterator() {
        return new QueryResultIterator(rows.iterator(), ENTRY, binary, serializationService);
    }

    public boolean add(QueryResultRow row) {
        return rows.add(row);
    }

    public boolean remove(QueryResultRow row) {
        return rows.remove(row);
    }

    public boolean contains(QueryResultRow row) {
        return rows.contains(row);
    }

    @Override
    public boolean add(Object row) {
        return rows.add((QueryResultRow) row);
    }

    @Override
    public Iterator iterator() {
        return new QueryResultIterator(rows.iterator(), iterationType, binary, serializationService);
    }

    @Override
    public int size() {
        return rows.size();
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
        out.writeBoolean(binary);
        out.writeByte(iterationType.getId());
        out.writeInt(rows.size());
        for (QueryResultRow entry : rows) {
            switch (iterationType) {
                case KEY:
                    out.writeData(entry.getKey());
                    break;
                case VALUE:
                    out.writeData(entry.getValue());
                    break;
                case ENTRY:
                    out.writeData(entry.getKey());
                    out.writeData(entry.getValue());
                    break;
                default:
                    throw new IllegalStateException("Unrecognized iterationType:" + iterationType);
            }
            out.writeObject(entry);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        binary = in.readBoolean();
        iterationType = IterationType.getById(in.readByte());
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            //todo:
            rows.add((QueryResultRow) in.readObject());
        }
    }

    @Override
    public String toString() {
        return "QueryResultSet{"
                + "rows=" + entries()
                + ", iterationType=" + iterationType
                + ", data=" + binary
                + '}';

    }

    private String entries() {
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
