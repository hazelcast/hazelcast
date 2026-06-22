/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.IterationType;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Predicate;

public class QueryResultCollection<E> extends AbstractSet<E> {

    private final Collection<QueryResultRow> rows;
    private final SerializationService serializationService;
    private final IterationType iterationType;
    private final boolean binary;

    public QueryResultCollection(SerializationService serializationService,
                                 IterationType iterationType,
                                 boolean binary,
                                 boolean distinct,
                                 QueryResult queryResult) {
        this.serializationService = serializationService;
        this.iterationType = iterationType;
        this.binary = binary;
        if (distinct) {
            // convert to a set
            this.rows = new HashSet<>(queryResult.getRows());
        } else {
            // reuse the existing underlying list
            this.rows = queryResult.getRows();
        }
    }

    // just for testing
    Collection<QueryResultRow> getRows() {
        return rows;
    }

    public IterationType getIterationType() {
        return iterationType;
    }

    @Override
    public Iterator<E> iterator() {
        return new QueryResultIterator(rows.iterator(), iterationType, binary, serializationService);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean contains(Object o) {
        switch (iterationType) {
            case KEY: {
                // rows is a HashSet when distinct=true (always true for keySet/entrySet).
                // In KEY queries the server never sends values, so every row has value=null.
                // Serializing the query object and probing with (keyData, null) gives O(1) lookup.
                Data keyData = binary ? (Data) o : serializationService.toData(o);
                return rows.contains(new QueryResultRow(keyData, null));
            }
            case ENTRY: {
                // rows is a HashSet when distinct=true. QueryResultRow.equals() compares both
                // key and value, so the probe row must carry both serialized fields.
                Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;
                Data keyData = binary ? (Data) entry.getKey() : serializationService.toData(entry.getKey());
                Data valueData = binary ? (Data) entry.getValue() : serializationService.toData(entry.getValue());
                return rows.contains(new QueryResultRow(keyData, valueData));
            }
            default:
                // VALUE mode uses distinct=false so rows is a List — fall back to O(n).
                return super.contains(o);
        }
    }

    @Override
    public int size() {
        return rows.size();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> coll) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }
}
