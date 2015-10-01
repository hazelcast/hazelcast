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

import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.util.IterationType;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class QueryResultCollection<E> extends AbstractSet<E> {

    private final QueryResult queryResult;
    private final SerializationService serializationService;
    private final IterationType iterationType;
    private final boolean binary;

    public QueryResultCollection(SerializationService serializationService, IterationType iterationType, boolean binary,
                                 QueryResult queryResult) {
        this.serializationService = serializationService;
        this.iterationType = iterationType;
        this.binary = binary;
        this.queryResult = queryResult;
    }

    // just for testing
    Collection<QueryResultRow> getRows() {
        return queryResult.getRows();
    }

    public IterationType getIterationType() {
        return iterationType;
    }

    @Override
    public Iterator<E> iterator() {
        return new QueryResultIterator();
    }

    @Override
    public int size() {
        return queryResult.size();
    }

    private final class QueryResultIterator implements Iterator {

        private final QueryResult.Cursor cursor;

        private QueryResultIterator() {
            this.cursor = queryResult.openCursor();
        }

        @Override
        public boolean hasNext() {
            return cursor.hasNext();
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object next() {
            if (!cursor.next()) {
                throw new NoSuchElementException();
            }

            switch (iterationType) {
                case VALUE:
                    return binary ? cursor.getValue() : serializationService.toObject(cursor.getValue());
                case KEY:
                    return binary ? cursor.getKey() : serializationService.toObject(cursor.getKey());
                case ENTRY:
                    if (binary) {
                        return new AbstractMap.SimpleImmutableEntry(cursor.getKey(), cursor.getValue());
                    } else {
                        Object key = serializationService.toObject(cursor.getKey());
                        Object value = serializationService.toObject(cursor.getValue());
                        return new AbstractMap.SimpleImmutableEntry(key, value);
                    }
                default:
                    throw new IllegalStateException("Unrecognized iteratorType:" + iterationType);
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
