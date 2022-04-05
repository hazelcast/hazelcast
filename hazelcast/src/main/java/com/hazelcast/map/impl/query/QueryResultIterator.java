/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.IterationType;

import java.util.AbstractMap;
import java.util.Iterator;

final class QueryResultIterator implements Iterator {

    private final Iterator<QueryResultRow> iterator;
    private final IterationType iteratorType;
    private final boolean binary;
    private final SerializationService serializationService;

    QueryResultIterator(Iterator<QueryResultRow> iterator, IterationType iteratorType, boolean binary,
                        SerializationService serializationService) {
        this.iterator = iterator;
        this.iteratorType = iteratorType;
        this.binary = binary;
        this.serializationService = serializationService;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object next() {
        QueryResultRow row = iterator.next();
        switch (iteratorType) {
            case VALUE:
                return binary ? row.getValue() : serializationService.toObject(row.getValue());
            case KEY:
                return binary ? row.getKey() : serializationService.toObject(row.getKey());
            case ENTRY:
                if (binary) {
                    return new AbstractMap.SimpleImmutableEntry(row.getKey(), row.getValue());
                } else {
                    Object key = serializationService.toObject(row.getKey());
                    Object value = serializationService.toObject(row.getValue());
                    return new AbstractMap.SimpleImmutableEntry(key, value);
                }
            default:
                throw new IllegalStateException("Unrecognized iteratorType:" + iteratorType);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
