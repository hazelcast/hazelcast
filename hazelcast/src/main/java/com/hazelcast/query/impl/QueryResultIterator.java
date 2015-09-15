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
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.util.IterationType;

import java.util.AbstractMap;
import java.util.Iterator;

/**
 * Iterator for this set.
 */
final class QueryResultIterator implements Iterator {

    private final Iterator<QueryResultEntry> iterator;
    private final IterationType iteratorType;
    private final boolean binary;
    private final SerializationService serializationService;

    QueryResultIterator(Iterator iterator, IterationType iteratorType, boolean binary,
                        SerializationService serializationService) {
        this.iteratorType = iteratorType;
        this.iterator = iterator;
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
        QueryResultEntry entry = iterator.next();
        //System.out.println("iterator.next ikey: "+entry.getKeyData()
        // +" index:"+entry.getIndexKey()+" value:"+entry.getValueData());

        switch (iteratorType) {
            case VALUE:
                if (binary) {
                    return entry.getValueData();
                } else {
                    return serializationService.toObject(entry.getValueData());
                }
            case KEY:
                if (binary) {
                    return entry.getKeyData();
                } else {
                    return serializationService.toObject(entry.getKeyData());
                }
            case ENTRY:
                if (binary) {
                    return new AbstractMap.SimpleImmutableEntry(entry.getKeyData(), entry.getValueData());
                } else {
                    Object key = serializationService.toObject(entry.getKeyData());
                    Object value = serializationService.toObject(entry.getValueData());
                    return new AbstractMap.SimpleImmutableEntry(key, value);
                }
            default:
                throw new IllegalStateException("Unhandled iterationType:" + iteratorType);
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}
