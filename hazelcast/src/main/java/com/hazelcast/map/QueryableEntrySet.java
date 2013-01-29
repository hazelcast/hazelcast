/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryableEntry;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class QueryableEntrySet extends AbstractSet<QueryableEntry> {

    List<ConcurrentMap<Data, Record>> recordMapList;
    SerializationServiceImpl serializationService;

    public QueryableEntrySet(SerializationServiceImpl serializationService, List<ConcurrentMap<Data, Record>> recordMapList) {
        this.recordMapList = recordMapList;
        this.serializationService = serializationService;
    }

    @Override
    public Iterator<QueryableEntry> iterator() {
        return new RecordIterator();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    class RecordIterator implements Iterator<QueryableEntry> {

        Iterator<Record> innerIterator;
        Iterator<ConcurrentMap<Data, Record>> iter = null;
        Record currentEntry = null;

        RecordIterator() {
            iter = recordMapList.iterator();
            if (iter.hasNext())
                innerIterator = iter.next().values().iterator();
        }

        public boolean hasNext() {
            if (innerIterator == null) {
                return false;
            }
            if (innerIterator.hasNext()) {
                currentEntry = innerIterator.next();
                return true;
            } else if (iter.hasNext()) {
                innerIterator = iter.next().values().iterator();
                return hasNext();
            }
            return false;
        }

        public QueryableEntry next() {
            if (currentEntry == null) return null;
            final Record record = currentEntry;
            Data key = record.getKey();
            Object value = null;
            if (record instanceof CachedDataRecord) {
                CachedDataRecord cachedDataRecord = (CachedDataRecord) record;
                value = cachedDataRecord.getCachedValue();
                if (value == null) {
                    value = serializationService.toObject(cachedDataRecord.getValue());
                    cachedDataRecord.setCachedValue(value);
                }
            } else if (record instanceof DataRecord) {
                value = serializationService.toObject(((DataRecord) record).getValue());
            } else {
                value = record.getValue();
            }
            return new QueryEntry(serializationService, key, key, value);
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
