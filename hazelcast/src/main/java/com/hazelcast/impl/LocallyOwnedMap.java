/*
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.nio.IOUtil.toObject;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

class LocallyOwnedMap {
    final static Logger logger = Logger.getLogger(LocallyOwnedMap.class.getName());
    final ConcurrentMap<Object, Record> mapCache = new ConcurrentHashMap<Object, Record>(1000);
    final private Queue<Record> localRecords = new ConcurrentLinkedQueue<Record>();

    public Object get(Object key) {
        processLocalRecords();
        Record record = mapCache.get(key);
        if (record == null) {
            return OBJECT_REDO;
        } else {
            if (record.isActive() && record.isValid()) {
                try {
                    Object value = toObject(record.getValue());
                    record.setLastAccessed();
                    return value;
                } catch (Throwable t) {
                    logger.log(Level.FINEST, "Exception when reading object ", t);
                    return OBJECT_REDO;
                }
            } else {
                //record is removed!
                mapCache.remove(key);
//                return null;
                return OBJECT_REDO;
            }
        }
    }

    public void reset() {
        localRecords.clear();
        mapCache.clear();
    }

    private void processLocalRecords() {
        Record record = localRecords.poll();
        while (record != null) {
            if (record.isActive()) {
                doPut(record);
            }
            record = localRecords.poll();
        }
    }

    public void doPut(Record record) {
        Object key = toObject(record.getKey());
        mapCache.put(key, record);
    }

    public void offerToCache(Record record) {
        localRecords.offer(record);
    }
}
