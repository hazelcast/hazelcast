/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.nio.IOUtil.toObject;

class LocallyOwnedMap {
    private static final Logger logger = Logger.getLogger(LocallyOwnedMap.class.getName());
    private final ConcurrentMap<Object, Record> mapCache = new ConcurrentHashMap<Object, Record>(1000);
    private final Queue<Record> localRecords = new ConcurrentLinkedQueue<Record>();
    private final AtomicInteger counter = new AtomicInteger();
    private final int LOCAL_INVALIDATION_COUNTER = 10000;
    private long lastEvictionTime = 0;

    public Object get(Object key) {
        processLocalRecords();
        if (counter.incrementAndGet() == LOCAL_INVALIDATION_COUNTER) {
            counter.addAndGet(-LOCAL_INVALIDATION_COUNTER);
            evict(System.currentTimeMillis());
        }
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
                return OBJECT_REDO;
            }
        }
    }

    public void evict(long now) {
        if (now - lastEvictionTime > 10000) {
            lastEvictionTime = now;
            Set<Map.Entry<Object, Record>> entries = mapCache.entrySet();
            List<Object> lsKeysToRemove = new ArrayList<Object>();
            for (Map.Entry<Object, Record> entry : entries) {
                Object key = entry.getKey();
                Record record = entry.getValue();
                if (!record.isActive() || !record.isValid(now)) {
                    lsKeysToRemove.add(key);
                }
            }
            for (Object key : lsKeysToRemove) {
                mapCache.remove(key);
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

    public void appendState(StringBuffer sbState) {
        sbState.append(", l.cache:");
        sbState.append(mapCache.size());
        sbState.append(", l.records:");
        sbState.append(localRecords.size());
    }
}
