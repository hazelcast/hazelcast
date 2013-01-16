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

package com.hazelcast.impl;

import com.hazelcast.impl.concurrentmap.RecordFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Data;
import com.hazelcast.util.Clock;
import com.hazelcast.util.SortedHashMap;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

public class NearCache {
    private final ILogger logger;
    private final Map<Data, Object> sortedMap;
    private final ConcurrentMap<Object, CacheEntry> cache;
    private final CMap cmap;
    private final SortedHashMap.OrderingType orderingType;
    private final int maxSize;        // 0 means infinite
    private final long ttl;           // 0 means never expires
    private final long maxIdleTime;   // 0 means never idle 
    private final boolean invalidateOnChange;
    private final RecordFactory recordFactory;

    public NearCache(CMap cmap, SortedHashMap.OrderingType orderingType, int maxSize, long ttl, long maxIdleTime, boolean invalidateOnChange) {
        this.cmap = cmap;
        this.orderingType = orderingType;
        this.logger = cmap.concurrentMapManager.node.getLogger(NearCache.class.getName());
        this.maxSize = (maxSize == 0) ? Integer.MAX_VALUE : maxSize;
        this.ttl = ttl;
        this.maxIdleTime = maxIdleTime;
        this.invalidateOnChange = invalidateOnChange;
        int size = (maxSize == 0 || maxSize > 50000) ? 10000 : maxSize;
        this.sortedMap = (orderingType == SortedHashMap.OrderingType.NONE)
                ? new HashMap<Data, Object>()
                : new SortedHashMap<Data, Object>(size, orderingType);
        this.cache = new ConcurrentHashMap<Object, CacheEntry>(size, 0.75f, 1);
        this.recordFactory = cmap.concurrentMapManager.recordFactory;
    }

    boolean shouldInvalidateOnChange() {
        return invalidateOnChange;
    }

    public boolean containsKey(Object key) {
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            return false;
        } else {
            long now = Clock.currentTimeMillis();
            return entry.isValid(now);
        }
    }

    public void setContainsKey(Object key, Data dataKey) {
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            put(key, dataKey, null);
        }
    }

    public Object get(Object key) {
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            return null;
        } else {
            long now = Clock.currentTimeMillis();
            if (entry.isValid(now)) {
                Object value = null;
                if (ThreadContext.get().isClient()) {
                    value = entry.getValueData();
                } else {
                    value = entry.getValue();
                }
                if (orderingType != SortedHashMap.OrderingType.NONE) {
                    cmap.concurrentMapManager.enqueueAndReturn(entry);
                }
                entry.touch(now);
                return value;
            } else {
                return null;
            }
        }
    }

    private List<Data> getInvalidEntries(long now) {
        List<Data> lsKeysToInvalidate = null;
        if (ttl != 0 || maxIdleTime != 0) {
            lsKeysToInvalidate = new ArrayList<Data>();
            Collection<CacheEntry> entries = cache.values();
            for (CacheEntry entry : entries) {
                if (!entry.isValid(now)) {
                    lsKeysToInvalidate.add(entry.record.getKeyData());
                }
            }
        }
        return lsKeysToInvalidate;
    }

    public void evict(long now, boolean serviceThread) {
        if (serviceThread) {
            checkThread();
        }
        if (maxSize == Integer.MAX_VALUE && maxIdleTime == 0 && ttl == 0) return;
        final List<Data> lsKeysToInvalidate = getInvalidEntries(now);
        if (lsKeysToInvalidate != null && lsKeysToInvalidate.size() > 0) {
            if (serviceThread) {
                for (Data key : lsKeysToInvalidate) {
                    invalidate(key);
                }
            } else {
                cmap.concurrentMapManager.enqueueAndReturn(new Processable() {
                    public void process() {
                        for (Data key : lsKeysToInvalidate) {
                            invalidate(key);
                        }
                    }
                });
            }
        }
    }

    public void put(Object key, Data keyData, Data value) {
        checkThread();
        if (cache.size() != sortedMap.size()) {
            logger.log(Level.WARNING, cmap.getName() + " cache and sorted map size should be the same: "
                    + cache.size() + " vs. " + sortedMap.size()
                    + "/nCheck equals and hashCode of key object!");
        }
        if (cache.size() + 1 >= maxSize) {
            startEviction();
        }
        if (cache.size() + 1 >= maxSize) {
            return;
        }
        if (!sortedMap.containsKey(keyData)) {
            sortedMap.put(keyData, key);
        }
        CacheEntry cacheEntry = cache.get(key);
        if (cacheEntry == null) {
            cacheEntry = new CacheEntry(key, keyData, value);
        } else {
            cacheEntry.setValueData(value);
        }
        cache.put(key, cacheEntry);
    }

    void startEviction() {
        checkThread();
        int evictionCount = (int) (cache.size() * 0.25);
        List<Data> lsRemoves = new ArrayList<Data>(evictionCount);
        int count = 0;
        for (Data key : sortedMap.keySet()) {
            lsRemoves.add(key);
            if (count++ >= evictionCount) {
                break;
            }
        }
        for (Data key : lsRemoves) {
            invalidate(key);
        }
        lsRemoves.clear();
    }

    public void invalidate(Data key) {
        checkThread();
        final Object theKey = sortedMap.remove(key);
        if (theKey != null) {
            final CacheEntry removedCacheEntry = cache.remove(theKey);
            if (removedCacheEntry != null) {
                removedCacheEntry.invalidate();
            } else {
                logger.log(Level.WARNING, cmap.name + " removed CacheEntry cannot be null");
            }
        }
    }

    void checkThread() {
        if (cmap.concurrentMapManager.node.serviceThread != Thread.currentThread()) {
            throw new RuntimeException("Only ServiceThread can update the cache! "
                    + Thread.currentThread().getName());
        }
    }

    public void appendState(StringBuffer sbState) {
        sbState.append(", n.sorted:").append(sortedMap.size());
        sbState.append(", n.cache:").append(cache.size());
    }

    public int getMaxSize() {
        return maxSize;
    }

    public boolean isEmpty() {
        return cache.isEmpty();
    }

    public void reset() {
        sortedMap.clear();
        for (CacheEntry entry : cache.values()) {
            entry.invalidate();
        }
        cache.clear();
    }

    private class CacheEntry implements Processable {
        private final NearCacheRecord record;
        private final long createTime;
        @SuppressWarnings("VolatileLongOrDoubleField")
        private volatile long lastAccessTime;

        private CacheEntry(Object key, Data keyData, Data valueData) {
            if (key == null) {
                throw new IllegalStateException("key cannot be null");
            }
            if (keyData == null) {
                throw new IllegalStateException("keyData cannot be null");
            }
            this.record = recordFactory.createNewNearCacheRecord(cmap, keyData, valueData);
            this.createTime = Clock.currentTimeMillis();
            touch(createTime);
        }

        public void touch(long now) {
            lastAccessTime = now;
        }

        public boolean isValid(long now) {
            if (ttl != 0) {
                if (now - createTime > ttl) {
                    return false;
                }
            }
            if (maxIdleTime != 0) {
                if ((now - lastAccessTime) > maxIdleTime) {
                    return false;
                }
            }
            return true;
        }

        public void setValueData(Data valueData) {
            record.setValueData(valueData);
        }

        public Object getValue() {
            return record.getValue();
        }

        public Data getValueData() {
            return record.getValueData();
        }

        public void process() {
            sortedMap.get(record.getKeyData());
        }

        public void invalidate() {
            record.invalidate();
        }
    }
}
