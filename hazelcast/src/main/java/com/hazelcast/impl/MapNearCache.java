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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Data;
import com.hazelcast.util.SortedHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.nio.IOUtil.toObject;

public class MapNearCache {
    private final ILogger logger;
    private final SortedHashMap<Data, Object> sortedMap;
    private final ConcurrentMap<Object, CacheEntry> cache;
    private final CMap cmap;
    private final int maxSize;        // 0 means infinite
    private final long ttl;           // 0 means never expires
    private final long maxIdleTime;   // 0 means never idle 
    private final boolean invalidateOnChange;
    private final int LOCAL_INVALIDATION_COUNTER = 10000;
    private final AtomicInteger counter = new AtomicInteger();
    private long lastEvictionTime = 0;

    public MapNearCache(CMap cmap, SortedHashMap.OrderingType orderingType, int maxSize, long ttl, long maxIdleTime, boolean invalidateOnChange) {
        this.cmap = cmap;
        this.logger = cmap.concurrentMapManager.node.getLogger(MapNearCache.class.getName());
        this.maxSize = maxSize;
        this.ttl = ttl;
        this.maxIdleTime = maxIdleTime;
        this.invalidateOnChange = invalidateOnChange;
        int size = (maxSize == 0 || maxSize > 50000) ? 10000 : maxSize;
        this.sortedMap = new SortedHashMap<Data, Object>(size, orderingType);
        this.cache = new ConcurrentHashMap<Object, CacheEntry>(size);
    }

    boolean shouldInvalidateOnChange() {
        return invalidateOnChange;
    }

    public boolean containsKey(Object key) {
        long now = System.currentTimeMillis();
        if (counter.incrementAndGet() == LOCAL_INVALIDATION_COUNTER) {
            counter.addAndGet(-(LOCAL_INVALIDATION_COUNTER));
            evict(now, false);
        }
        CacheEntry entry = cache.get(key);
        return !(entry == null || entry.isValid(now));
    }

    public void setContainsKey(Object key, Data dataKey) {
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            put(key, dataKey, null);
        }
    }

    public Object get(Object key) {
        long now = System.currentTimeMillis();
        if (counter.incrementAndGet() == LOCAL_INVALIDATION_COUNTER) {
            counter.addAndGet(-(LOCAL_INVALIDATION_COUNTER));
            evict(now, false);
        }
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            return null;
        } else {
            if (entry.isValid(now)) {
                Object value = entry.getValue();
                cmap.concurrentMapManager.enqueueAndReturn(entry);
                entry.touch(now);
                return value;
            } else {
                return null;
            }
        }
    }

    private List<Data> getInvalidEntries(long now) {
        List<Data> lsKeysToInvalidate = null;
        if (now - lastEvictionTime > 10000) {
            if (ttl != 0 || maxIdleTime != 0) {
                lsKeysToInvalidate = new ArrayList<Data>();
                Collection<CacheEntry> entries = cache.values();
                for (CacheEntry entry : entries) {
                    if (!entry.isValid(now)) {
                        lsKeysToInvalidate.add(entry.keyData);
                    }
                }
            }
            lastEvictionTime = now;
        }
        return lsKeysToInvalidate;
    }

    public void evict(long now, boolean serviceThread) {
        if (serviceThread) {
            checkThread();
        }
        if (maxSize == Integer.MAX_VALUE) return;
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
            throw new IllegalStateException("cache and sorted map size should be the same: "
                    + cache.size() + " vs. " + sortedMap.size());
        }
        if (cache.size() + 1 >= maxSize) {
            startEviction();
        }
        if (cache.size() + 1 >= maxSize) {
            return;
        }
        if (!sortedMap.containsKey(keyData)) {
            sortedMap.put(keyData, key);
            cache.put(key, new CacheEntry(key, keyData, value));
        }
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
        Object theKey = sortedMap.remove(key);
        if (theKey != null) {
            CacheEntry removedCacheEntry = cache.remove(theKey);
            if (removedCacheEntry == null) {
                throw new IllegalStateException("Removed CacheEntry cannot be null");
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
        sbState.append(", n.sorted:" + sortedMap.size());
        sbState.append(", n.cache:" + cache.size());
    }

    public int getMaxSize() {
        return maxSize;
    }

    private class CacheEntry implements Processable {
        final Object key;
        final Data keyData;
        private Data valueData = null;
        private Object value = null;
        private final long createTime;
        private volatile long lastAccessTime;

        private CacheEntry(Object key, Data keyData, Data valueData) {
            if (key == null) {
                throw new IllegalStateException("key cannot be null");
            }
            if (keyData == null) {
                throw new IllegalStateException("keyData cannot be null");
            }
            if (valueData == null) {
                throw new IllegalStateException("valueData cannot be null");
            }
            this.key = key;
            this.keyData = keyData;
            this.valueData = valueData;
            this.createTime = System.currentTimeMillis();
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

        public Object getValue() {
            if (value != null) {
                return value;
            }
            if (valueData == null) {
                return null;
            }
            value = toObject(valueData);
            valueData = null;
            return value;
        }

        public Data getValueData() {
            return valueData;
        }

        public void process() {
            sortedMap.get(keyData);
        }
    }
}
