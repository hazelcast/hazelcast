package com.hazelcast.impl;

import com.hazelcast.util.SortedHashMap;
import com.hazelcast.nio.Data;
import static com.hazelcast.nio.IOUtil.toObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class MapNearCache {
    private final SortedHashMap<Data, Object> sortedMap;
    private final ConcurrentMap<Object, CacheEntry> cache;
    private final CMap cmap;
    private final int maxSize;        // 0 means infinite
    private final long ttl;           // 0 means never expires
    private final long maxIdleTime;   // 0 means never idle 
    private final AtomicLong callCount = new AtomicLong();
    private final boolean invalidateOnChange;

    public MapNearCache(CMap cmap, SortedHashMap.OrderingType orderingType, int maxSize, long ttl, long maxIdleTime, boolean invalidateOnChange) {
        this.cmap = cmap;
        this.maxSize = maxSize;
        this.ttl = ttl;
        this.maxIdleTime = maxIdleTime;
        this.invalidateOnChange = invalidateOnChange;
        int size = (maxSize == 0) ? 1000 : maxSize;
        this.sortedMap = new SortedHashMap<Data, Object>(size, orderingType);
        this.cache = new ConcurrentHashMap<Object, CacheEntry>(size);
    }

    boolean shouldInvalidateOnChange() {
        return invalidateOnChange;
    }

    public Object get(Object key) {
        if (ttl != 0 || maxIdleTime != 0) {
            long callCountNow = callCount.incrementAndGet();
            if (callCountNow == 5000) {
                callCount.addAndGet(-5000);
                cmap.concurrentMapManager.enqueueAndReturn(new Processable() {
                    public void process() {
                        Collection<CacheEntry> entries = cache.values();
                        long now = System.currentTimeMillis();
                        for (CacheEntry entry : entries) {
                            if (!entry.isValid(now)) {
                                invalidate(entry.keyData);
                            }
                        }
                    }
                });
            }
        }
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            return null;
        } else {
            long now = System.currentTimeMillis();
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

    private void startEviction() {
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
            value = toObject(valueData);
            valueData = null;
            return value;
        }

        public void process() {
            sortedMap.get(keyData);
        }
    }
}
