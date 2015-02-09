package com.hazelcast.map.impl;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.util.Clock;
import com.hazelcast.util.QuickMath;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Entry holder to be used in Client and Node side Near cache
 */
public class NearCacheRecord {
    private static final Comparator<NearCacheRecord> LRU_COMPARATOR = new Comparator<NearCacheRecord>() {
        public int compare(NearCacheRecord o1, NearCacheRecord o2) {
            final int result = QuickMath.compareLongs(o1.lastAccessTime, o2.lastAccessTime);
            if (result != 0) {
                return result;
            }
            return QuickMath.compareIntegers(o1.key.hashCode(), o2.key.hashCode());
        }
    };

    private static final Comparator<NearCacheRecord> LFU_COMPARATOR = new Comparator<NearCacheRecord>() {
        public int compare(NearCacheRecord o1, NearCacheRecord o2) {
            final int result = QuickMath.compareLongs(o1.hit.get(), o2.hit.get());
            if (result != 0) {
                return result;
            }
            return QuickMath.compareIntegers(o1.key.hashCode(), o2.key.hashCode());
        }
    };

    private static final Comparator<NearCacheRecord> DEFAULT_COMPARATOR = new Comparator<NearCacheRecord>() {
        public int compare(NearCacheRecord o1, NearCacheRecord o2) {
            return QuickMath.compareIntegers(o1.key.hashCode(), o2.key.hashCode());
        }
    };

    private final Object key;
    private final Object value;
    private final long creationTime;
    private final AtomicLong hit;
    private volatile long lastAccessTime;

    public NearCacheRecord(Object key, Object value) {
        this.key = key;
        this.value = value;
        long time = Clock.currentTimeMillis();
        this.lastAccessTime = time;
        this.creationTime = time;
        this.hit = new AtomicLong();
    }

    public Object getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public void access() {
        hit.incrementAndGet();
        lastAccessTime = Clock.currentTimeMillis();
    }

    public long getCost() {
        // todo find object size  if not a Data instance.
        if (!(value instanceof Data)) {
            return 0;
        }
        if (!(key instanceof Data)) {
            return 0;
        }
        // value is Data
        return ((Data) key).getHeapCost()
                + ((Data) value).getHeapCost()
                + 2 * (Long.SIZE / Byte.SIZE)
                // sizeof atomic long
                + (Long.SIZE / Byte.SIZE)
                // object references (key, value, hit)
                + 3 * (Integer.SIZE / Byte.SIZE);
    }

    public boolean isExpired(long maxIdleMillis, long timeToLiveMillis) {
        long time = Clock.currentTimeMillis();
        return (maxIdleMillis > 0 && time > lastAccessTime + maxIdleMillis)
                || (timeToLiveMillis > 0 && time > creationTime + timeToLiveMillis);
    }

    /**
     * @param evictionPolicy EvictionPolicy
     * @return appropriate comparator function depending on the eviction policy
     */
    public static Comparator<NearCacheRecord> getComparator(EvictionPolicy evictionPolicy) {
        if (EvictionPolicy.LRU.equals(evictionPolicy)) {
            return NearCacheRecord.LRU_COMPARATOR;
        } else if (EvictionPolicy.LFU.equals(evictionPolicy)) {
            return NearCacheRecord.LFU_COMPARATOR;
        } else {
            return NearCacheRecord.DEFAULT_COMPARATOR;
        }
    }


}
