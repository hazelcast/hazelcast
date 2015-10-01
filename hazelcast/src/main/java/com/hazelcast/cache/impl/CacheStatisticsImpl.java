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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * {@link CacheStatistics} implementation for {@link com.hazelcast.cache.ICache}.
 *
 * <p>Statistics accumulated in this data object is published through MxBean as defined by spec.</p>
 *
 * @see com.hazelcast.cache.impl.CacheMXBeanImpl
 */
public class CacheStatisticsImpl
        implements DataSerializable, CacheStatistics {

    private static final float FLOAT_HUNDRED = 100.0f;
    private static final long NANOSECONDS_IN_A_MICROSECOND = 1000L;

    private static final AtomicLongFieldUpdater<CacheStatisticsImpl> LAST_ACCESS_TIME =
            AtomicLongFieldUpdater.newUpdater(CacheStatisticsImpl.class, "lastAccessTime");
    private static final AtomicLongFieldUpdater<CacheStatisticsImpl> LAST_UPDATE_TIME =
            AtomicLongFieldUpdater.newUpdater(CacheStatisticsImpl.class, "lastUpdateTime");
    private static final AtomicLongFieldUpdater<CacheStatisticsImpl> REMOVALS =
            AtomicLongFieldUpdater.newUpdater(CacheStatisticsImpl.class, "removals");
    private static final AtomicLongFieldUpdater<CacheStatisticsImpl> EXPIRIES =
            AtomicLongFieldUpdater.newUpdater(CacheStatisticsImpl.class, "expiries");
    private static final AtomicLongFieldUpdater<CacheStatisticsImpl> PUTS =
            AtomicLongFieldUpdater.newUpdater(CacheStatisticsImpl.class, "puts");
    private static final AtomicLongFieldUpdater<CacheStatisticsImpl> HITS =
            AtomicLongFieldUpdater.newUpdater(CacheStatisticsImpl.class, "hits");
    private static final AtomicLongFieldUpdater<CacheStatisticsImpl> MISSES =
            AtomicLongFieldUpdater.newUpdater(CacheStatisticsImpl.class, "misses");
    private static final AtomicLongFieldUpdater<CacheStatisticsImpl> EVICTIONS =
            AtomicLongFieldUpdater.newUpdater(CacheStatisticsImpl.class, "evictions");
    private static final AtomicLongFieldUpdater<CacheStatisticsImpl> PUT_TIME_TAKEN_NANOS =
            AtomicLongFieldUpdater.newUpdater(CacheStatisticsImpl.class, "putTimeTakenNanos");
    private static final AtomicLongFieldUpdater<CacheStatisticsImpl> GET_CACHE_TIME_TAKEN_NANOS =
            AtomicLongFieldUpdater.newUpdater(CacheStatisticsImpl.class, "getCacheTimeTakenNanos");
    private static final AtomicLongFieldUpdater<CacheStatisticsImpl> REMOVE_TIME_TAKEN_NANOS =
            AtomicLongFieldUpdater.newUpdater(CacheStatisticsImpl.class, "removeTimeTakenNanos");

    /**
     * This field is not mutated (read only) so no need to define it as volatile.
     */
    private long creationTime;

    private volatile long lastAccessTime;
    private volatile long lastUpdateTime;
    private volatile long removals;
    private volatile long expiries;
    private volatile long puts;
    private volatile long hits;
    private volatile long misses;
    private volatile long evictions;
    private volatile long putTimeTakenNanos;
    private volatile long getCacheTimeTakenNanos;
    private volatile long removeTimeTakenNanos;

    /**
     * This field is used when this stats instance is deserialized.
     * So it is not mutated (read only) and no need to define it as volatile.
     */
    private long ownedEntryCount;
    private transient CacheContext cacheContext;

    public CacheStatisticsImpl() {
    }

    public CacheStatisticsImpl(long creationTime, CacheContext cacheContext) {
        this.creationTime = creationTime;
        this.cacheContext = cacheContext;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccessTime;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @Override
    public long getOwnedEntryCount() {
        if (cacheContext != null) {
            return cacheContext.getEntryCount();
        } else  {
            return ownedEntryCount;
        }
    }

    @Override
    public long getCacheRemovals() {
        return removals;
    }

    /**
     *
     * The total number of expiries from the cache. An expiry may or may not be evicted.
     * This number represents the entries that fail evaluation and may not include the entries which are not yet
     * evaluated for expiry or not accessed.
     *
     * @return the number of expiries.
     */
    public long getCacheExpiries() {
        return expiries;
    }

    @Override
    public long getCacheGets() {
        return getCacheHits() + getCacheMisses();
    }

    @Override
    public long getCachePuts() {
        return puts;
    }

    @Override
    public long getCacheHits() {
        return hits;
    }

    @Override
    public long getCacheMisses() {
        return misses;
    }

    @Override
    public long getCacheEvictions() {
        return evictions;
    }

    public long getCachePutTimeTakenNanos() {
        return putTimeTakenNanos;
    }

    public long getCacheGetTimeTakenNanos() {
        return getCacheTimeTakenNanos;
    }

    public long getCacheRemoveTimeTakenNanos() {
        return removeTimeTakenNanos;
    }

    @Override
    public float getCacheHitPercentage() {
        final long cacheHits = getCacheHits();
        final long cacheGets = getCacheGets();
        if (cacheHits == 0 || cacheGets == 0) {
            return 0;
        }
        return (float) cacheHits / cacheGets * FLOAT_HUNDRED;
    }

    @Override
    public float getCacheMissPercentage() {
        final long cacheMisses = getCacheMisses();
        final long cacheGets = getCacheGets();
        if (cacheMisses == 0 || cacheGets == 0) {
            return 0;
        }
        return (float) cacheMisses / cacheGets * FLOAT_HUNDRED;
    }

    @Override
    public float getAverageGetTime() {
        final long cacheGetTimeTakenNanos = getCacheGetTimeTakenNanos();
        final long cacheGets = getCacheGets();
        if (cacheGetTimeTakenNanos == 0 || cacheGets == 0) {
            return 0;
        }
        return ((1f * cacheGetTimeTakenNanos) / cacheGets) / NANOSECONDS_IN_A_MICROSECOND;
    }

    @Override
    public float getAveragePutTime() {
        final long cachePutTimeTakenNanos = getCachePutTimeTakenNanos();
        final long cachePuts = getCachePuts();
        if (cachePutTimeTakenNanos == 0 || cachePuts == 0) {
            return 0;
        }
        return ((1f * cachePutTimeTakenNanos) / cachePuts) / NANOSECONDS_IN_A_MICROSECOND;
    }

    @Override
    public float getAverageRemoveTime() {
        final long cacheRemoveTimeTakenNanos = getCacheRemoveTimeTakenNanos();
        final long cacheRemoves = getCacheRemovals();
        if (cacheRemoveTimeTakenNanos == 0 || cacheRemoves == 0) {
            return 0;
        }
        return ((1f * cacheRemoveTimeTakenNanos) / cacheRemoves) / NANOSECONDS_IN_A_MICROSECOND;
    }

    /**
     * Implementation of {@link javax.cache.management.CacheStatisticsMXBean#clear()}.
     * @see javax.cache.management.CacheStatisticsMXBean#clear()
     */
    public void clear() {
        // TODO Should we clear `creationTime` also? In fact, it doesn't make sense
        // TODO Should we clear `ownedEntryCount` also? In fact, it doesn't make sense

        puts = 0;
        misses = 0;
        removals = 0;
        expiries = 0;
        hits = 0;
        evictions = 0;
        getCacheTimeTakenNanos = 0;
        putTimeTakenNanos = 0;
        removeTimeTakenNanos = 0;
    }

    /**
     * Sets the cache last access time as atomic if the given time is bigger than it.
     *
     * @param time time to set the cache last access time
     */
    public void setLastAccessTime(long time) {
        for (;;) {
            if (time > lastAccessTime) {
                if (LAST_ACCESS_TIME.compareAndSet(this, lastAccessTime, time)) {
                    break;
                }
            } else {
                break;
            }
        }
    }

    /**
     * Sets the cache last update time as atomic if the given time is bigger than it.
     *
     * @param time time to set the cache last update time
     */
    public void setLastUpdateTime(long time) {
        for (;;) {
            if (time > lastUpdateTime) {
                if (LAST_UPDATE_TIME.compareAndSet(this, lastUpdateTime, time)) {
                    break;
                }
            } else {
                break;
            }
        }
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number by which the counter is increased.
     */
    public void increaseCacheRemovals(long number) {
        REMOVALS.addAndGet(this, number);
        setLastUpdateTime(Clock.currentTimeMillis());
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number by which the counter is increased.
     */
    public void increaseCacheExpiries(long number) {
        EXPIRIES.addAndGet(this, number);
        setLastUpdateTime(Clock.currentTimeMillis());
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number by which the counter is increased.
     */
    public void increaseCachePuts(long number) {
        PUTS.addAndGet(this, number);
        setLastUpdateTime(Clock.currentTimeMillis());
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number by which the counter is increased.
     */
    public void increaseCacheHits(long number) {
        HITS.addAndGet(this, number);
        setLastAccessTime(Clock.currentTimeMillis());
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number by which the counter is increased.
     */
    public void increaseCacheMisses(long number) {
        MISSES.addAndGet(this, number);
        setLastAccessTime(Clock.currentTimeMillis());
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number by which the counter is increased.
     */
    public void increaseCacheEvictions(long number) {
        EVICTIONS.addAndGet(this, number);
        setLastUpdateTime(Clock.currentTimeMillis());
    }

    /**
     * Increments the getCache time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addGetTimeNanos(long duration) {
        for (;;) {
            long nanos = getCacheTimeTakenNanos;
            if (nanos <= Long.MAX_VALUE - duration) {
                if (GET_CACHE_TIME_TAKEN_NANOS.compareAndSet(this, nanos, nanos + duration)) {
                    return;
                }
            } else {
                //counter full. Just reset.
                if (GET_CACHE_TIME_TAKEN_NANOS.compareAndSet(this, nanos, duration)) {
                    clear();
                    return;
                }
            }
        }
    }

    /**
     * Increments the put time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addPutTimeNanos(long duration) {
        for (;;) {
            long nanos = putTimeTakenNanos;
            if (nanos <= Long.MAX_VALUE - duration) {
                if (PUT_TIME_TAKEN_NANOS.compareAndSet(this, nanos, nanos + duration)) {
                    return;
                }
            } else {
                //counter full. Just reset.
                if (PUT_TIME_TAKEN_NANOS.compareAndSet(this, nanos, duration)) {
                    clear();
                    return;
                }
            }
        }
    }

    /**
     * Increments the remove time accumulator.
     *
     * @param duration the time taken in nanoseconds.
     */
    public void addRemoveTimeNanos(long duration) {
        for (;;) {
            long nanos = removeTimeTakenNanos;
            if (nanos <= Long.MAX_VALUE - duration) {
                if (REMOVE_TIME_TAKEN_NANOS.compareAndSet(this, nanos, nanos + duration)) {
                    return;
                }
            } else {
                //counter full. Just reset.
                if (REMOVE_TIME_TAKEN_NANOS.compareAndSet(this, nanos, duration)) {
                    clear();
                    return;
                }
            }
        }
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeLong(creationTime);

        out.writeLong(lastAccessTime);
        out.writeLong(lastUpdateTime);
        out.writeLong(getOwnedEntryCount());

        out.writeLong(puts);
        out.writeLong(removals);
        out.writeLong(expiries);
        out.writeLong(evictions);

        out.writeLong(hits);
        out.writeLong(misses);

        out.writeLong(putTimeTakenNanos);
        out.writeLong(getCacheTimeTakenNanos);
        out.writeLong(removeTimeTakenNanos);
    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        creationTime = in.readLong();

        lastAccessTime = in.readLong();
        lastUpdateTime = in.readLong();
        ownedEntryCount = in.readLong();

        puts = in.readLong();
        removals = in.readLong();
        expiries = in.readLong();
        evictions = in.readLong();

        hits = in.readLong();
        misses = in.readLong();

        putTimeTakenNanos = in.readLong();
        getCacheTimeTakenNanos = in.readLong();
        removeTimeTakenNanos = in.readLong();
    }

    @Override
    public String toString() {
        return "CacheStatisticsImpl{"
                    + "creationTime=" + creationTime
                    + ", lastAccessTime=" + lastAccessTime
                    + ", lastUpdateTime=" + lastUpdateTime
                    + ", ownedEntryCount=" + getOwnedEntryCount()
                    + ", removals=" + removals
                    + ", expiries=" + expiries
                    + ", puts=" + puts
                    + ", hits=" + hits
                    + ", misses=" + misses
                    + ", evictions=" + evictions
                    + ", putTimeTakenNanos=" + putTimeTakenNanos
                    + ", getCacheTimeTakenNanos=" + getCacheTimeTakenNanos
                    + ", removeTimeTakenNanos=" + removeTimeTakenNanos
                + '}';
    }
}
