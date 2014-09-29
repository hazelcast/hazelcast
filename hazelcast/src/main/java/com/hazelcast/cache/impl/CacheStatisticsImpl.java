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

package com.hazelcast.cache.impl;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Some statistics of a {@link com.hazelcast.cache.ICache}
 *
 * Statistics accumulated in this data object is published through MxBean
 *
 * @see com.hazelcast.cache.impl.CacheMXBeanImpl
 */
public class CacheStatisticsImpl
        implements DataSerializable, CacheStatistics {

    private static final float FLOAT_HUNDRED = 100.0f;
    private static final long NANOSECONDS_IN_A_MICROSECOND = 1000L;

    private final AtomicLong removals = new AtomicLong();
    private final AtomicLong expiries = new AtomicLong();
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();
    private final AtomicLong putTimeTakenNanos = new AtomicLong();
    private final AtomicLong getCacheTimeTakenNanos = new AtomicLong();
    private final AtomicLong removeTimeTakenNanos = new AtomicLong();

    public CacheStatisticsImpl() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCacheRemovals() {
        return removals.get();
    }

    /**
     * The total number of expiries from the cache. An expiry may or may not be evicted.
     * This number represent the entries that fail evaluation and may not include the entries which are not yet
     * evaluated for expiry or not accessed.
     *
     * @return the number of expiries
     */
    public long getCacheExpiries() {
        return expiries.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCacheGets() {
        return getCacheHits() + getCacheMisses();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCachePuts() {
        return puts.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCacheHits() {
        return hits.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCacheMisses() {
        return misses.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getCacheEvictions() {
        return evictions.get();
    }

    /**
     * {@inheritDoc}
     */
    public long getCachePutTimeTakenNanos() {
        return putTimeTakenNanos.get();
    }

    /**
     * {@inheritDoc}
     */
    public long getCacheGetTimeTakenNanos() {
        return getCacheTimeTakenNanos.get();
    }

    /**
     * {@inheritDoc}
     */
    public long getCacheRemoveTimeTakenNanos() {
        return removeTimeTakenNanos.get();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public float getCacheHitPercentage() {
        final long cacheHits = getCacheHits();
        final long cacheGets = getCacheGets();
        if (cacheHits == 0 || cacheGets == 0) {
            return 0;
        }
        return (float) cacheHits / cacheGets * FLOAT_HUNDRED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getCacheMissPercentage() {
        final long cacheMisses = getCacheMisses();
        final long cacheGets = getCacheGets();
        if (cacheMisses == 0 || cacheGets == 0) {
            return 0;
        }
        return (float) cacheMisses / cacheGets * FLOAT_HUNDRED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getAverageGetTime() {
        final long cacheGetTimeTakenNanos = getCacheGetTimeTakenNanos();
        final long cacheGets = getCacheGets();
        if (cacheGetTimeTakenNanos == 0 || cacheGets == 0) {
            return 0;
        }
        return ((1f * cacheGetTimeTakenNanos) / cacheGets) / NANOSECONDS_IN_A_MICROSECOND;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getAveragePutTime() {
        final long cachePutTimeTakenNanos = getCachePutTimeTakenNanos();
        final long cacheGets = getCacheGets();
        if (cachePutTimeTakenNanos == 0 || cacheGets == 0) {
            return 0;
        }
        return ((1f * cachePutTimeTakenNanos) / cacheGets) / NANOSECONDS_IN_A_MICROSECOND;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getAverageRemoveTime() {
        final long cacheRemoveTimeTakenNanos = getCacheRemoveTimeTakenNanos();
        final long cacheGets = getCacheGets();
        if (cacheRemoveTimeTakenNanos == 0 || cacheGets == 0) {
            return 0;
        }
        return ((1f * cacheRemoveTimeTakenNanos) / cacheGets) / NANOSECONDS_IN_A_MICROSECOND;
    }

    /**
     * implementation of {@link javax.cache.management.CacheStatisticsMXBean#clear()}
     * @see javax.cache.management.CacheStatisticsMXBean#clear()
     */
    public void clear() {
        puts.set(0);
        misses.set(0);
        removals.set(0);
        expiries.set(0);
        hits.set(0);
        evictions.set(0);
        getCacheTimeTakenNanos.set(0);
        putTimeTakenNanos.set(0);
        removeTimeTakenNanos.set(0);
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheRemovals(long number) {
        removals.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheExpiries(long number) {
        expiries.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCachePuts(long number) {
        puts.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheHits(long number) {
        hits.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheMisses(long number) {
        misses.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheEvictions(long number) {
        evictions.getAndAdd(number);
    }

    /**
     * Increments the getCache time accumulator
     *
     * @param duration the time taken in nanoseconds
     */
    public void addGetTimeNano(long duration) {
        if (getCacheTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            getCacheTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            getCacheTimeTakenNanos.set(duration);
        }
    }

    /**
     * Increments the put time accumulator
     *
     * @param duration the time taken in nanoseconds
     */
    public void addPutTimeNano(long duration) {
        if (putTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            putTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            putTimeTakenNanos.set(duration);
        }
    }

    /**
     * Increments the remove time accumulator
     *
     * @param duration the time taken in nanoseconds
     */
    public void addRemoveTimeNano(long duration) {
        if (removeTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            removeTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            removeTimeTakenNanos.set(duration);
        }
    }

    /**
     *
     * Simple CacheStatistics adder. Can be used to merge two statistics data,
     * such as the ones collected from multiple nodes.
     * @param other CacheStatisticsImpl to be merged
     * @return CacheStatisticsImpl with merged data
     */
    public CacheStatisticsImpl accumulate(CacheStatisticsImpl other) {
        puts.addAndGet(other.getCachePuts());
        removals.addAndGet(other.getCacheRemovals());
        expiries.addAndGet(other.getCacheExpiries());
        evictions.addAndGet(other.getCacheEvictions());
        hits.addAndGet(other.getCacheHits());
        misses.addAndGet(other.getCacheMisses());
        putTimeTakenNanos.addAndGet(other.getCachePutTimeTakenNanos());
        getCacheTimeTakenNanos.addAndGet(other.getCacheGetTimeTakenNanos());
        removeTimeTakenNanos.addAndGet(other.getCacheRemoveTimeTakenNanos());
        return this;
    }

    /**
     *
     *
     * @param out output
     * @throws IOException
     */
    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeLong(puts.get());
        out.writeLong(removals.get());
        out.writeLong(expiries.get());
        out.writeLong(evictions.get());

        out.writeLong(hits.get());
        out.writeLong(misses.get());

        out.writeLong(putTimeTakenNanos.get());
        out.writeLong(getCacheTimeTakenNanos.get());
        out.writeLong(removeTimeTakenNanos.get());
    }

    /**
     * {@inheritDoc}
     * @param in input
     * @throws IOException
     */
    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        puts.set(in.readLong());
        removals.set(in.readLong());
        expiries.set(in.readLong());
        evictions.set(in.readLong());

        hits.set(in.readLong());
        misses.set(in.readLong());

        putTimeTakenNanos.set(in.readLong());
        getCacheTimeTakenNanos.set(in.readLong());
        removeTimeTakenNanos.set(in.readLong());
    }
}
