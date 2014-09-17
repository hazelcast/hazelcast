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

    @Override
    public long getCacheRemovals() {
        return removals.get();
    }

    public long getCacheExpiries() {
        return expiries.get();
    }

    @Override
    public long getCacheGets() {
        return getCacheHits() + getCacheMisses();
    }

    @Override
    public long getCachePuts() {
        return puts.get();
    }

    @Override
    public long getCacheHits() {
        return hits.get();
    }

    @Override
    public long getCacheMisses() {
        return misses.get();
    }

    @Override
    public long getCacheEvictions() {
        return evictions.get();
    }

    public long getCachePutTimeTakenNanos() {
        return putTimeTakenNanos.get();
    }

    public long getCacheGetTimeTakenNanos() {
        return getCacheTimeTakenNanos.get();
    }

    public long getCacheRemoveTimeTakenNanos() {
        return removeTimeTakenNanos.get();
    }


    @Override
    public float getCacheHitPercentage() {
        Long hits = getCacheHits();
        if (hits == 0) {
            return 0;
        }
        return (float) hits / getCacheGets() * FLOAT_HUNDRED;
    }


    @Override
    public float getCacheMissPercentage() {
        Long misses = getCacheMisses();
        if (misses == 0) {
            return 0;
        }
        return (float) misses / getCacheGets() * FLOAT_HUNDRED;
    }


    @Override
    public float getAverageGetTime() {
        if (getCacheGetTimeTakenNanos() == 0 || getCacheGets() == 0) {
            return 0;
        }
        float avgGetTime = ((1f * getCacheGetTimeTakenNanos()) / getCacheGets()) / NANOSECONDS_IN_A_MICROSECOND;
        return avgGetTime;
    }

    @Override
    public float getAveragePutTime() {
        if (getCachePutTimeTakenNanos() == 0 || getCacheGets() == 0) {
            return 0;
        }
        float avgPutTime = ((1f * getCachePutTimeTakenNanos()) / getCacheGets()) / NANOSECONDS_IN_A_MICROSECOND;
        return avgPutTime;
    }

    @Override
    public float getAverageRemoveTime() {
        if (getCacheRemoveTimeTakenNanos() == 0 || getCacheGets() == 0) {
            return 0;
        }
        float avgRemoveTime = ((1f * getCacheRemoveTimeTakenNanos()) / getCacheGets()) / NANOSECONDS_IN_A_MICROSECOND;
        return avgRemoveTime;
    }

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

    public CacheStatisticsImpl acumulate(CacheStatisticsImpl other) {
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
