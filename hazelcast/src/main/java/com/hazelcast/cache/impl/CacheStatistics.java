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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;


public class CacheStatistics implements DataSerializable {

    private static final long NANOSECONDS_IN_A_MICROSECOND = 1000L;


    private final AtomicLong removals = new AtomicLong();
    private final AtomicLong expiries = new AtomicLong();
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();
    private final AtomicLong putTimeTakenNanos = new AtomicLong();
    private final AtomicLong getTimeTakenNanos = new AtomicLong();
    private final AtomicLong removeTimeTakenNanos = new AtomicLong();

    public CacheStatistics() {
    }

    public long getRemovals() {
        return removals.get();
    }

    public long getExpiries() {
        return expiries.get();
    }

    public long getPuts() {
        return puts.get();
    }

    public long getHits() {
        return hits.get();
    }

    public long getMisses() {
        return misses.get();
    }

    public long getEvictions() {
        return evictions.get();
    }

    public long getPutTimeTakenNanos() {
        return putTimeTakenNanos.get();
    }

    public long getGetTimeTakenNanos() {
        return getTimeTakenNanos.get();
    }

    public long getRemoveTimeTakenNanos() {
        return removeTimeTakenNanos.get();
    }

    public void clear() {
        puts.set(0);
        misses.set(0);
        removals.set(0);
        expiries.set(0);
        hits.set(0);
        evictions.set(0);
        getTimeTakenNanos.set(0);
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
     * Increments the get time accumulator
     *
     * @param duration the time taken in nanoseconds
     */
    public void addGetTimeNano(long duration) {
        if (getTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            getTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            getTimeTakenNanos.set(duration);
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

    public CacheStatistics acumulate(CacheStatistics other) {
        puts.addAndGet(other.getPuts());
        removals.set(other.getRemovals());
        expiries.set(other.getExpiries());
        evictions.set(other.getEvictions());

        hits.set(other.getHits());
        misses.set(other.getMisses());

        putTimeTakenNanos.set(other.getPutTimeTakenNanos());
        getTimeTakenNanos.set(other.getGetTimeTakenNanos());
        removeTimeTakenNanos.set(other.getRemoveTimeTakenNanos());
        return this;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(puts.get());
        out.writeLong(removals.get());
        out.writeLong(expiries.get());
        out.writeLong(evictions.get());

        out.writeLong(hits.get());
        out.writeLong(misses.get());

        out.writeLong(putTimeTakenNanos.get());
        out.writeLong(getTimeTakenNanos.get());
        out.writeLong(removeTimeTakenNanos.get());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        puts.set(in.readLong());
        removals.set(in.readLong());
        expiries.set(in.readLong());
        evictions.set(in.readLong());

        hits.set(in.readLong());
        misses.set(in.readLong());

        putTimeTakenNanos.set(in.readLong());
        getTimeTakenNanos.set(in.readLong());
        removeTimeTakenNanos.set(in.readLong());
    }
}
