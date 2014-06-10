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

package com.hazelcast.cache;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import javax.cache.Cache;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;


public class CacheStatistics implements DataSerializable {

    private static final long NANOSECONDS_IN_A_MICROSECOND = 1000L;


    private final AtomicLong cacheRemovals = new AtomicLong();
    private final AtomicLong cacheExpiries = new AtomicLong();
    private final AtomicLong cachePuts = new AtomicLong();
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final AtomicLong cacheEvictions = new AtomicLong();
    private final AtomicLong cachePutTimeTakenNanos = new AtomicLong();
    private final AtomicLong cacheGetTimeTakenNanos = new AtomicLong();
    private final AtomicLong cacheRemoveTimeTakenNanos = new AtomicLong();


    public void clear() {
        cachePuts.set(0);
        cacheMisses.set(0);
        cacheRemovals.set(0);
        cacheExpiries.set(0);
        cacheHits.set(0);
        cacheEvictions.set(0);
        cacheGetTimeTakenNanos.set(0);
        cachePutTimeTakenNanos.set(0);
        cacheRemoveTimeTakenNanos.set(0);
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheRemovals(long number) {
        cacheRemovals.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheExpiries(long number) {
        cacheExpiries.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCachePuts(long number) {
        cachePuts.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheHits(long number) {
        cacheHits.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheMisses(long number) {
        cacheMisses.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     *
     * @param number the number to increase the counter by
     */
    public void increaseCacheEvictions(long number) {
        cacheEvictions.getAndAdd(number);
    }

    /**
     * Increments the get time accumulator
     *
     * @param duration the time taken in nanoseconds
     */
    public void addGetTimeNano(long duration) {
        if (cacheGetTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            cacheGetTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            cacheGetTimeTakenNanos.set(duration);
        }
    }


    /**
     * Increments the put time accumulator
     *
     * @param duration the time taken in nanoseconds
     */
    public void addPutTimeNano(long duration) {
        if (cachePutTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            cachePutTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            cachePutTimeTakenNanos.set(duration);
        }
    }

    /**
     * Increments the remove time accumulator
     *
     * @param duration the time taken in nanoseconds
     */
    public void addRemoveTimeNano(long duration) {
        if (cacheRemoveTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            cacheRemoveTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            cacheRemoveTimeTakenNanos.set(duration);
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {

    }
}
