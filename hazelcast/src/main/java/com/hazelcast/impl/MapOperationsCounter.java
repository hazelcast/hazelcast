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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MapOperationsCounter {
    private AtomicLong mapPuts = new AtomicLong();
    private AtomicLong mapGets = new AtomicLong();
    private AtomicLong mapRemoves = new AtomicLong();
    private AtomicLong mapOthers = new AtomicLong();
    private long startTime = now();
    private long endTime = Long.MAX_VALUE;
    private transient MapOperationStats published = null;
    private List<MapOperationsCounter> listOfSubStats = new ArrayList<MapOperationsCounter>();
    final private Object lock = new Object();

    final private long interval;

    public MapOperationsCounter() {
        this(10000);
    }

    public MapOperationsCounter(long interval) {
        this.interval = interval;
    }

    private MapOperationsCounter getAndReset() {
        long mapPutsNow = mapPuts.getAndSet(0);
        long mapGetsNow = mapGets.getAndSet(0);
        long mapRemovesNow = mapRemoves.getAndSet(0);
        long mapOthersNow = mapOthers.getAndSet(0);
        MapOperationsCounter newOne = new MapOperationsCounter();
        newOne.mapPuts.set(mapPutsNow);
        newOne.mapGets.set(mapGetsNow);
        newOne.mapRemoves.set(mapRemovesNow);
        newOne.mapOthers.set(mapOthersNow);
        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    public MapOperationStats getPublishedStats() {
        if (published == null) {
            synchronized (lock) {
                if (published == null) {
                    published = getThis();
                }
            }
        }
        return published;
    }

    public void incrementPuts() {
        mapPuts.incrementAndGet();
        publishSubResult();
    }

    public void incrementGets() {
        mapGets.incrementAndGet();
        publishSubResult();
    }

    public void incrementRemoves() {
        mapRemoves.incrementAndGet();
        publishSubResult();
    }

    public void incrementOtherOperations() {
        mapOthers.incrementAndGet();
        publishSubResult();
    }

    long now() {
        return System.currentTimeMillis();
    }

    private void publishSubResult() {
        long subInterval = interval / 10;
        if (now() - startTime > subInterval) {
            synchronized (lock) {
                if (now() - startTime >= subInterval) {
                    MapOperationsCounter copy = getAndReset();
                    if (listOfSubStats.size() == 10) {
                        listOfSubStats.remove(0);
                    }
                    listOfSubStats.add(copy);
                    this.published = aggregate(listOfSubStats);
                }
            }
        }
    }

    private MapOperationStats aggregate(List<MapOperationsCounter> list) {
        MapOperationStatsImpl stats = new MapOperationStatsImpl();
        stats.periodStart = list.get(0).startTime;
        for (int i = 0; i < list.size(); i++) {
            MapOperationsCounter sub = list.get(i);
            stats.numberOfGets += sub.mapGets.get();
            stats.numberOfPuts += sub.mapPuts.get();
            stats.numberOfRemoves += sub.mapRemoves.get();
            stats.numberOfOtherOperations += sub.mapOthers.get();
            stats.periodEnd = sub.endTime;
        }
        return stats;
    }

    private MapOperationStats getThis() {
        MapOperationStatsImpl stats = new MapOperationStatsImpl();
        stats.periodStart = this.startTime;
        stats.numberOfGets = this.mapGets.get();
        stats.numberOfPuts = this.mapPuts.get();
        stats.numberOfRemoves = this.mapRemoves.get();
        stats.periodEnd = now();
        return stats;
    }
}
