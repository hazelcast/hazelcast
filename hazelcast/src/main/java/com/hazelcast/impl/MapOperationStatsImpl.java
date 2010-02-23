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

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MapOperationStatsImpl implements DataSerializable, MapOperationStats {
    private AtomicLong mapPuts = new AtomicLong();
    private AtomicLong mapGets = new AtomicLong();
    private AtomicLong mapRemoves = new AtomicLong();
    private long startTime = now();
    private long endTime = Long.MAX_VALUE;
    private transient MapOperationStatsImpl published = null;
    private List<MapOperationStatsImpl> listOfSubStats = new ArrayList<MapOperationStatsImpl>();
    final private Object lock = new Object();

    final private long interval;

    public MapOperationStatsImpl() {
        this(10000);
    }

    public MapOperationStatsImpl(long interval) {
        this.interval = interval;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeLong(mapPuts.get());
        out.writeLong(mapGets.get());
        out.writeLong(mapRemoves.get());
        out.writeLong(startTime);
        out.writeLong(endTime);
    }

    public void readData(DataInput in) throws IOException {
        mapPuts.set(in.readLong());
        mapGets.set(in.readLong());
        mapRemoves.set(in.readLong());
        startTime = in.readLong();
        endTime = in.readLong();
    }

    private MapOperationStatsImpl getAndReset() {
        long mapPutsNow = mapPuts.getAndSet(0);
        long mapGetsNow = mapGets.getAndSet(0);
        long mapRemovesNow = mapRemoves.getAndSet(0);
        MapOperationStatsImpl newOne = new MapOperationStatsImpl();
        newOne.mapPuts.set(mapPutsNow);
        newOne.mapGets.set(mapGetsNow);
        newOne.mapRemoves.set(mapRemovesNow);
        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    public MapOperationStatsImpl getPublishedStats() {
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

    long now() {
        return System.currentTimeMillis();
    }

    private void publishSubResult() {
        long subInterval = interval / 10;
        if (now() - startTime > subInterval) {
            synchronized (lock) {
                if (now() - startTime >= subInterval) {
                    MapOperationStatsImpl copy = getAndReset();
                    if (listOfSubStats.size() == 10) {
                        listOfSubStats.remove(0);
                    }
                    listOfSubStats.add(copy);
                    this.published = aggregate(listOfSubStats);
                }
            }
        }
    }

    private MapOperationStatsImpl aggregate(List<MapOperationStatsImpl> list) {
        MapOperationStatsImpl stats = new MapOperationStatsImpl();
        stats.startTime = list.get(0).startTime;
        for (int i = 0; i < list.size(); i++) {
            MapOperationStatsImpl sub = list.get(i);
            stats.mapGets.addAndGet(sub.mapGets.get());
            stats.mapPuts.addAndGet(sub.mapPuts.get());
            stats.mapRemoves.addAndGet(sub.mapRemoves.get());
            stats.endTime = sub.getPeriodEnd();
        }
        return stats;
    }

    private MapOperationStatsImpl getThis() {
        this.endTime = now();
        return this;
    }

    public long total() {
        return mapPuts.get() + mapGets.get() + mapRemoves.get();
    }

    public String toString() {
        return "MapOperationStats{" +
                "total= " + total() +
                ", puts:" + mapPuts.get() +
                ", gets:" + mapGets.get() +
                ", removes:" + mapRemoves.get() +
                "}";
    }

    public long getPeriodStart() {
        return startTime;
    }

    public long getPeriodEnd() {
        return endTime;
    }

    public long getNumberOfPuts() {
        return mapPuts.get();
    }

    public long getNumberOfGets() {
        return mapGets.get();
    }

    public long getNumberOfRemoves() {
        return mapRemoves.get();
    }
}
