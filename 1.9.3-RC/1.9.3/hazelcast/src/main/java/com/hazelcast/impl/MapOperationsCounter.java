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

import com.hazelcast.monitor.LocalMapOperationStats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MapOperationsCounter {
    private final static LocalMapOperationStats empty = new MapOperationStatsImpl();
    private final OperationCounter puts = new OperationCounter();
    private final OperationCounter gets = new OperationCounter();
    private final AtomicLong removes = new AtomicLong();
    private final AtomicLong others = new AtomicLong();
    private final AtomicLong events = new AtomicLong();

    private final List<MapOperationsCounter> listOfSubStats = new ArrayList<MapOperationsCounter>();
    private final long interval;
    private final Object lock = new Object();
    private volatile LocalMapOperationStats published = null;
    private long startTime = now();
    private long endTime = Long.MAX_VALUE;

    public MapOperationsCounter() {
        this(5000);
    }

    public MapOperationsCounter(long interval) {
        this.interval = interval;
    }

    private MapOperationsCounter getAndReset() {
        OperationCounter putsNow = puts.copyAndReset();
        OperationCounter getsNow = gets.copyAndReset();
        long removesNow = removes.getAndSet(0);
        long othersNow = others.getAndSet(0);
        long eventsNow = events.getAndSet(0);
        MapOperationsCounter newOne = new MapOperationsCounter();
        newOne.puts.set(putsNow);
        newOne.gets.set(getsNow);
        newOne.removes.set(removesNow);
        newOne.others.set(othersNow);
        newOne.events.set(eventsNow);
        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    public LocalMapOperationStats getPublishedStats() {
        if (published == null) {
            synchronized (lock) {
                if (published == null) {
                    published = getThis();
                }
            }
        }
        if (published.getPeriodEnd() < now() - interval) {
            return empty;
        }
        return published;
    }

    public void incrementPuts(long elapsed) {
        puts.count(elapsed);
        publishSubResult();
    }

    public void incrementGets(long elapsed) {
        gets.count(elapsed);
        publishSubResult();
    }

    public void incrementRemoves() {
        removes.incrementAndGet();
        publishSubResult();
    }

    public void incrementOtherOperations() {
        others.incrementAndGet();
        publishSubResult();
    }

    public void incrementReceivedEvents() {
        events.incrementAndGet();
        publishSubResult();
    }

    long now() {
        return System.currentTimeMillis();
    }

    private void publishSubResult() {
        long subInterval = interval / 5;
        if (now() - startTime > subInterval) {
            synchronized (lock) {
                if (now() - startTime >= subInterval) {
                    MapOperationsCounter copy = getAndReset();
                    if (listOfSubStats.size() == 5) {
                        listOfSubStats.remove(0);
                    }
                    listOfSubStats.add(copy);
                    this.published = aggregate(listOfSubStats);
                }
            }
        }
    }

    private LocalMapOperationStats aggregate(List<MapOperationsCounter> list) {
        MapOperationStatsImpl stats = new MapOperationStatsImpl();
        stats.periodStart = list.get(0).startTime;
        for (int i = 0; i < list.size(); i++) {
            MapOperationsCounter sub = list.get(i);
            stats.gets.add(sub.gets.count.get(), sub.gets.totalLatency.get());
            stats.puts.add(sub.puts.count.get(), sub.puts.totalLatency.get());
            stats.numberOfRemoves += sub.removes.get();
            stats.numberOfOtherOperations += sub.others.get();
            stats.numberOfEvents += sub.events.get();
            stats.periodEnd = sub.endTime;
        }
        return stats;
    }

    private LocalMapOperationStats getThis() {
        MapOperationStatsImpl stats = new MapOperationStatsImpl();
        stats.periodStart = this.startTime;
        stats.gets = stats.new OperationStat(this.gets.count.get(), this.gets.totalLatency.get());
        stats.puts = stats.new OperationStat(this.puts.count.get(), this.puts.totalLatency.get());
//        stats.numberOfPuts = this.puts.get();
        stats.numberOfRemoves = this.removes.get();
        stats.numberOfEvents = this.events.get();
        stats.periodEnd = now();
        return stats;
    }

    @Override
    public String toString() {
        return "MapOperationsCounter{" +
                "empty=" + empty +
                ", puts=" + puts +
                ", gets=" + gets +
                ", removes=" + removes +
                ", others=" + others +
                ", events=" + events +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", published=" + published +
                ", listOfSubStats=" + listOfSubStats +
                ", lock=" + lock +
                ", interval=" + interval +
                '}';
    }

    class OperationCounter {
        final AtomicLong count;
        final AtomicLong totalLatency;

        public OperationCounter() {
            this(0, 0);
        }

        public OperationCounter(long c, long l) {
            this.count = new AtomicLong(c);
            totalLatency = new AtomicLong(l);
        }

        public OperationCounter copyAndReset() {
            OperationCounter copy = new OperationCounter(count.get(), totalLatency.get());
            this.count.set(0);
            this.totalLatency.set(0);
            return copy;
        }

        public void set(OperationCounter now) {
            this.count.set(now.count.get());
            this.totalLatency.set(now.totalLatency.get());
        }

        public void count(long elapsed) {
            this.count.incrementAndGet();
            this.totalLatency.addAndGet(elapsed);
        }

        @Override
        public String toString() {
            long count = this.count.get();
            return "OperationStat{" +
                    "count=" + count +
                    ", averageLatency=" + ((count==0)?0:totalLatency.get() / count) +
                    '}';
        }
    }
}
