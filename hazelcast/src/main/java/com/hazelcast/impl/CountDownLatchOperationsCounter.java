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

import com.hazelcast.monitor.LocalCountDownLatchOperationStats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class CountDownLatchOperationsCounter {
    private AtomicLong awaitsReleased = new AtomicLong();
    private AtomicLong gatesOpened = new AtomicLong();
    private OperationCounter await = new OperationCounter();
    private OperationCounter countdown = new OperationCounter();
    private OperationCounter other = new OperationCounter();
    private long startTime = now();
    private long endTime = Long.MAX_VALUE;
    private transient LocalCountDownLatchOperationStatsImpl published = null;
    private List<CountDownLatchOperationsCounter> listOfSubStats = new ArrayList<CountDownLatchOperationsCounter>();
    final private static LocalCountDownLatchOperationStats empty = new LocalCountDownLatchOperationStatsImpl();
    final private Object lock = new Object();

    final private long interval;

    public CountDownLatchOperationsCounter() {
        this(5000);
    }

    public CountDownLatchOperationsCounter(long interval) {
        this.interval = interval;
    }

    LocalCountDownLatchOperationStats getPublishedStats() {
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

    void incrementAwait(long elapsed) {
        await.count(elapsed);
        publishSubResult();
    }

    void incrementCountDown(long elapsed, int releasedThreads) {
        countdown.count(elapsed);
        if(releasedThreads > 0){
            awaitsReleased.addAndGet(releasedThreads);
            gatesOpened.incrementAndGet();
        }
        publishSubResult();
    }

    void incrementOther(long elapsed) {
        other.count(elapsed);
        publishSubResult();
    }

    private long now() {
        return System.currentTimeMillis();
    }

    private void publishSubResult() {
        long subInterval = interval / 5;
        if (now() - startTime > subInterval) {
            synchronized (lock) {
                if (now() - startTime >= subInterval) {
                    CountDownLatchOperationsCounter copy = getAndReset();
                    if (listOfSubStats.size() == 5) {
                        listOfSubStats.remove(0);
                    }
                    listOfSubStats.add(copy);
                    this.published = aggregate(listOfSubStats);
                }
            }
        }
    }

    private LocalCountDownLatchOperationStatsImpl aggregate(List<CountDownLatchOperationsCounter> list) {
        LocalCountDownLatchOperationStatsImpl stats = new LocalCountDownLatchOperationStatsImpl();
        stats.periodStart = list.get(0).startTime;
        for (CountDownLatchOperationsCounter sub : list) {
            stats.await.add(sub.await.count.get(), sub.await.totalLatency.get());
            stats.countdown.add(sub.countdown.count.get(), sub.countdown.totalLatency.get());
            stats.other.add(sub.other.count.get(), sub.other.totalLatency.get());
            stats.numberOfAwaitsReleased += sub.awaitsReleased.get();
            stats.numberOfGatesOpened += sub.gatesOpened.get();
            stats.periodEnd = sub.endTime;
        }
        return stats;
    }

    private CountDownLatchOperationsCounter getAndReset() {
        CountDownLatchOperationsCounter newOne = new CountDownLatchOperationsCounter();
        newOne.await.set(await.copyAndReset());
        newOne.countdown.set(countdown.copyAndReset());
        newOne.other.set(other.copyAndReset());
        newOne.awaitsReleased.set(awaitsReleased.getAndSet(0));
        newOne.gatesOpened.set(gatesOpened.getAndSet(0));
        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    private LocalCountDownLatchOperationStatsImpl getThis() {
        LocalCountDownLatchOperationStatsImpl stats = new LocalCountDownLatchOperationStatsImpl();
        stats.periodStart = this.startTime;
        stats.await = stats.new OperationStat(this.await.count.get(), this.await.totalLatency.get());
        stats.countdown = stats.new OperationStat(this.countdown.count.get(), this.countdown.totalLatency.get());
        stats.other = stats.new OperationStat(this.other.count.get(), this.other.totalLatency.get());
        stats.numberOfAwaitsReleased = this.awaitsReleased.get();
        stats.numberOfGatesOpened = this.gatesOpened.get();
        stats.periodEnd = now();
        return stats;
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
            return "OperationStat{" + "count=" + count + ", averageLatency=" + ((count == 0) ? 0 : totalLatency.get() / count) + '}';
        }
    }
}
