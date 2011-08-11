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

import com.hazelcast.monitor.LocalAtomicNumberOperationStats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class AtomicNumberOperationsCounter {
    private OperationCounter modified = new OperationCounter();
    private OperationCounter nonModified = new OperationCounter();
    private long startTime = now();
    private long endTime = Long.MAX_VALUE;
    private transient LocalAtomicNumberOperationStatsImpl published = null;
    private List<AtomicNumberOperationsCounter> listOfSubStats = new ArrayList<AtomicNumberOperationsCounter>();
    final private static LocalAtomicNumberOperationStats empty = new LocalAtomicNumberOperationStatsImpl();
    final private Object lock = new Object();

    final private long interval;

    public AtomicNumberOperationsCounter() {
        this(5000);
    }

    public AtomicNumberOperationsCounter(long interval) {
        this.interval = interval;
    }

    LocalAtomicNumberOperationStats getPublishedStats() {
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

    void incrementModified(long elapsed) {
        modified.count(elapsed);
        publishSubResult();
    }

    void incrementNonModified(long elapsed) {
        nonModified.count(elapsed);
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
                    AtomicNumberOperationsCounter copy = getAndReset();
                    if (listOfSubStats.size() == 5) {
                        listOfSubStats.remove(0);
                    }
                    listOfSubStats.add(copy);
                    this.published = aggregate(listOfSubStats);
                }
            }
        }
    }

    private LocalAtomicNumberOperationStatsImpl aggregate(List<AtomicNumberOperationsCounter> list) {
        LocalAtomicNumberOperationStatsImpl stats = new LocalAtomicNumberOperationStatsImpl();
        stats.periodStart = list.get(0).startTime;
        for (AtomicNumberOperationsCounter sub : list) {
            stats.modified.add(sub.modified.count.get(), sub.modified.totalLatency.get());
            stats.nonModified.add(sub.nonModified.count.get(), sub.nonModified.totalLatency.get());
            stats.periodEnd = sub.endTime;
        }
        return stats;
    }

    private AtomicNumberOperationsCounter getAndReset() {
        AtomicNumberOperationsCounter newOne = new AtomicNumberOperationsCounter();
        newOne.modified.set(modified.copyAndReset());
        newOne.nonModified.set(nonModified.copyAndReset());
        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    private LocalAtomicNumberOperationStatsImpl getThis() {
        LocalAtomicNumberOperationStatsImpl stats = new LocalAtomicNumberOperationStatsImpl();
        stats.periodStart = this.startTime;
        stats.modified = stats.new OperationStat(this.modified.count.get(), this.modified.totalLatency.get());
        stats.nonModified = stats.new OperationStat(this.nonModified.count.get(), this.nonModified.totalLatency.get());
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
