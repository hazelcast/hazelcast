/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.monitor.impl;

import com.hazelcast.util.Clock;
import com.hazelcast.monitor.LocalInstanceOperationStats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

abstract class OperationsCounterSupport<T extends LocalInstanceOperationStats> {

    final long interval;
    final Object lock = new Object();
    long startTime = now();
    long endTime = Long.MAX_VALUE;
    transient volatile T published = null;

    final List listOfSubCounters = new ArrayList();

    public OperationsCounterSupport() {
        this(5000);
    }

    public OperationsCounterSupport(long interval) {
        super();
        this.interval = interval;
    }

    public final T getPublishedStats() {
        //noinspection DoubleCheckedLocking
        if (published == null) {
            synchronized (lock) {
                if (published == null) {
                    published = getThis();
                }
            }
        }
        if (published.getPeriodEnd() < now() - interval) {
            return getEmpty();
        }
        return published;
    }

    final void publishSubResult() {
        long subInterval = interval / 5;
        if (now() - startTime > subInterval) {
            synchronized (lock) {
                if (now() - startTime >= subInterval) {
                    OperationsCounterSupport<T> copy = getAndReset();
                    if (listOfSubCounters.size() == 5) {
                        listOfSubCounters.remove(0);
                    }
                    listOfSubCounters.add(copy);
                    this.published = aggregateSubCounterStats();
                }
            }
        }
    }

    abstract T getThis();

    abstract T getEmpty();

    abstract OperationsCounterSupport<T> getAndReset();

    abstract T aggregateSubCounterStats();

    final long now() {
        return Clock.currentTimeMillis();
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
            OperationCounter copy = new OperationCounter(count.get(),
                    totalLatency.get());
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
            return "OperationStat{" + "count=" + count + ", averageLatency="
                    + ((count == 0) ? 0 : totalLatency.get() / count) + '}';
        }
    }
}
