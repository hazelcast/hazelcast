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

import com.hazelcast.monitor.LocalSemaphoreOperationStats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SemaphoreOperationsCounter {
    private OperationCounter modified = new OperationCounter();
    private OperationCounter nonModified = new OperationCounter();
    private OperationCounter acquires = new OperationCounter();
    private OperationCounter nonAcquires = new OperationCounter();
    private AtomicLong rejectedAcquires = new AtomicLong();
    private AtomicLong permitsAcquired = new AtomicLong();
    private AtomicLong permitsReleased = new AtomicLong();
    private AtomicLong permitsAttached = new AtomicLong();
    private AtomicLong permitsDetached = new AtomicLong();
    private AtomicLong permitsReduced = new AtomicLong();
    private long startTime = now();
    private long endTime = Long.MAX_VALUE;
    private transient LocalSemaphoreOperationStatsImpl published = null;
    private List<SemaphoreOperationsCounter> listOfSubStats = new ArrayList<SemaphoreOperationsCounter>();
    final private static LocalSemaphoreOperationStats empty = new LocalSemaphoreOperationStatsImpl();
    final private Object lock = new Object();

    final private long interval;

    public SemaphoreOperationsCounter() {
        this(5000);
    }

    public SemaphoreOperationsCounter(long interval) {
        this.interval = interval;
    }

    LocalSemaphoreOperationStats getPublishedStats() {
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

    void incrementAcquires(long elapsed, int permits, boolean attached) {
        acquires.count(elapsed);
        permitsAcquired.addAndGet(permits);
        if (attached)
            permitsAttached.addAndGet(permits);
        publishSubResult();
    }

    void incrementRejectedAcquires(long elapsed) {
        acquires.count(elapsed);
        rejectedAcquires.incrementAndGet();
        publishSubResult();
    }

    void incrementReleases(long elapsed, int permits, boolean detached) {
        permitsReleased.addAndGet(permits);
        if(detached)
            incrementNonAcquires(elapsed, -permits);
        else
            incrementNonAcquires(elapsed, 0);
    }

    void incrementNonAcquires(long elapsed, int attachedDelta) {
        nonAcquires.count(elapsed);
        if (attachedDelta > 0) {
            permitsAttached.addAndGet(attachedDelta);
        } else if (attachedDelta < 0) {
            permitsDetached.addAndGet(-attachedDelta);
        }
        publishSubResult();
    }

    public void incrementPermitsReduced(long elapsed, int reduction) {
        permitsReduced.addAndGet(reduction);
        incrementNonAcquires(elapsed, 0);
    }

    private long now() {
        return System.currentTimeMillis();
    }

    private void publishSubResult() {
        long subInterval = interval / 5;
        if (now() - startTime > subInterval) {
            synchronized (lock) {
                if (now() - startTime >= subInterval) {
                    SemaphoreOperationsCounter copy = getAndReset();
                    if (listOfSubStats.size() == 5) {
                        listOfSubStats.remove(0);
                    }
                    listOfSubStats.add(copy);
                    this.published = aggregate(listOfSubStats);
                }
            }
        }
    }

    private LocalSemaphoreOperationStatsImpl aggregate(List<SemaphoreOperationsCounter> list) {
        LocalSemaphoreOperationStatsImpl stats = new LocalSemaphoreOperationStatsImpl();
        stats.periodStart = list.get(0).startTime;
        for (SemaphoreOperationsCounter sub : list) {
            stats.acquires.add(sub.acquires.count.get(), sub.acquires.totalLatency.get());
            stats.nonAcquires.add(sub.nonAcquires.count.get(), sub.nonAcquires.totalLatency.get());
            stats.numberOfRejectedAcquires += sub.rejectedAcquires.get();
            stats.numberOfPermitsAcquired += sub.permitsAcquired.get();
            stats.numberOfPermitsReleased += sub.permitsReleased.get();
            stats.numberOfPermitsAttached += sub.permitsAttached.get();
            stats.numberOfPermitsDetached += sub.permitsDetached.get();
            stats.numberOfPermitsReduced += sub.permitsReduced.get();
            stats.periodEnd = sub.endTime;
        }
        return stats;
    }

    private SemaphoreOperationsCounter getAndReset() {
        SemaphoreOperationsCounter newOne = new SemaphoreOperationsCounter();
        newOne.acquires.set(acquires.copyAndReset());
        newOne.nonAcquires.set(nonAcquires.copyAndReset());
        newOne.rejectedAcquires.set(rejectedAcquires.getAndSet(0));
        newOne.permitsAcquired.set(permitsAcquired.getAndSet(0));
        newOne.permitsReleased.set(permitsReleased.getAndSet(0));
        newOne.permitsAttached.set(permitsAttached.getAndSet(0));
        newOne.permitsDetached.set(permitsDetached.getAndSet(0));
        newOne.permitsReduced.set(permitsReduced.getAndSet(0));
        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    private LocalSemaphoreOperationStatsImpl getThis() {
        LocalSemaphoreOperationStatsImpl stats = new LocalSemaphoreOperationStatsImpl();
        stats.periodStart = this.startTime;
        stats.acquires = stats.new OperationStat(this.acquires.count.get(), this.acquires.totalLatency.get());
        stats.nonAcquires = stats.new OperationStat(this.nonAcquires.count.get(), this.nonAcquires.totalLatency.get());
        stats.numberOfRejectedAcquires = this.rejectedAcquires.get();
        stats.numberOfPermitsAcquired = this.permitsAcquired.get();
        stats.numberOfPermitsReleased = this.permitsReleased.get();
        stats.numberOfPermitsAttached = this.permitsAttached.get();
        stats.numberOfPermitsDetached = this.permitsDetached.get();
        stats.numberOfPermitsReduced = this.permitsReduced.get();
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
