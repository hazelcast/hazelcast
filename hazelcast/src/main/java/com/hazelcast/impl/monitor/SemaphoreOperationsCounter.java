/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.monitor;

import com.hazelcast.monitor.LocalSemaphoreOperationStats;

import java.util.concurrent.atomic.AtomicLong;

public class SemaphoreOperationsCounter extends OperationsCounterSupport<LocalSemaphoreOperationStats> {

    final private static LocalSemaphoreOperationStats empty = new LocalSemaphoreOperationStatsImpl();

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

    public SemaphoreOperationsCounter() {
        super();
    }

    public SemaphoreOperationsCounter(long interval) {
        super(interval);
    }

    public void incrementModified(long elapsed) {
        modified.count(elapsed);
        publishSubResult();
    }

    public void incrementNonModified(long elapsed) {
        nonModified.count(elapsed);
        publishSubResult();
    }

    public void incrementAcquires(long elapsed, int permits, boolean attached) {
        acquires.count(elapsed);
        permitsAcquired.addAndGet(permits);
        if (attached)
            permitsAttached.addAndGet(permits);
        publishSubResult();
    }

    public void incrementRejectedAcquires(long elapsed) {
        acquires.count(elapsed);
        rejectedAcquires.incrementAndGet();
        publishSubResult();
    }

    public void incrementReleases(long elapsed, int permits, boolean detached) {
        permitsReleased.addAndGet(permits);
        if (detached)
            incrementNonAcquires(elapsed, -permits);
        else
            incrementNonAcquires(elapsed, 0);
    }

    public void incrementNonAcquires(long elapsed, int attachedDelta) {
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

    LocalSemaphoreOperationStatsImpl aggregateSubCounterStats() {
        LocalSemaphoreOperationStatsImpl stats = new LocalSemaphoreOperationStatsImpl();
        stats.periodStart = ((SemaphoreOperationsCounter) listOfSubCounters.get(0)).startTime;
        for (Object obj : listOfSubCounters) {
            SemaphoreOperationsCounter sub = (SemaphoreOperationsCounter) obj;
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

    SemaphoreOperationsCounter getAndReset() {
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

    LocalSemaphoreOperationStats getThis() {
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

    LocalSemaphoreOperationStats getEmpty() {
        return empty;
    }
}
