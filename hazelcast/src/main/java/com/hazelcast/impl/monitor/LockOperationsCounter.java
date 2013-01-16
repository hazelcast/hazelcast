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

package com.hazelcast.impl.monitor;

import com.hazelcast.monitor.LocalLockOperationStats;

import java.util.concurrent.atomic.AtomicLong;

public class LockOperationsCounter extends OperationsCounterSupport<LocalLockOperationStats> {
    private static final LocalLockOperationStats empty = new LocalLockOperationStatsImpl();

    private AtomicLong locks = new AtomicLong();
    private AtomicLong unlocks = new AtomicLong();
    private AtomicLong failedLocks = new AtomicLong();

    public LockOperationsCounter() {
        super();
    }

    public LockOperationsCounter(long interval) {
        super(interval);
    }

    LockOperationsCounter getAndReset() {
        LockOperationsCounter newOne = new LockOperationsCounter();
        newOne.locks.set(locks.getAndSet(0));
        newOne.unlocks.set(unlocks.getAndSet(0));
        newOne.failedLocks.set(failedLocks.getAndSet(0));
        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    public void incrementLocks() {
        locks.incrementAndGet();
        publishSubResult();
    }

    public void incrementUnlocks() {
        unlocks.incrementAndGet();
        publishSubResult();
    }

    public void incrementFailedLocks() {
        failedLocks.incrementAndGet();
        publishSubResult();
    }

    LocalLockOperationStats aggregateSubCounterStats() {
        LocalLockOperationStatsImpl stats = new LocalLockOperationStatsImpl();
        stats.periodStart = ((LockOperationsCounter) listOfSubCounters.get(0)).startTime;
        for (int i = 0; i < listOfSubCounters.size(); i++) {
            LockOperationsCounter sub = (LockOperationsCounter) listOfSubCounters.get(i);
            stats.numberOfLocks += sub.locks.get();
            stats.numberOfUnlocks += sub.unlocks.get();
            stats.numberOfFailedLocks += sub.failedLocks.get();
            stats.periodEnd = sub.endTime;
        }
        return stats;
    }

    LocalLockOperationStats getThis() {
        LocalLockOperationStatsImpl stats = new LocalLockOperationStatsImpl();
        stats.periodStart = this.startTime;
        stats.numberOfLocks = this.locks.get();
        stats.numberOfUnlocks = this.unlocks.get();
        stats.numberOfFailedLocks = this.failedLocks.get();
        stats.periodEnd = now();
        return stats;
    }

    LocalLockOperationStats getEmpty() {
        return empty;
    }
}
