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

package com.hazelcast.monitor.impl;

import com.hazelcast.monitor.LocalAtomicNumberOperationStats;

public class AtomicNumberOperationsCounter extends OperationsCounterSupport<LocalAtomicNumberOperationStats> {

    final private static LocalAtomicNumberOperationStats empty = new LocalAtomicNumberOperationStatsImpl();

    private OperationCounter modified = new OperationCounter();
    private OperationCounter nonModified = new OperationCounter();

    public AtomicNumberOperationsCounter() {
        super();
    }

    public AtomicNumberOperationsCounter(long interval) {
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

    LocalAtomicNumberOperationStats aggregateSubCounterStats() {
        LocalAtomicNumberOperationStatsImpl stats = new LocalAtomicNumberOperationStatsImpl();
        stats.periodStart = ((AtomicNumberOperationsCounter) listOfSubCounters.get(0)).startTime;
        for (Object obj : listOfSubCounters) {
            AtomicNumberOperationsCounter sub = (AtomicNumberOperationsCounter) obj;
            stats.modified.add(sub.modified.count.get(), sub.modified.totalLatency.get());
            stats.nonModified.add(sub.nonModified.count.get(), sub.nonModified.totalLatency.get());
            stats.periodEnd = sub.endTime;
        }
        return stats;
    }

    AtomicNumberOperationsCounter getAndReset() {
        AtomicNumberOperationsCounter newOne = new AtomicNumberOperationsCounter();
        newOne.modified.set(modified.copyAndReset());
        newOne.nonModified.set(nonModified.copyAndReset());
        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    LocalAtomicNumberOperationStats getThis() {
        LocalAtomicNumberOperationStatsImpl stats = new LocalAtomicNumberOperationStatsImpl();
        stats.periodStart = this.startTime;
        stats.modified = stats.new OperationStat(this.modified.count.get(), this.modified.totalLatency.get());
        stats.nonModified = stats.new OperationStat(this.nonModified.count.get(), this.nonModified.totalLatency.get());
        stats.periodEnd = now();
        return stats;
    }

    LocalAtomicNumberOperationStats getEmpty() {
        return empty;
    }
}
