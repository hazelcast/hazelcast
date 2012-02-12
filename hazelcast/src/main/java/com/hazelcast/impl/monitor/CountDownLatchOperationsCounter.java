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

package com.hazelcast.impl.monitor;

import com.hazelcast.monitor.LocalCountDownLatchOperationStats;

import java.util.concurrent.atomic.AtomicLong;

public class CountDownLatchOperationsCounter extends OperationsCounterSupport<LocalCountDownLatchOperationStats> {

    final private static LocalCountDownLatchOperationStats empty = new LocalCountDownLatchOperationStatsImpl();

    private AtomicLong awaitsReleased = new AtomicLong();
    private AtomicLong gatesOpened = new AtomicLong();
    private OperationCounter await = new OperationCounter();
    private OperationCounter countdown = new OperationCounter();
    private OperationCounter other = new OperationCounter();

    public CountDownLatchOperationsCounter() {
        super();
    }

    public CountDownLatchOperationsCounter(long interval) {
        super(interval);
    }

    public void incrementAwait(long elapsed) {
        await.count(elapsed);
        publishSubResult();
    }

    public void incrementCountDown(long elapsed, int releasedThreads) {
        countdown.count(elapsed);
        if (releasedThreads > 0) {
            awaitsReleased.addAndGet(releasedThreads);
            gatesOpened.incrementAndGet();
        }
        publishSubResult();
    }

    public void incrementOther(long elapsed) {
        other.count(elapsed);
        publishSubResult();
    }

    LocalCountDownLatchOperationStatsImpl aggregateSubCounterStats() {
        LocalCountDownLatchOperationStatsImpl stats = new LocalCountDownLatchOperationStatsImpl();
        stats.periodStart = ((CountDownLatchOperationsCounter) listOfSubCounters.get(0)).startTime;
        for (Object obj : listOfSubCounters) {
            CountDownLatchOperationsCounter sub = (CountDownLatchOperationsCounter) obj;
            stats.await.add(sub.await.count.get(), sub.await.totalLatency.get());
            stats.countdown.add(sub.countdown.count.get(), sub.countdown.totalLatency.get());
            stats.other.add(sub.other.count.get(), sub.other.totalLatency.get());
            stats.numberOfAwaitsReleased += sub.awaitsReleased.get();
            stats.numberOfGatesOpened += sub.gatesOpened.get();
            stats.periodEnd = sub.endTime;
        }
        return stats;
    }

    CountDownLatchOperationsCounter getAndReset() {
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

    LocalCountDownLatchOperationStats getThis() {
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

    LocalCountDownLatchOperationStats getEmpty() {
        return empty;
    }
}
