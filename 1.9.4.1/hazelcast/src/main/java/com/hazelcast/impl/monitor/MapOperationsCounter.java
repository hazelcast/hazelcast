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

package com.hazelcast.impl.monitor;

import java.util.concurrent.atomic.AtomicLong;

import com.hazelcast.monitor.LocalMapOperationStats;

public class MapOperationsCounter extends OperationsCounterSupport<LocalMapOperationStats> {
	
    private final static LocalMapOperationStats empty = new LocalMapOperationStatsImpl();
    
    private final OperationCounter puts = new OperationCounter();
    private final OperationCounter gets = new OperationCounter();
    private final OperationCounter removes = new OperationCounter();
    private final AtomicLong others = new AtomicLong();
    private final AtomicLong events = new AtomicLong();

    public MapOperationsCounter() {
        super();
    }

    public MapOperationsCounter(long interval) {
        super(interval);
    }

    MapOperationsCounter getAndReset() {
        OperationCounter putsNow = puts.copyAndReset();
        OperationCounter getsNow = gets.copyAndReset();
        OperationCounter removesNow = removes.copyAndReset();
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

    public void incrementPuts(long elapsed) {
        puts.count(elapsed);
        publishSubResult();
    }

    public void incrementGets(long elapsed) {
        gets.count(elapsed);
        publishSubResult();
    }

    public void incrementRemoves(long elapsed) {
        removes.count(elapsed);
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

    LocalMapOperationStats aggregateSubCounterStats() {
        LocalMapOperationStatsImpl stats = new LocalMapOperationStatsImpl();
        stats.periodStart = ((MapOperationsCounter) listOfSubCounters.get(0)).startTime;
        for (int i = 0; i < listOfSubCounters.size(); i++) {
            MapOperationsCounter sub = (MapOperationsCounter) listOfSubCounters.get(i);
            stats.gets.add(sub.gets.count.get(), sub.gets.totalLatency.get());
            stats.puts.add(sub.puts.count.get(), sub.puts.totalLatency.get());
            stats.removes.add(sub.removes.count.get(), sub.removes.totalLatency.get());
            stats.numberOfOtherOperations += sub.others.get();
            stats.numberOfEvents += sub.events.get();
            stats.periodEnd = sub.endTime;
        }
        return stats;
    }

    LocalMapOperationStats getThis() {
        LocalMapOperationStatsImpl stats = new LocalMapOperationStatsImpl();
        stats.periodStart = this.startTime;
        stats.gets = stats.new OperationStat(this.gets.count.get(), this.gets.totalLatency.get());
        stats.puts = stats.new OperationStat(this.puts.count.get(), this.puts.totalLatency.get());
        stats.removes = stats.new OperationStat(this.removes.count.get(), this.removes.totalLatency.get());
        stats.numberOfEvents = this.events.get();
        stats.periodEnd = now();
        return stats;
    }
    
    LocalMapOperationStats getEmpty() {
    	return empty;
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
                ", listOfSubStats=" + listOfSubCounters +
                ", lock=" + lock +
                ", interval=" + interval +
                '}';
    }
}
