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

import com.hazelcast.monitor.LocalQueueOperationStats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class QueueOperationsCounter {
    private AtomicLong offers = new AtomicLong();
    private AtomicLong rejectedOffers = new AtomicLong();
    private AtomicLong polls = new AtomicLong();
    private AtomicLong emptyPolls = new AtomicLong();
    private AtomicLong others = new AtomicLong();
    private AtomicLong events = new AtomicLong();
    private long startTime = now();
    private long endTime = Long.MAX_VALUE;
    private transient LocalQueueOperationStats published = null;
    private List<QueueOperationsCounter> listOfSubStats = new ArrayList<QueueOperationsCounter>();
    final private Object lock = new Object();
    final private LocalQueueOperationStats empty = new LocalQueueOperationStatsImpl();

    final private long interval;

    public QueueOperationsCounter() {
        this(5000);
    }

    public QueueOperationsCounter(long interval) {
        this.interval = interval;
    }

    private QueueOperationsCounter getAndReset() {
        QueueOperationsCounter newOne = new QueueOperationsCounter();
        newOne.offers.set(offers.getAndSet(0));
        newOne.polls.set(polls.getAndSet(0));
        newOne.rejectedOffers.set(rejectedOffers.getAndSet(0));
        newOne.emptyPolls.set(emptyPolls.getAndSet(0));
        newOne.others.set(others.getAndSet(0));
        newOne.events.set(events.getAndSet(0));
        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    public LocalQueueOperationStats getPublishedStats() {
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

    public void incrementOffers() {
        offers.incrementAndGet();
        publishSubResult();
    }

    public void incrementRejectedOffers() {
        rejectedOffers.incrementAndGet();
        publishSubResult();
    }

    public void incrementPolls() {
        polls.incrementAndGet();
        publishSubResult();
    }

    public void incrementEmptyPolls() {
        emptyPolls.incrementAndGet();
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
                    QueueOperationsCounter copy = getAndReset();
                    if (listOfSubStats.size() == 5) {
                        listOfSubStats.remove(0);
                    }
                    listOfSubStats.add(copy);
                    this.published = aggregate(listOfSubStats);
                }
            }
        }
    }

    private LocalQueueOperationStats aggregate(List<QueueOperationsCounter> list) {
        LocalQueueOperationStatsImpl stats = new LocalQueueOperationStatsImpl();
        stats.periodStart = list.get(0).startTime;
        for (int i = 0; i < list.size(); i++) {
            QueueOperationsCounter sub = list.get(i);
            stats.numberOfPolls += sub.polls.get();
            stats.numberOfOffers += sub.offers.get();
            stats.numberOfRejectedOffers += sub.rejectedOffers.get();
            stats.numberOfEmptyPolls += sub.emptyPolls.get();
            stats.numberOfOtherOperations += sub.others.get();
            stats.numberOfEvents += sub.events.get();
            stats.periodEnd = sub.endTime;
        }
        return stats;
    }

    private LocalQueueOperationStats getThis() {
        LocalQueueOperationStatsImpl stats = new LocalQueueOperationStatsImpl();
        stats.periodStart = this.startTime;
        stats.numberOfPolls = this.polls.get();
        stats.numberOfOffers = this.offers.get();
        stats.numberOfEmptyPolls = this.emptyPolls.get();
        stats.numberOfRejectedOffers = this.rejectedOffers.get();
        stats.numberOfEvents = this.events.get();
        stats.periodEnd = now();
        return stats;
    }
}