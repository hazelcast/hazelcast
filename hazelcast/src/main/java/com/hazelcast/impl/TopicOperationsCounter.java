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

import com.hazelcast.monitor.LocalTopicOperationStats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class TopicOperationsCounter {
    private AtomicLong messagePublishes = new AtomicLong();
    private AtomicLong receivedMessages = new AtomicLong();
    private long startTime = now();
    private long endTime = Long.MAX_VALUE;
    private transient LocalTopicOperationStats published = null;
    private List<TopicOperationsCounter> listOfSubStats = new ArrayList<TopicOperationsCounter>();
    final private Object lock = new Object();
    final private LocalTopicOperationStats empty = new LocalTopicOperationStatsImpl();

    final private long interval;

    public TopicOperationsCounter() {
        this(5000);
    }

    public TopicOperationsCounter(long interval) {
        this.interval = interval;
    }

    private TopicOperationsCounter getAndReset() {
        TopicOperationsCounter newOne = new TopicOperationsCounter();
        newOne.messagePublishes.set(messagePublishes.getAndSet(0));
        newOne.receivedMessages.set(receivedMessages.getAndSet(0));
        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    public LocalTopicOperationStats getPublishedStats() {
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

    public void incrementPublishes() {
        messagePublishes.incrementAndGet();
        publishSubResult();
    }

    public void incrementReceivedMessages() {
        receivedMessages.incrementAndGet();
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
                    TopicOperationsCounter copy = getAndReset();
                    if (listOfSubStats.size() == 5) {
                        listOfSubStats.remove(0);
                    }
                    listOfSubStats.add(copy);
                    this.published = aggregate(listOfSubStats);
                }
            }
        }
    }

    private LocalTopicOperationStats aggregate(List<TopicOperationsCounter> list) {
        LocalTopicOperationStatsImpl stats = new LocalTopicOperationStatsImpl();
        stats.periodStart = list.get(0).startTime;
        for (int i = 0; i < list.size(); i++) {
            TopicOperationsCounter sub = list.get(i);
            stats.numberOfPublishes += sub.messagePublishes.get();
            stats.numberOfReceives += sub.receivedMessages.get();
            stats.periodEnd = sub.endTime;
        }
        return stats;
    }

    private LocalTopicOperationStats getThis() {
        LocalTopicOperationStatsImpl stats = new LocalTopicOperationStatsImpl();
        stats.periodStart = this.startTime;
        stats.numberOfPublishes = this.messagePublishes.get();
        stats.numberOfReceives = this.receivedMessages.get();
        stats.periodEnd = now();
        return stats;
    }
}
