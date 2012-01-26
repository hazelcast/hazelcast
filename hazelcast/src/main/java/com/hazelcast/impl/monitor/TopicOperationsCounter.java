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

import com.hazelcast.monitor.LocalTopicOperationStats;

import java.util.concurrent.atomic.AtomicLong;

public class TopicOperationsCounter extends OperationsCounterSupport<LocalTopicOperationStats> {
    private static final LocalTopicOperationStats empty = new LocalTopicOperationStatsImpl();

    private AtomicLong messagePublishes = new AtomicLong();
    private AtomicLong receivedMessages = new AtomicLong();

    public TopicOperationsCounter() {
        super();
    }

    public TopicOperationsCounter(long interval) {
        super(interval);
    }

    TopicOperationsCounter getAndReset() {
        TopicOperationsCounter newOne = new TopicOperationsCounter();
        newOne.messagePublishes.set(messagePublishes.getAndSet(0));
        newOne.receivedMessages.set(receivedMessages.getAndSet(0));
        newOne.startTime = this.startTime;
        newOne.endTime = now();
        this.startTime = newOne.endTime;
        return newOne;
    }

    public void incrementPublishes() {
        messagePublishes.incrementAndGet();
        publishSubResult();
    }

    public void incrementReceivedMessages() {
        receivedMessages.incrementAndGet();
        publishSubResult();
    }

    LocalTopicOperationStats aggregateSubCounterStats() {
        LocalTopicOperationStatsImpl stats = new LocalTopicOperationStatsImpl();
        stats.periodStart = ((TopicOperationsCounter) listOfSubCounters.get(0)).startTime;
        for (int i = 0; i < listOfSubCounters.size(); i++) {
            TopicOperationsCounter sub = (TopicOperationsCounter) listOfSubCounters.get(i);
            stats.numberOfPublishes += sub.messagePublishes.get();
            stats.numberOfReceives += sub.receivedMessages.get();
            stats.periodEnd = sub.endTime;
        }
        return stats;
    }

    LocalTopicOperationStats getThis() {
        LocalTopicOperationStatsImpl stats = new LocalTopicOperationStatsImpl();
        stats.periodStart = this.startTime;
        stats.numberOfPublishes = this.messagePublishes.get();
        stats.numberOfReceives = this.receivedMessages.get();
        stats.periodEnd = now();
        return stats;
    }

    LocalTopicOperationStats getEmpty() {
        return empty;
    }
}
