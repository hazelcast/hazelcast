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

package com.hazelcast.topic;

import com.hazelcast.monitor.impl.TopicOperationsCounter;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLong;

/**
 * User: sancar
 * Date: 2/26/13
 * Time: 2:37 PM
 */
public class TopicContainer {

    private final long creationTime;
    private final TopicOperationsCounter operationsCounter = new TopicOperationsCounter();
    private final AtomicLong totalReceivedMessages = new AtomicLong();
    private final AtomicLong totalPublishes = new AtomicLong();
    private final AtomicLong lastAccessTime = new AtomicLong();

    public TopicContainer() {
        creationTime = Clock.currentTimeMillis();
    }

    public long getLastAccessTime() {
        return lastAccessTime.get();
    }

    public long getTotalPublishes() {
        return totalPublishes.get();
    }

    public long getTotalReceivedMessages() {
        return totalReceivedMessages.get();
    }

    public TopicOperationsCounter getOperationsCounter() {
        return operationsCounter;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void incrementPublishes() {
        operationsCounter.incrementPublishes();
        totalPublishes.incrementAndGet();
        lastAccessTime.set(Clock.currentTimeMillis());
    }

    public void incrementReceivedMessages() {
        operationsCounter.incrementReceivedMessages();
        totalReceivedMessages.incrementAndGet();
    }


}
