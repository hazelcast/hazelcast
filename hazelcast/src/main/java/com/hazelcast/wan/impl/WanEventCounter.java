/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan.impl;

import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Counter for WAN events for a single distributed object type (map or
 * cache).
 */
public class WanEventCounter {
    private static final ConstructorFunction<String, EventCounter> EVENT_COUNTER_CONSTRUCTOR_FN
            = new ConstructorFunction<String, EventCounter>() {
        @Override
        public EventCounter createNew(String ignored) {
            return new EventCounter();
        }
    };
    private final ConcurrentHashMap<String, EventCounter> eventCounterMap = new ConcurrentHashMap<String, EventCounter>();

    /**
     * Increment the number of sync events for the {@code distributedObjectName}.
     */
    public void incrementSync(String distributedObjectName) {
        getOrPutIfAbsent(eventCounterMap, distributedObjectName, EVENT_COUNTER_CONSTRUCTOR_FN).incrementSyncCount();
    }

    /**
     * Increment the number of update events for the {@code distributedObjectName}.
     */
    public void incrementUpdate(String distributedObjectName) {
        getOrPutIfAbsent(eventCounterMap, distributedObjectName, EVENT_COUNTER_CONSTRUCTOR_FN).incrementUpdateCount();
    }

    /**
     * Increment the number of remove events for the {@code distributedObjectName}.
     */
    public void incrementRemove(String distributedObjectName) {
        getOrPutIfAbsent(eventCounterMap, distributedObjectName, EVENT_COUNTER_CONSTRUCTOR_FN).incrementRemoveCount();
    }

    /**
     * Increment the number of dropped events for the {@code distributedObjectName}.
     */
    public void incrementDropped(String distributedObjectName) {
        getOrPutIfAbsent(eventCounterMap, distributedObjectName, EVENT_COUNTER_CONSTRUCTOR_FN).incrementDroppedCount();
    }

    /**
     * Removes the counter for the given {@code dataStructureName}.
     */
    public void removeCounter(String dataStructureName) {
        eventCounterMap.remove(dataStructureName);
    }

    /**
     * Returns a map from distributed object name to {@link EventCounter}.
     */
    public ConcurrentHashMap<String, EventCounter> getEventCounterMap() {
        return eventCounterMap;
    }

    /**
     * Counter for WAN events for a single map or cache.
     */
    public static final class EventCounter {
        private final AtomicLong syncCount = new AtomicLong();
        private final AtomicLong updateCount = new AtomicLong();
        private final AtomicLong removeCount = new AtomicLong();
        private final AtomicLong droppedCount = new AtomicLong();

        private EventCounter() {
        }

        /** Increment the counter for entry sync events */
        private void incrementSyncCount() {
            syncCount.incrementAndGet();
        }

        /** Increment the counter for entry update events */
        private void incrementUpdateCount() {
            updateCount.incrementAndGet();
        }

        /** Increment the counter for entry remove events */
        private void incrementRemoveCount() {
            removeCount.incrementAndGet();
        }

        /** Increment the counter for dropped entry events */
        private void incrementDroppedCount() {
            droppedCount.incrementAndGet();
        }

        /** Returns the number of dropped entry events */
        public long getDroppedCount() {
            return droppedCount.longValue();
        }

        /** Returns the number of entry sync events */
        public long getSyncCount() {
            return syncCount.longValue();
        }

        /** Returns the number of entry update events */
        public long getUpdateCount() {
            return updateCount.longValue();
        }

        /** Returns the number of entry remove events */
        public long getRemoveCount() {
            return removeCount.longValue();
        }
    }
}
