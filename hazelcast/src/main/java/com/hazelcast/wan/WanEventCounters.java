/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan;

import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_DROPPED_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_REMOVE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_SYNC_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.WAN_METRIC_UPDATE_COUNT;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;

/**
 * Counters for WAN events for a single distributed object type (map or
 * cache).
 * This class may contain counters for a single WAN publisher or multiple
 * WAN publishers, depending on its usage.
 */
public class WanEventCounters {
    private static final ConstructorFunction<String, DistributedObjectWanEventCounters> EVENT_COUNTER_CONSTRUCTOR_FN
            = ignored -> new DistributedObjectWanEventCounters();
    private final ConcurrentHashMap<String, DistributedObjectWanEventCounters> eventCounterMap
            = new ConcurrentHashMap<>();

    /**
     * Increment the number of sync events for the {@code distributedObjectName}.
     */
    public void incrementSync(String distributedObjectName) {
        incrementSync(distributedObjectName, 1);
    }

    /**
     * Increment the number of sync events for the {@code distributedObjectName}
     * by {@code count}.
     */
    public void incrementSync(String distributedObjectName, int count) {
        getOrPutIfAbsent(eventCounterMap, distributedObjectName, EVENT_COUNTER_CONSTRUCTOR_FN)
                .incrementSyncCount(count);
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
     * Returns a map from distributed object name to {@link DistributedObjectWanEventCounters}.
     */
    public ConcurrentHashMap<String, DistributedObjectWanEventCounters> getEventCounterMap() {
        return eventCounterMap;
    }

    /**
     * Counters for WAN events for a single map or cache.
     */
    public static final class DistributedObjectWanEventCounters {
        @Probe(name = WAN_METRIC_SYNC_COUNT)
        private final AtomicLong syncCount = new AtomicLong();
        @Probe(name = WAN_METRIC_UPDATE_COUNT)
        private final AtomicLong updateCount = new AtomicLong();
        @Probe(name = WAN_METRIC_REMOVE_COUNT)
        private final AtomicLong removeCount = new AtomicLong();
        @Probe(name = WAN_METRIC_DROPPED_COUNT)
        private final AtomicLong droppedCount = new AtomicLong();

        private DistributedObjectWanEventCounters() {
        }

        /** Increment the counter for entry sync events */
        private void incrementSyncCount(int count) {
            syncCount.addAndGet(count);
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
