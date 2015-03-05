/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.eclipsesource.json.JsonObject;
import com.hazelcast.management.JsonSerializable;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.JsonUtil.getLong;

/**
 * Internal API
 */
public class InstantLocalMapStats implements JsonSerializable {

    private static final AtomicLongFieldUpdater<InstantLocalMapStats> LAST_ACCESS_TIME_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "lastAccessTime");
    private static final AtomicLongFieldUpdater<InstantLocalMapStats> LAST_UPDATE_TIME_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "lastUpdateTime");
    private static final AtomicLongFieldUpdater<InstantLocalMapStats> NUMBER_OF_OTHER_OPERATIONS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "numberOfOtherOperations");
    private static final AtomicLongFieldUpdater<InstantLocalMapStats> NUMBER_OF_EVENTS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "numberOfEvents");
    private static final AtomicLongFieldUpdater<InstantLocalMapStats> GET_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "getCount");
    private static final AtomicLongFieldUpdater<InstantLocalMapStats> PUT_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "putCount");
    private static final AtomicLongFieldUpdater<InstantLocalMapStats> REMOVE_COUNT_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "removeCount");
    private static final AtomicLongFieldUpdater<InstantLocalMapStats> TOTAL_GET_LATENCIES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "totalGetLatencies");
    private static final AtomicLongFieldUpdater<InstantLocalMapStats> TOTAL_PUT_LATENCIES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "totalPutLatencies");
    private static final AtomicLongFieldUpdater<InstantLocalMapStats> TOTAL_REMOVE_LATENCIES_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "totalRemoveLatencies");
    private static final AtomicLongFieldUpdater<InstantLocalMapStats> MAX_GET_LATENCY_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "maxGetLatency");
    private static final AtomicLongFieldUpdater<InstantLocalMapStats> MAX_PUT_LATENCY_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "maxPutLatency");
    private static final AtomicLongFieldUpdater<InstantLocalMapStats> MAX_REMOVE_LATENCY_UPDATER = AtomicLongFieldUpdater
            .newUpdater(InstantLocalMapStats.class, "maxRemoveLatency");

    // These fields are only accessed through the updaters
    private volatile long lastAccessTime;
    private volatile long lastUpdateTime;
    private volatile long numberOfOtherOperations;
    private volatile long numberOfEvents;
    private volatile long getCount;
    private volatile long putCount;
    private volatile long removeCount;
    private volatile long totalGetLatencies;
    private volatile long totalPutLatencies;
    private volatile long totalRemoveLatencies;
    private volatile long maxGetLatency;
    private volatile long maxPutLatency;
    private volatile long maxRemoveLatency;

    private volatile long creationTime;

    public InstantLocalMapStats() {
        creationTime = Clock.currentTimeMillis();
    }

    public long getCreationTime() {
        return creationTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        LAST_ACCESS_TIME_UPDATER.set(this, Math.max(this.lastAccessTime, lastAccessTime));
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        LAST_UPDATE_TIME_UPDATER.set(this, Math.max(this.lastUpdateTime, lastUpdateTime));
    }

    public long total() {
        return putCount + getCount + removeCount + numberOfOtherOperations;
    }

    public long getPutOperationCount() {
        return putCount;
    }

    public void incrementPuts(long latency) {
        PUT_COUNT_UPDATER.incrementAndGet(this);
        TOTAL_PUT_LATENCIES_UPDATER.addAndGet(this, latency);
        MAX_PUT_LATENCY_UPDATER.set(this, Math.max(maxPutLatency, latency));
    }

    public long getGetOperationCount() {
        return getCount;
    }

    public void incrementGets(long latency) {
        GET_COUNT_UPDATER.incrementAndGet(this);
        TOTAL_GET_LATENCIES_UPDATER.addAndGet(this, latency);
        MAX_GET_LATENCY_UPDATER.set(this, Math.max(maxGetLatency, latency));
    }

    public long getRemoveOperationCount() {
        return removeCount;
    }

    public void incrementRemoves(long latency) {
        REMOVE_COUNT_UPDATER.incrementAndGet(this);
        TOTAL_REMOVE_LATENCIES_UPDATER.addAndGet(this, latency);
        MAX_REMOVE_LATENCY_UPDATER.set(this, Math.max(maxRemoveLatency, latency));
    }

    public long getTotalPutLatency() {
        return totalPutLatencies;
    }

    public long getTotalGetLatency() {
        return totalGetLatencies;
    }

    public long getTotalRemoveLatency() {
        return totalRemoveLatencies;
    }

    public long getMaxPutLatency() {
        return maxPutLatency;
    }

    public long getMaxGetLatency() {
        return maxGetLatency;
    }

    public long getMaxRemoveLatency() {
        return maxRemoveLatency;
    }

    public long getOtherOperationCount() {
        return numberOfOtherOperations;
    }

    public void incrementOtherOperations() {
        NUMBER_OF_OTHER_OPERATIONS_UPDATER.incrementAndGet(this);
    }

    public long getEventOperationCount() {
        return numberOfEvents;
    }

    public void incrementReceivedEvents() {
        NUMBER_OF_EVENTS_UPDATER.incrementAndGet(this);
    }

    @Override
    public JsonObject toJson() {
        final JsonObject root = new JsonObject();

        root.add("creationTime", creationTime);
        root.add("getCount", getCount);
        root.add("putCount", putCount);
        root.add("removeCount", removeCount);
        root.add("numberOfOtherOperations", numberOfOtherOperations);
        root.add("numberOfEvents", numberOfEvents);
        root.add("lastAccessTime", lastAccessTime);
        root.add("lastUpdateTime", lastUpdateTime);
        root.add("totalGetLatencies", totalGetLatencies);
        root.add("totalPutLatencies", totalPutLatencies);
        root.add("totalRemoveLatencies", totalRemoveLatencies);
        root.add("maxGetLatency", maxGetLatency);
        root.add("maxPutLatency", maxPutLatency);
        root.add("maxRemoveLatency", maxRemoveLatency);

        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        this.creationTime = getLong(json, "creationTime", -1L);
        GET_COUNT_UPDATER.set(this, getLong(json, "getCount", -1L));
        PUT_COUNT_UPDATER.set(this, getLong(json, "putCount", -1L));
        REMOVE_COUNT_UPDATER.set(this, getLong(json, "removeCount", -1L));
        NUMBER_OF_OTHER_OPERATIONS_UPDATER.set(this, getLong(json, "numberOfOtherOperations", -1L));
        NUMBER_OF_EVENTS_UPDATER.set(this, getLong(json, "numberOfEvents", -1L));
        LAST_ACCESS_TIME_UPDATER.set(this, getLong(json, "lastAccessTime", -1L));
        LAST_UPDATE_TIME_UPDATER.set(this, getLong(json, "lastUpdateTime", -1L));
        TOTAL_GET_LATENCIES_UPDATER.set(this, getLong(json, "totalGetLatencies", -1L));
        TOTAL_PUT_LATENCIES_UPDATER.set(this, getLong(json, "totalPutLatencies", -1L));
        TOTAL_REMOVE_LATENCIES_UPDATER.set(this, getLong(json, "totalRemoveLatencies", -1L));
        MAX_GET_LATENCY_UPDATER.set(this, getLong(json, "maxGetLatency", -1L));
        MAX_PUT_LATENCY_UPDATER.set(this, getLong(json, "maxPutLatency", -1L));
        MAX_REMOVE_LATENCY_UPDATER.set(this, getLong(json, "maxRemoveLatency", -1L));
    }

    @Override
    public String toString() {
        return "LocalMapStatsImpl{"
                + "lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", numberOfOtherOperations=" + numberOfOtherOperations
                + ", numberOfEvents=" + numberOfEvents
                + ", getCount=" + getCount
                + ", putCount=" + putCount
                + ", removeCount=" + removeCount
                + ", totalGetLatencies=" + totalGetLatencies
                + ", totalPutLatencies=" + totalPutLatencies
                + ", totalRemoveLatencies=" + totalRemoveLatencies
                + ", creationTime=" + creationTime
                + '}';
    }
}
