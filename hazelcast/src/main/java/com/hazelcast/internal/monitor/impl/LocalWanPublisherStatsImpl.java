/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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


package com.hazelcast.internal.monitor.impl;

import com.hazelcast.wan.WanPublisherState;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.monitor.LocalWanPublisherStats;
import com.hazelcast.wan.DistributedServiceWanEventCounters.DistributedObjectWanEventCounters;
import com.hazelcast.wan.WanSyncStats;
import com.hazelcast.wan.ConsistencyCheckResult;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.internal.util.JsonUtil.getBoolean;
import static com.hazelcast.internal.util.JsonUtil.getInt;
import static com.hazelcast.internal.util.JsonUtil.getLong;
import static com.hazelcast.internal.util.JsonUtil.getString;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class LocalWanPublisherStatsImpl implements LocalWanPublisherStats {

    private static final AtomicLongFieldUpdater<LocalWanPublisherStatsImpl> TOTAL_PUBLISH_LATENCY =
            newUpdater(LocalWanPublisherStatsImpl.class, "totalPublishLatency");
    private static final AtomicLongFieldUpdater<LocalWanPublisherStatsImpl> TOTAL_PUBLISHED_EVENT_COUNT =
            newUpdater(LocalWanPublisherStatsImpl.class, "totalPublishedEventCount");

    private volatile boolean connected;
    private volatile WanPublisherState state;
    private volatile int outboundQueueSize;
    private volatile long totalPublishLatency;
    private volatile long totalPublishedEventCount;
    private volatile Map<String, DistributedObjectWanEventCounters> sentMapEventCounter;
    private volatile Map<String, DistributedObjectWanEventCounters> sentCacheEventCounter;
    private volatile Map<String, ConsistencyCheckResult> lastConsistencyCheckResults;
    private volatile Map<String, WanSyncStats> lastSyncStats;

    @Override
    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    @Override
    public int getOutboundQueueSize() {
        return outboundQueueSize;
    }

    public void setOutboundQueueSize(int outboundQueueSize) {
        this.outboundQueueSize = outboundQueueSize;
    }

    @Override
    public WanPublisherState getPublisherState() {
        return state;
    }

    public void setState(WanPublisherState state) {
        this.state = state;
    }

    @Override
    public long getTotalPublishLatency() {
        return totalPublishLatency;
    }

    @Override
    public long getTotalPublishedEventCount() {
        return totalPublishedEventCount;
    }

    @Override
    public Map<String, DistributedObjectWanEventCounters> getSentMapEventCounter() {
        return sentMapEventCounter;
    }

    public void setSentMapEventCounter(Map<String, DistributedObjectWanEventCounters> sentMapEventCounter) {
        this.sentMapEventCounter = sentMapEventCounter;
    }

    @Override
    public Map<String, DistributedObjectWanEventCounters> getSentCacheEventCounter() {
        return sentCacheEventCounter;
    }

    public void setSentCacheEventCounter(Map<String, DistributedObjectWanEventCounters> sentCacheEventCounter) {
        this.sentCacheEventCounter = sentCacheEventCounter;
    }

    public void setLastConsistencyCheckResults(Map<String, ConsistencyCheckResult> lastConsistencyCheckResults) {
        this.lastConsistencyCheckResults = lastConsistencyCheckResults;
    }

    @Override
    public Map<String, ConsistencyCheckResult> getLastConsistencyCheckResults() {
        return lastConsistencyCheckResults;
    }

    public void setLastSyncStats(Map<String, WanSyncStats> lastSyncStats) {
        this.lastSyncStats = lastSyncStats;
    }

    @Override
    public Map<String, WanSyncStats> getLastSyncStats() {
        return lastSyncStats;
    }

    public void incrementPublishedEventCount(long latency) {
        TOTAL_PUBLISHED_EVENT_COUNT.incrementAndGet(this);
        TOTAL_PUBLISH_LATENCY.addAndGet(this, latency);
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("isConnected", connected);
        root.add("totalPublishLatencies", totalPublishLatency);
        root.add("totalPublishedEventCount", totalPublishedEventCount);
        root.add("outboundQueueSize", outboundQueueSize);
        root.add("state", state.name());
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        connected = getBoolean(json, "isConnected", false);
        totalPublishLatency = getLong(json, "totalPublishLatencies", -1);
        totalPublishedEventCount = getLong(json, "totalPublishedEventCount", -1);
        outboundQueueSize = getInt(json, "outboundQueueSize", -1);
        state = WanPublisherState.valueOf(getString(json, "state", WanPublisherState.REPLICATING.name()));
    }

    @Override
    public String toString() {
        return "LocalPublisherStatsImpl{"
                + "connected=" + connected
                + ", totalPublishLatency=" + totalPublishLatency
                + ", totalPublishedEventCount=" + totalPublishedEventCount
                + ", outboundQueueSize=" + outboundQueueSize
                + ", state=" + state
                + '}';
    }
}
