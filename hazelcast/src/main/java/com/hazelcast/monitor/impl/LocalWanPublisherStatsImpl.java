/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.monitor.LocalWanPublisherStats;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static com.hazelcast.util.JsonUtil.getBoolean;
import static com.hazelcast.util.JsonUtil.getInt;
import static com.hazelcast.util.JsonUtil.getLong;
import static java.util.concurrent.atomic.AtomicLongFieldUpdater.newUpdater;

public class LocalWanPublisherStatsImpl implements LocalWanPublisherStats {

    private static final AtomicLongFieldUpdater<LocalWanPublisherStatsImpl> TOTAL_PUBLISH_LATENCY =
            newUpdater(LocalWanPublisherStatsImpl.class, "totalPublishLatency");
    private static final AtomicLongFieldUpdater<LocalWanPublisherStatsImpl> TOTAL_PUBLISHED_EVENT_COUNT =
            newUpdater(LocalWanPublisherStatsImpl.class, "totalPublishedEventCount");

    private volatile boolean connected;
    private volatile boolean paused;
    private volatile int outboundQueueSize;
    private volatile long totalPublishLatency;
    private volatile long totalPublishedEventCount;

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
    public boolean isPaused() {
        return paused;
    }

    public void setPaused(boolean paused) {
        this.paused = paused;
    }

    @Override
    public long getTotalPublishLatency() {
        return totalPublishLatency;
    }

    @Override
    public long getTotalPublishedEventCount() {
        return totalPublishedEventCount;
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
        root.add("paused", paused);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        connected = getBoolean(json, "isConnected", false);
        totalPublishLatency = getLong(json, "totalPublishLatencies", -1);
        totalPublishedEventCount = getLong(json, "totalPublishedEventCount", -1);
        outboundQueueSize = getInt(json, "outboundQueueSize", -1);
        paused = getBoolean(json, "paused");
    }

    @Override
    public String toString() {
        return "LocalPublisherStatsImpl{"
                + "connected=" + connected
                + ", totalPublishLatency=" + totalPublishLatency
                + ", totalPublishedEventCount=" + totalPublishedEventCount
                + ", outboundQueueSize=" + outboundQueueSize
                + ", paused=" + paused
                + '}';
    }
}
