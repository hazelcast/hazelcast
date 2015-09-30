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
import com.hazelcast.monitor.LocalWanPublisherStats;

import static com.hazelcast.util.JsonUtil.getBoolean;
import static com.hazelcast.util.JsonUtil.getInt;

public class LocalWanPublisherStatsImpl implements LocalWanPublisherStats {
    private volatile boolean connected;
    private volatile int outboundRecsSec;
    private volatile int outboundLatencyMs;
    private volatile int outboundQueueSize;

    @Override
    public boolean isConnected() {
        return connected;
    }

    public void setConnected(boolean connected) {
        this.connected = connected;
    }

    @Override
    public int getOutboundRecsSec() {
        return outboundRecsSec;
    }

    public void setOutboundRecsSec(int outboundRecsSec) {
        this.outboundRecsSec = outboundRecsSec;
    }

    @Override
    public int getOutboundLatencyMs() {
        return outboundLatencyMs;
    }

    public void setOutboundLatencyMs(int outboundLatencyMs) {
        this.outboundLatencyMs = outboundLatencyMs;
    }

    @Override
    public int getOutboundQueueSize() {
        return outboundQueueSize;
    }

    public void setOutboundQueueSize(int outboundQueueSize) {
        this.outboundQueueSize = outboundQueueSize;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("isConnected", connected);
        root.add("outboundRecsSec", outboundRecsSec);
        root.add("outboundLatencyMs", outboundLatencyMs);
        root.add("outboundQueueSize", outboundQueueSize);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        connected = getBoolean(json, "isConnected", false);
        outboundRecsSec = getInt(json, "outboundRecsSec", -1);
        outboundLatencyMs = getInt(json, "outboundLatencyMs", -1);
        outboundQueueSize = getInt(json, "outboundQueueSize", -1);
    }

    @Override
    public String toString() {
        return "LocalPublisherStatsImpl{"
                + "connected=" + connected
                + ", outboundRecsSec=" + outboundRecsSec
                + ", outboundLatencyMs=" + outboundLatencyMs
                + ", outboundQueueSize=" + outboundQueueSize
                + '}';
    }
}
