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


package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.monitor.LocalWanPublisherStats;
import com.hazelcast.internal.monitor.LocalWanStats;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.json.internal.JsonSerializable;

import java.util.HashMap;
import java.util.Map;

/**
 * Local WAN replication statistics for a single WAN replication scheme.
 * A single WAN replication scheme can contain multiple WAN replication
 * publishers, identified by name.
 */
public class LocalWanStatsImpl implements LocalWanStats, JsonSerializable {
    /**
     * Local WAN replication statistics for a single scheme, grouped by WAN
     * publisher ID.
     */
    private volatile Map<String, LocalWanPublisherStats> localPublisherStatsMap = new HashMap<>();
    private final long creationTime;

    public LocalWanStatsImpl() {
        creationTime = Clock.currentTimeMillis();
    }

    @Override
    public Map<String, LocalWanPublisherStats> getLocalWanPublisherStats() {
        return localPublisherStatsMap;
    }

    public void setLocalPublisherStatsMap(Map<String, LocalWanPublisherStats> localPublisherStatsMap) {
        this.localPublisherStatsMap = localPublisherStatsMap;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public JsonObject toJson() {
        JsonObject wanStatsObject = new JsonObject();
        for (Map.Entry<String, LocalWanPublisherStats> entry : localPublisherStatsMap.entrySet()) {
            wanStatsObject.add(entry.getKey(), entry.getValue().toJson());
        }
        return wanStatsObject;
    }

    @Override
    public void fromJson(JsonObject json) {
        for (JsonObject.Member next : json) {
            LocalWanPublisherStats localPublisherStats = new LocalWanPublisherStatsImpl();
            localPublisherStats.fromJson(next.getValue().asObject());
            localPublisherStatsMap.put(next.getName(), localPublisherStats);
        }
    }

    @Override
    public String toString() {
        return "LocalWanStatsImpl{"
                + "localPublisherStatsMap=" + localPublisherStatsMap
                + ", creationTime=" + creationTime
                + '}';
    }
}
