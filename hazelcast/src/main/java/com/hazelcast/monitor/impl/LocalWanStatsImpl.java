/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.util.Clock;

import java.util.HashMap;
import java.util.Map;

public class LocalWanStatsImpl implements LocalWanStats {

    private volatile Map<String, LocalWanPublisherStats> localPublisherStatsMap = new HashMap<String, LocalWanPublisherStats>();
    private volatile long creationTime;

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
}
