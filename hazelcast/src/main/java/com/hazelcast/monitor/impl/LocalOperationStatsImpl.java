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

package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.monitor.LocalOperationStats;
import com.hazelcast.util.Clock;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.spi.properties.GroupProperty.MC_MAX_VISIBLE_SLOW_OPERATION_COUNT;
import static com.hazelcast.util.JsonUtil.getArray;
import static com.hazelcast.util.JsonUtil.getLong;

/**
 * Hazelcast statistic implementations for local operations.
 */
public class LocalOperationStatsImpl implements LocalOperationStats {

    private long maxVisibleSlowOperationCount;
    private List<SlowOperationDTO> slowOperations;
    private long creationTime;

    public LocalOperationStatsImpl() {
        this.maxVisibleSlowOperationCount = Long.MAX_VALUE;
        this.slowOperations = new ArrayList<SlowOperationDTO>();
        this.creationTime = Clock.currentTimeMillis();
    }

    public LocalOperationStatsImpl(Node node) {
        this.maxVisibleSlowOperationCount = node.getProperties().getInteger(MC_MAX_VISIBLE_SLOW_OPERATION_COUNT);
        this.slowOperations = node.nodeEngine.getOperationService().getSlowOperationDTOs();
        this.creationTime = Clock.currentTimeMillis();
    }

    public long getMaxVisibleSlowOperationCount() {
        return maxVisibleSlowOperationCount;
    }

    public List<SlowOperationDTO> getSlowOperations() {
        return slowOperations;
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("maxVisibleSlowOperationCount", maxVisibleSlowOperationCount);
        JsonArray slowOperationArray = new JsonArray();
        int logCount = 0;
        for (SlowOperationDTO slowOperation : slowOperations) {
            if (logCount++ < maxVisibleSlowOperationCount) {
                slowOperationArray.add(slowOperation.toJson());
            }
        }
        root.add("slowOperations", slowOperationArray);
        root.add("creationTime", creationTime);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        maxVisibleSlowOperationCount = getLong(json, "maxVisibleSlowOperationCount", Long.MAX_VALUE);
        for (JsonValue jsonValue : getArray(json, "slowOperations")) {
            SlowOperationDTO slowOperationDTO = new SlowOperationDTO();
            slowOperationDTO.fromJson(jsonValue.asObject());
            slowOperations.add(slowOperationDTO);
        }
        creationTime = getLong(json, "creationTime", -1L);
    }

    @Override
    public String toString() {
        return "LocalOperationStatsImpl{"
                + "maxVisibleSlowOperationCount=" + maxVisibleSlowOperationCount
                + ", slowOperations=" + slowOperations
                + ", creationTime=" + creationTime
                + '}';
    }
}
