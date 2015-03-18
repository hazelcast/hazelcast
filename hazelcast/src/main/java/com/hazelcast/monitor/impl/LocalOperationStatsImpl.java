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

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.JsonSerializable;
import com.hazelcast.monitor.LocalOperationStats;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.util.JsonUtil.getLong;

/**
 * Hazelcast statistic implementations for local operations.
 */
public class LocalOperationStatsImpl implements LocalOperationStats {

    private long creationTime;
    private Collection<JsonSerializable> slowOperations;
    private long maxVisibleSlowOperationCount;

    public LocalOperationStatsImpl() {
        this.slowOperations = new ConcurrentLinkedQueue<JsonSerializable>();
        this.maxVisibleSlowOperationCount = Long.MAX_VALUE;
    }

    public LocalOperationStatsImpl(Node node) {
        InternalOperationService operationService = (InternalOperationService) node.nodeEngine.getOperationService();
        this.slowOperations = operationService.getSlowOperations();
        this.maxVisibleSlowOperationCount = node.groupProperties.MC_MAX_SLOW_OPERATION_COUNT.getInteger();
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public JsonObject toJson() {
        JsonObject root = new JsonObject();
        root.add("creationTime", creationTime);
        root.add("maxVisibleSlowOperationCount", maxVisibleSlowOperationCount);
        JsonArray slowOperationArray = new JsonArray();
        int logCount = 0;
        for (JsonSerializable slowOperation : slowOperations) {
            if (logCount++ < maxVisibleSlowOperationCount) {
                slowOperationArray.add(slowOperation.toJson());
            }
        }
        root.add("slowOperations", slowOperationArray);
        return root;
    }

    @Override
    public void fromJson(JsonObject json) {
        creationTime = getLong(json, "creationTime", -1L);
        maxVisibleSlowOperationCount = getLong(json, "maxVisibleSlowOperationCount", Long.MAX_VALUE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LocalOperationStatsImpl)) {
            return false;
        }
        LocalOperationStatsImpl that = (LocalOperationStatsImpl) o;
        if (creationTime != that.creationTime) {
            return false;
        }
        if (maxVisibleSlowOperationCount != that.maxVisibleSlowOperationCount) {
            return false;
        }
        if (slowOperations != null ? !isEqualSlowOperations(that.slowOperations) : that.slowOperations != null) {
            return false;
        }
        return true;
    }

    private boolean isEqualSlowOperations(Collection<JsonSerializable> thatSlowOperations) {
        if (slowOperations.size() != thatSlowOperations.size()) {
            return false;
        }
        if (!slowOperations.containsAll(thatSlowOperations)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = slowOperations != null ? slowOperations.hashCode() : 0;
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (maxVisibleSlowOperationCount ^ (maxVisibleSlowOperationCount >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LocalOperationStatsImpl{"
                + "creationTime=" + creationTime
                + ", slowOperations=" + slowOperations
                + ", maxVisibleSlowOperationCount=" + maxVisibleSlowOperationCount
                + '}';
    }
}
