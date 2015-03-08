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
import com.hazelcast.monitor.LocalOperationStats;
import com.hazelcast.spi.impl.InternalOperationService;
import com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationLog;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.hazelcast.util.JsonUtil.getLong;

/**
 * Hazelcast statistic implementations for local operations.
 */
public class LocalOperationStatsImpl implements LocalOperationStats {

    private long creationTime;
    private Collection<SlowOperationLog> logs;
    private long maxVisibleSlowOperationCount;

    public LocalOperationStatsImpl() {
        this.logs = new ConcurrentLinkedQueue<SlowOperationLog>();
        this.maxVisibleSlowOperationCount = Long.MAX_VALUE;
    }

    public LocalOperationStatsImpl(Node node) {
        this.logs = ((InternalOperationService) node.nodeEngine.getOperationService()).getSlowOperationLogs();
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
        JsonArray slowOperationLogs = new JsonArray();
        int logCount = 0;
        for (SlowOperationLog log : logs) {
            if (logCount++ < maxVisibleSlowOperationCount) {
                slowOperationLogs.add(log.toJson());
            }
        }
        root.add("slowOperationLogs", slowOperationLogs);
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
        if (logs != null ? !isEqualLogs(that.logs) : that.logs != null) {
            return false;
        }
        return true;
    }

    private boolean isEqualLogs(Collection<SlowOperationLog> thatLogs) {
        if (logs.size() != thatLogs.size()) {
            return false;
        }
        if (!logs.containsAll(thatLogs)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = logs != null ? logs.hashCode() : 0;
        result = 31 * result + (int) (creationTime ^ (creationTime >>> 32));
        result = 31 * result + (int) (maxVisibleSlowOperationCount ^ (maxVisibleSlowOperationCount >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "LocalOperationStatsImpl{"
                + "creationTime=" + creationTime
                + ", logs=" + logs
                + ", maxVisibleSlowOperationCount=" + maxVisibleSlowOperationCount
                + '}';
    }
}
