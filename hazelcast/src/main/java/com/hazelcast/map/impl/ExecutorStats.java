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

package com.hazelcast.map.impl;

import com.hazelcast.internal.monitor.impl.LocalExecutorStatsImpl;
import com.hazelcast.internal.util.ConcurrencyUtil;
import com.hazelcast.internal.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;


/**
 * Shared functionality to collect stats of various
 * executors' tasks. One instance of this class is created
 * per executor service. Scheduled, durable and executor
 * services' implementations use this class to collect stat.
 */
public final class ExecutorStats {

    private static final LocalExecutorStatsImpl EMPTY_LOCAL_EXECUTOR_STATS = new LocalExecutorStatsImpl();

    private final ConcurrentHashMap<String, LocalExecutorStatsImpl> statsMap = new ConcurrentHashMap<>();
    private final ConstructorFunction<String, LocalExecutorStatsImpl> executorStatsConstructor
            = key -> new LocalExecutorStatsImpl();

    public ExecutorStats() {
    }

    public ConcurrentHashMap<String, LocalExecutorStatsImpl> getStatsMap() {
        return statsMap;
    }

    public void startExecution(String executorName, long elapsed) {
        getLocalExecutorStats(executorName, true).startExecution(elapsed);
    }

    public void finishExecution(String executorName, long elapsed) {
        getLocalExecutorStats(executorName, true).finishExecution(elapsed);
    }

    public void startPending(String executorName) {
        getLocalExecutorStats(executorName, true).startPending();
    }

    public void rejectExecution(String executorName) {
        getLocalExecutorStats(executorName, true).rejectExecution();
    }

    public void cancelExecution(String executorName) {
        getLocalExecutorStats(executorName, true).cancelExecution();
    }

    public LocalExecutorStatsImpl getLocalExecutorStats(String executorName, boolean createIfAbsent) {
        LocalExecutorStatsImpl localExecutorStats = statsMap.get(executorName);
        if (localExecutorStats != null) {
            return localExecutorStats;
        }

        if (createIfAbsent) {
            return ConcurrencyUtil.getOrPutIfAbsent(statsMap, executorName, executorStatsConstructor);
        }

        return EMPTY_LOCAL_EXECUTOR_STATS;
    }

    public void clear() {
        statsMap.clear();
    }

    public void removeStats(String executorName) {
        statsMap.remove(executorName);
    }
}
