/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.config;

import com.hazelcast.mapreduce.TopologyChangedStrategy;

public class JobTrackerConfig {

    public static final int DEFAULT_MAX_THREAD_SIZE = Runtime.getRuntime().availableProcessors();
    public static final int DEFAULT_RETRY_COUNT = 0;
    public static final int DEFAULT_CHUNK_SIZE = 1000;
    public static final int DEFAULT_QUEUE_SIZE = 0;
    public static final boolean DEFAULT_COMMUNICATE_STATS = true;
    public static final TopologyChangedStrategy DEFAULT_TOPOLOGY_CHANGED_STRATEGY
            = TopologyChangedStrategy.CANCEL_RUNNING_OPERATION;

    private String name;
    private int maxThreadSize = DEFAULT_MAX_THREAD_SIZE;
    private int retryCount = DEFAULT_RETRY_COUNT;
    private int chunkSize = DEFAULT_CHUNK_SIZE;
    private int queueSize = DEFAULT_QUEUE_SIZE;
    private boolean communicateStats = DEFAULT_COMMUNICATE_STATS;
    private TopologyChangedStrategy topologyChangedStrategy = DEFAULT_TOPOLOGY_CHANGED_STRATEGY;

    public JobTrackerConfig() {
    }

    public JobTrackerConfig(JobTrackerConfig source) {
        this.name = source.name;
        this.maxThreadSize = source.maxThreadSize;
        this.retryCount = source.retryCount;
        this.chunkSize = source.chunkSize;
        this.queueSize = source.queueSize;
        this.communicateStats = source.communicateStats;
        this.topologyChangedStrategy = source.topologyChangedStrategy;
    }

    public JobTrackerConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public int getMaxThreadSize() {
        return maxThreadSize;
    }

    public void setMaxThreadSize(int maxThreadSize) {
        this.maxThreadSize = maxThreadSize;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    public JobTrackerConfig getAsReadOnly() {
        return new JobTrackerConfigReadOnly(this);
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public boolean isCommunicateStats() {
        return communicateStats;
    }

    public void setCommunicateStats(boolean communicateStats) {
        this.communicateStats = communicateStats;
    }

    public TopologyChangedStrategy getTopologyChangedStrategy() {
        return topologyChangedStrategy;
    }

    public void setTopologyChangedStrategy(TopologyChangedStrategy topologyChangedStrategy) {
        this.topologyChangedStrategy = topologyChangedStrategy;
    }
}
