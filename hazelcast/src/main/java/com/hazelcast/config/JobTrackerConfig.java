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

package com.hazelcast.config;

import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.mapreduce.TopologyChangedStrategy;

/**
 * Contains the configuration for an {@link com.hazelcast.mapreduce.JobTracker}.
 */
public class JobTrackerConfig {

    /**
     * Default size of thread.
     */
    public static final int DEFAULT_MAX_THREAD_SIZE = RuntimeAvailableProcessors.get();
    /**
     * Default value of retry counter.
     */
    public static final int DEFAULT_RETRY_COUNT = 0;
    /**
     * Default value of chunk size.
     */
    public static final int DEFAULT_CHUNK_SIZE = 1000;
    /**
     * Default value of Queue size.
     */
    public static final int DEFAULT_QUEUE_SIZE = 0;
    /**
     * Default boolean value of communication statics.define how a map reduce job behaves
     */
    public static final boolean DEFAULT_COMMUNICATE_STATS = true;
    /**
     * Define how a map reduce job behaves.
     */
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

    /**
     * Sets the name of this {@link com.hazelcast.mapreduce.JobTracker}.
     *
     * @param name the name of the {@link com.hazelcast.mapreduce.JobTracker}
     * @return the current job tracker config instance
     */
    public JobTrackerConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Gets the name of this {@link com.hazelcast.mapreduce.JobTracker}.
     *
     * @return the name of the {@link com.hazelcast.mapreduce.JobTracker}
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the maximum thread pool size of this {@link com.hazelcast.mapreduce.JobTracker}.
     *
     * @return the maximum thread pool size of the {@link com.hazelcast.mapreduce.JobTracker}
     */
    public int getMaxThreadSize() {
        return maxThreadSize;
    }

    /**
     * Sets the maximum thread pool size of this {@link com.hazelcast.mapreduce.JobTracker}.
     *
     * @param maxThreadSize the maximum thread pool size of the {@link com.hazelcast.mapreduce.JobTracker}
     */
    public void setMaxThreadSize(int maxThreadSize) {
        this.maxThreadSize = maxThreadSize;
    }

    /**
     * retry count is currently not used but reserved for later use where the framework will
     * automatically try to restart / retry operations from a available savepoint.
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * retry count is currently not used but reserved for later use where the framework will
     * automatically try to restart / retry operations from a available savepoint.
     */
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * Gets the number of emitted values before a chunk is send to the reducers.
     * If your emitted values are big, you might want to change this to a lower value. If you want
     * to better balance your work, you might want to change this to a higher value.
     * A value of 0 means immediate transmission, but remember that low values mean higher traffic
     * costs.
     * A very high value might cause an OutOfMemoryError to occur if emitted values do not fit into
     * heap memory before being sent to reducers. To prevent this, you might want to use a combiner
     * to pre-reduce values on mapping nodes.
     *
     * @return the number of emitted values before a chunk is sent to the reducers
     */
    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * Sets the number of emitted values before a chunk is send to the reducers.
     * If your emitted values are big, you might want to change this to a lower value. If you want
     * to better balance your work, you might want to change this to a higher value.
     * A value of 0 means immediate transmission, but remember that low values mean higher traffic
     * costs.
     * A very high value might cause an OutOfMemoryError to occur if emitted values do not fit into
     * heap memory before being sent to reducers. To prevent this, you might want to use a combiner
     * to pre-reduce values on mapping nodes.
     *
     * @param chunkSize the number of emitted values before a chunk is sent to the reducers
     */
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public JobTrackerConfig getAsReadOnly() {
        return new JobTrackerConfigReadOnly(this);
    }

    /**
     * Gets the maximum size of the queue; the maximum number of tasks that can wait to be processed. A
     * value of 0 means an unbounded queue.
     *
     * @return the maximum size of the queue
     */
    public int getQueueSize() {
        return queueSize;
    }

    /**
     * Sets the maximum size of the queue; the maximum number of tasks that can wait to be processed. A
     * value of 0 means an unbounded queue.
     *
     * @param queueSize the maximum size of the queue
     */
    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    /**
     * True if statistics (for example, about processed entries)
     * are transmitted to the job emitter, false otherwise. This might be used to show any kind of progress to
     * users inside of UI systems, but this produces additional traffic. If statistics are not needed, you might
     * want to deactivate this.
     *
     * @return {@code true} if statistics (for example, about processed entries) are transmitted to the job emitter,
     * {@code false} otherwise.
     */
    public boolean isCommunicateStats() {
        return communicateStats;
    }

    /**
     * Set to true if statistics (for example, about processed entries)
     * should be transmitted to the job emitter, false otherwise. This might be used to show any kind of progress to
     * users inside of UI systems, but this produces additional traffic. If statistics are not needed, you might
     * want to deactivate this.
     *
     * @param communicateStats {@code true} if statistics (for example, about processed entries) are transmitted to the job
     *                         emitter, {@code false} otherwise.
     */
    public void setCommunicateStats(boolean communicateStats) {
        this.communicateStats = communicateStats;
    }

    /**
     * Gets how the map reduce framework will react on topology
     * changes while executing a job.
     * Currently only CANCEL_RUNNING_OPERATION is fully supported; it throws an exception to
     * the job emitter (throws com.hazelcast.mapreduce.TopologyChangedException).
     *
     * @return How the map reduce framework will react on topology
     * changes while executing a job.
     */
    public TopologyChangedStrategy getTopologyChangedStrategy() {
        return topologyChangedStrategy;
    }

    /**
     * Sets how the map reduce framework will react on topology
     * changes while executing a job.
     * Currently only CANCEL_RUNNING_OPERATION is fully supported; it throws an exception to
     * the job emitter (throws com.hazelcast.mapreduce.TopologyChangedException).
     *
     * @param topologyChangedStrategy How the map reduce framework will react on topology
     *                                changes while executing a job.
     */
    public void setTopologyChangedStrategy(TopologyChangedStrategy topologyChangedStrategy) {
        this.topologyChangedStrategy = topologyChangedStrategy;
    }
}
