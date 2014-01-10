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

// TODO retry count needed?
public class JobTrackerConfig {

    public static final int DEFAULT_MAX_THREAD_SIZE = Runtime.getRuntime().availableProcessors();
    public static final int DEFAULT_RETRY_COUNT = 0;
    public static final int DEFAULT_CHUNK_SIZE = 1000;
    public static final int DEFAULT_QUEUE_SIZE = 0;

    private String name;
    private int maxThreadSize;
    private int retryCount;
    private int chunkSize;
    private int queueSize;

    public JobTrackerConfig() {
    }

    public JobTrackerConfig(JobTrackerConfig source) {
        this.name = source.name;
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

}
