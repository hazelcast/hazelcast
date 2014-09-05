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
/**
 * Contains the configuration for an {@link com.hazelcast.mapreduce.JobTracker}.
 */
public class JobTrackerConfigReadOnly
        extends JobTrackerConfig {

    JobTrackerConfigReadOnly(JobTrackerConfig jobTrackerConfig) {
        super(jobTrackerConfig);
    }

    @Override
    public JobTrackerConfigReadOnly setName(String name) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setMaxThreadSize(int maxThreadSize) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setRetryCount(int retryCount) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setChunkSize(int chunkSize) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setQueueSize(int queueSize) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setCommunicateStats(boolean communicateStats) {
        throw new UnsupportedOperationException("This config is read-only");
    }

    @Override
    public void setTopologyChangedStrategy(TopologyChangedStrategy topologyChangedStrategy) {
        throw new UnsupportedOperationException("This config is read-only");
    }

}
