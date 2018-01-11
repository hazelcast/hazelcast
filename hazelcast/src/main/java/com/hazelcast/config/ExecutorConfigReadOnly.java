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

/**
 * Configuration for Executor(Read Only)
 *
 * @deprecated this class will be removed in 4.0; it is meant for internal usage only.
 */
public class ExecutorConfigReadOnly extends ExecutorConfig {

    public ExecutorConfigReadOnly(ExecutorConfig config) {
        super(config);
    }

    @Override
    public ExecutorConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only executor: " + getName());
    }

    @Override
    public ExecutorConfig setPoolSize(int poolSize) {
        throw new UnsupportedOperationException("This config is read-only executor: " + getName());
    }

    @Override
    public ExecutorConfig setQueueCapacity(int queueCapacity) {
        throw new UnsupportedOperationException("This config is read-only executor: " + getName());
    }

    @Override
    public ExecutorConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw new UnsupportedOperationException("This config is read-only executor: " + getName());
    }

    @Override
    public ExecutorConfig setQuorumName(String quorumName) {
        throw new UnsupportedOperationException("This config is read-only executor: " + getName());
    }
}
