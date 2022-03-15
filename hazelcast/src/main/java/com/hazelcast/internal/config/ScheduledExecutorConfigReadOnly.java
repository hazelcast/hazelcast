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

package com.hazelcast.internal.config;

import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.ScheduledExecutorConfig;

public class ScheduledExecutorConfigReadOnly extends ScheduledExecutorConfig {

    public ScheduledExecutorConfigReadOnly(ScheduledExecutorConfig config) {
        super(config);
    }

    @Override
    public ScheduledExecutorConfig setName(String name) {
        throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
    }

    @Override
    public ScheduledExecutorConfig setDurability(int durability) {
        throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
    }

    @Override
    public ScheduledExecutorConfig setPoolSize(int poolSize) {
        throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
    }

    @Override
    public ScheduledExecutorConfig setCapacity(int capacity) {
        throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
    }

    @Override
    public ScheduledExecutorConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
    }

    @Override
    public ScheduledExecutorConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
    }

    @Override
    public ScheduledExecutorConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw new UnsupportedOperationException("This config is read-only scheduled executor: " + getName());
    }
}
