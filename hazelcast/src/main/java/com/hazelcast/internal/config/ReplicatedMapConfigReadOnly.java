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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MergePolicyConfig;
import com.hazelcast.config.ReplicatedMapConfig;

import java.util.List;

public class ReplicatedMapConfigReadOnly extends ReplicatedMapConfig {

    public ReplicatedMapConfigReadOnly(ReplicatedMapConfig replicatedMapConfig) {
        super(replicatedMapConfig);
    }

    @Override
    public ReplicatedMapConfig setName(String name) {
        throw throwReadOnly();
    }

    @Override
    public ReplicatedMapConfig setInMemoryFormat(InMemoryFormat inMemoryFormat) {
        throw throwReadOnly();
    }

    @Override
    public ReplicatedMapConfig setListenerConfigs(List<ListenerConfig> listenerConfigs) {
        throw throwReadOnly();
    }

    @Override
    public ReplicatedMapConfig setAsyncFillup(boolean asyncFillup) {
        throw throwReadOnly();
    }

    @Override
    public ReplicatedMapConfig setStatisticsEnabled(boolean statisticsEnabled) {
        throw throwReadOnly();
    }

    @Override
    public ReplicatedMapConfig setSplitBrainProtectionName(String splitBrainProtectionName) {
        throw throwReadOnly();
    }

    @Override
    public ReplicatedMapConfig setMergePolicyConfig(MergePolicyConfig mergePolicyConfig) {
        throw throwReadOnly();
    }

    private UnsupportedOperationException throwReadOnly() {
        throw new UnsupportedOperationException("This config is read-only");
    }
}
