/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for wan replication.
 */
public class WanReplicationConfig {

    private String name;
    private WanConsumerConfig wanConsumerConfig;
    private List<WanPublisherConfig> wanPublisherConfigs = new ArrayList<WanPublisherConfig>(2);

    public String getName() {
        return name;
    }

    public WanReplicationConfig setName(String name) {
        this.name = name;
        return this;
    }

    public WanConsumerConfig getWanConsumerConfig() {
        return wanConsumerConfig;
    }

    public WanReplicationConfig setWanConsumerConfig(WanConsumerConfig wanConsumerConfig) {
        this.wanConsumerConfig = wanConsumerConfig;
        return this;
    }

    public void setWanPublisherConfigs(List<WanPublisherConfig> wanPublisherConfigs) {
        if (wanPublisherConfigs != null
                && !wanPublisherConfigs.isEmpty()) {
            this.wanPublisherConfigs = wanPublisherConfigs;
        }
    }

    public List<WanPublisherConfig> getWanPublisherConfigs() {
        return wanPublisherConfigs;
    }

    public WanReplicationConfig addWanPublisherConfig(WanPublisherConfig wanPublisherConfig) {
        wanPublisherConfigs.add(wanPublisherConfig);
        return this;
    }

    @Override
    public String toString() {
        return "WanReplicationConfig"
                + "{name='" + name + '\''
                + ", wanPublisherConfigs=" + wanPublisherConfigs
                + '}';
    }
}
