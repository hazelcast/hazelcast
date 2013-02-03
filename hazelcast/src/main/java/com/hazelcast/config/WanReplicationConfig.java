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

import java.util.ArrayList;
import java.util.List;

public class WanReplicationConfig {
    String name;
    List<WanTargetClusterConfig> lsTargetClusterConfigs;

    public List<WanTargetClusterConfig> getTargetClusterConfigs() {
        return lsTargetClusterConfigs;
    }

    public WanReplicationConfig addTargetClusterConfig(WanTargetClusterConfig wanTargetClusterConfig) {
        if (lsTargetClusterConfigs == null) {
            lsTargetClusterConfigs = new ArrayList<WanTargetClusterConfig>(2);
        }
        lsTargetClusterConfigs.add(wanTargetClusterConfig);
        return this;
    }

    public void setTargetClusterConfigs(List<WanTargetClusterConfig> list) {
        lsTargetClusterConfigs = list;
    }

    public String getName() {
        return name;
    }

    public WanReplicationConfig setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("WanReplicationConfig");
        sb.append("{name='").append(name).append('\'');
        sb.append(", targetClusterConfigs=").append(lsTargetClusterConfigs);
        sb.append('}');
        return sb.toString();
    }
}
