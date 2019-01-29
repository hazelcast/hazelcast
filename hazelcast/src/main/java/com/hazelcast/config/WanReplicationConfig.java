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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for WAN replication. This configuration is referenced from a map or cache to determine the receivers for the
 * WAN events. Each receiver is defined with a {@link WanPublisherConfig}
 *
 * @see MapConfig#setWanReplicationRef
 * @see CacheConfig#setWanReplicationRef
 */
public class WanReplicationConfig implements IdentifiedDataSerializable {

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

    public List<WanPublisherConfig> getWanPublisherConfigs() {
        return wanPublisherConfigs;
    }

    public void setWanPublisherConfigs(List<WanPublisherConfig> wanPublisherConfigs) {
        if (wanPublisherConfigs != null && !wanPublisherConfigs.isEmpty()) {
            this.wanPublisherConfigs = wanPublisherConfigs;
        }
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

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ConfigDataSerializerHook.WAN_REPLICATION_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        if (wanConsumerConfig != null) {
            out.writeBoolean(true);
            wanConsumerConfig.writeData(out);
        } else {
            out.writeBoolean(false);
        }
        int publisherCount = wanPublisherConfigs.size();
        out.writeInt(publisherCount);
        for (WanPublisherConfig wanPublisherConfig : wanPublisherConfigs) {
            wanPublisherConfig.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        boolean consumerConfigExists = in.readBoolean();
        if (consumerConfigExists) {
            WanConsumerConfig consumerConfig = new WanConsumerConfig();
            consumerConfig.readData(in);
            wanConsumerConfig = consumerConfig;
        }
        int publisherCount = in.readInt();
        for (int i = 0; i < publisherCount; i++) {
            WanPublisherConfig publisherConfig = new WanPublisherConfig();
            publisherConfig.readData(in);
            wanPublisherConfigs.add(publisherConfig);
        }
    }
}
