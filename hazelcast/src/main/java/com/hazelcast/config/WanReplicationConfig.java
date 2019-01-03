/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Configuration for WAN replication. This configuration is referenced from an
 * IMap or ICache configuration to determine the receivers for the WAN events.
 * Each receiver is defined with a {@link WanPublisherConfig}.
 * <p>
 * A single WAN replication configuration may consist of several
 * {@link WanPublisherConfig WAN publisher configurations}.
 * You may consider each WAN publisher configuration as a single target cluster
 * or a single external system. The WAN subsystem will track replication for
 * each publisher separately. Having multiple publishers in a single WAN
 * replication config simplifies simultaneous publication of map and cache
 * events to multiple target systems.
 * <p>
 * In addition to defining publishers, you may optionally configure a WAN
 * consumer. The WAN consumer is in charge of consuming (processing) incoming
 * WAN events. Usually when defining a custom consumer you need to define a
 * custom WAN publisher as well.
 * <p>
 * NOTE:
 * Implements Versioned since it serialized WanConsumerConfig directly by
 * invoking writeData. This means that, even though WanConsumerConfig is
 * Versioned, the version will not be injected.
 *
 * @see MapConfig#setWanReplicationRef
 * @see CacheConfig#setWanReplicationRef
 */
public class WanReplicationConfig implements IdentifiedDataSerializable, Versioned {

    private String name;
    private WanConsumerConfig wanConsumerConfig;
    private List<WanPublisherConfig> wanPublisherConfigs = new ArrayList<WanPublisherConfig>(2);

    public String getName() {
        return name;
    }

    /**
     * Sets the name of this WAN replication config. This name is used by the
     * {@link WanReplicationRef} configuration.
     *
     * @param name the WAN replication config name
     * @return this config
     * @see WanReplicationRef#getName()
     */
    public WanReplicationConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Returns the {@link WanConsumerConfig WAN consumer configuration} for this
     * WAN replication. The WAN consumer is in charge of consuming (processing)
     * incoming WAN events.
     */
    public WanConsumerConfig getWanConsumerConfig() {
        return wanConsumerConfig;
    }

    /**
     * Sets the {@link WanConsumerConfig WAN consumer configuration} for this
     * WAN replication. The WAN consumer is in charge of consuming (processing)
     * incoming WAN events.
     *
     * @param wanConsumerConfig the WAN consumer configuration
     * @return this config
     */
    public WanReplicationConfig setWanConsumerConfig(WanConsumerConfig wanConsumerConfig) {
        this.wanConsumerConfig = wanConsumerConfig;
        return this;
    }

    /**
     * Returns the list of configured WAN publisher targets for this WAN
     * replication.
     */
    public List<WanPublisherConfig> getWanPublisherConfigs() {
        return wanPublisherConfigs;
    }

    /**
     * Sets the list of configured WAN publisher targets for this WAN replication.
     *
     * @param wanPublisherConfigs WAN publisher list
     */
    public void setWanPublisherConfigs(List<WanPublisherConfig> wanPublisherConfigs) {
        if (wanPublisherConfigs != null && !wanPublisherConfigs.isEmpty()) {
            this.wanPublisherConfigs = wanPublisherConfigs;
        }
    }

    /**
     * Adds a WAN publisher configuration to this WAN replication.
     *
     * @param wanPublisherConfig the WAN publisher configuration
     * @return this config
     */
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

        // RU_COMPAT_3_11
        if (out.getVersion().isGreaterOrEqual(Versions.V3_12)) {
            // using this method is nicer since the object
            // can implement Versioned and have a version injected
            out.writeObject(wanConsumerConfig);
        } else {
            if (wanConsumerConfig != null) {
                out.writeBoolean(true);
                wanConsumerConfig.writeData(out);
            } else {
                out.writeBoolean(false);
            }
        }

        int publisherCount = wanPublisherConfigs.size();
        out.writeInt(publisherCount);
        for (WanPublisherConfig wanPublisherConfig : wanPublisherConfigs) {
            // RU_COMPAT_3_11
            if (out.getVersion().isGreaterOrEqual(Versions.V3_12)) {
                // using this method is nicer since the object
                // can implement Versioned and have a version injected
                out.writeObject(wanPublisherConfig);
            } else {
                wanPublisherConfig.writeData(out);
            }
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();

        // RU_COMPAT_3_11
        if (in.getVersion().isGreaterOrEqual(Versions.V3_12)) {
            wanConsumerConfig = in.readObject();
        } else {
            boolean consumerConfigExists = in.readBoolean();
            if (consumerConfigExists) {
                WanConsumerConfig consumerConfig = new WanConsumerConfig();
                consumerConfig.readData(in);
                wanConsumerConfig = consumerConfig;
            }
        }

        int publisherCount = in.readInt();
        for (int i = 0; i < publisherCount; i++) {
            WanPublisherConfig publisherConfig;

            // RU_COMPAT_3_11
            if (in.getVersion().isGreaterOrEqual(Versions.V3_12)) {
                publisherConfig = in.readObject();
            } else {
                publisherConfig = new WanPublisherConfig();
                publisherConfig.readData(in);
            }
            wanPublisherConfigs.add(publisherConfig);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WanReplicationConfig that = (WanReplicationConfig) o;

        if (!name.equals(that.name)) {
            return false;
        }
        if (wanConsumerConfig != null
                ? !wanConsumerConfig.equals(that.wanConsumerConfig)
                : that.wanConsumerConfig != null) {
            return false;
        }
        return wanPublisherConfigs.equals(that.wanPublisherConfigs);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (wanConsumerConfig != null ? wanConsumerConfig.hashCode() : 0);
        result = 31 * result + wanPublisherConfigs.hashCode();
        return result;
    }
}
