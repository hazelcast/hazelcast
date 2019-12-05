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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.SerializationUtil.readList;
import static com.hazelcast.internal.serialization.impl.SerializationUtil.writeCollection;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Configuration for WAN replication. This configuration is referenced from a
 * IMap or ICache configuration to determine the receivers for the WAN events.
 * Each receiver is defined with a either a
 * {@link WanBatchReplicationPublisherConfig} or a {@link CustomWanPublisherConfig}.
 * <p>
 * A single WAN replication configuration may consist of several WAN publisher
 * configurations. The built-in WAN publisher implementation should be
 * configured using {@link WanBatchReplicationPublisherConfig} and custom
 * WAN publisher implementations can be configured using
 * {@link CustomWanPublisherConfig}.
 * <p>
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
 *
 * @see MapConfig#setWanReplicationRef
 * @see CacheConfig#setWanReplicationRef
 */
public class WanReplicationConfig implements IdentifiedDataSerializable {

    private String name;
    private WanConsumerConfig wanConsumerConfig;
    private List<CustomWanPublisherConfig> customPublisherConfigs = new ArrayList<>(0);
    private List<WanBatchReplicationPublisherConfig> batchPublisherConfigs = new ArrayList<>(1);

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
    public WanReplicationConfig setName(@Nonnull String name) {
        this.name = checkNotNull(name, "Name must not be null");
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
     * Returns the list of custom WAN publisher implementations for this WAN
     * replication.
     */
    public @Nonnull
    List<CustomWanPublisherConfig> getCustomPublisherConfigs() {
        return customPublisherConfigs;
    }

    /**
     * Sets the list of custom WAN publisher implementations for this WAN
     * replication.
     *
     * @param customPublisherConfigs publisher configurations
     * @return this config
     */
    public WanReplicationConfig setCustomPublisherConfigs(
            @Nonnull List<CustomWanPublisherConfig> customPublisherConfigs) {
        this.customPublisherConfigs = checkNotNull(customPublisherConfigs, "Publisher configurations must not be null");
        return this;
    }


    /**
     * Returns the list of WAN publisher configurations using the built-in
     * batching WAN implementation for this WAN replication.
     */
    public @Nonnull
    List<WanBatchReplicationPublisherConfig> getBatchPublisherConfigs() {
        return batchPublisherConfigs;
    }

    /**
     * Sets the list of WAN publisher configurations using the built-in
     * batching WAN implementation for this WAN replication.
     *
     * @param batchPublisherConfigs configurations for the built-in WAN publisher implementation
     * @return this config
     */
    public WanReplicationConfig setBatchPublisherConfigs(
            @Nonnull List<WanBatchReplicationPublisherConfig> batchPublisherConfigs) {
        this.batchPublisherConfigs = checkNotNull(batchPublisherConfigs, "Publisher configurations must not be null");
        return this;
    }

    /**
     * Adds a WAN publisher configuration using the built-in batching WAN
     * implementation to this WAN replication.
     *
     * @param config the WAN publisher configuration
     * @return this config
     */
    public WanReplicationConfig addWanBatchReplicationPublisherConfig(WanBatchReplicationPublisherConfig config) {
        this.batchPublisherConfigs.add(config);
        return this;
    }

    /**
     * Adds a custom WAN publisher implementation configuration to this WAN
     * replication.
     *
     * @param config the WAN publisher configuration
     * @return this config
     */
    public WanReplicationConfig addCustomPublisherConfig(CustomWanPublisherConfig config) {
        this.customPublisherConfigs.add(config);
        return this;
    }

    @Override
    public String toString() {
        return "WanReplicationConfig{"
                + "name='" + name + '\''
                + ", wanConsumerConfig=" + wanConsumerConfig
                + ", customPublisherConfigs=" + customPublisherConfigs
                + ", batchPublisherConfigs=" + batchPublisherConfigs
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.WAN_REPLICATION_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(wanConsumerConfig);
        writeCollection(batchPublisherConfigs, out);
        writeCollection(customPublisherConfigs, out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        wanConsumerConfig = in.readObject();

        batchPublisherConfigs = readList(in);
        customPublisherConfigs = readList(in);
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

        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (wanConsumerConfig != null ? !wanConsumerConfig.equals(that.wanConsumerConfig) : that.wanConsumerConfig != null) {
            return false;
        }
        if (!customPublisherConfigs.equals(that.customPublisherConfigs)) {
            return false;
        }
        return batchPublisherConfigs.equals(that.batchPublisherConfigs);
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (wanConsumerConfig != null ? wanConsumerConfig.hashCode() : 0);
        result = 31 * result + customPublisherConfigs.hashCode();
        result = 31 * result + batchPublisherConfigs.hashCode();
        return result;
    }
}
