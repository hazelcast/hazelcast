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

package com.hazelcast.config;

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.NodeFilter;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkHasText;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * This configuration class describes the top-level config of the discovery
 * SPI and its discovery strategies.
 */
public class DiscoveryConfig implements IdentifiedDataSerializable {

    private List<DiscoveryStrategyConfig> discoveryStrategyConfigs = new ArrayList<>();
    private DiscoveryServiceProvider discoveryServiceProvider;
    private NodeFilter nodeFilter;
    private String nodeFilterClass;

    public DiscoveryConfig() {
    }

    protected DiscoveryConfig(DiscoveryServiceProvider discoveryServiceProvider, NodeFilter nodeFilter, String nodeFilterClass,
                              Collection<DiscoveryStrategyConfig> discoveryStrategyConfigs) {
        this.discoveryServiceProvider = discoveryServiceProvider;
        this.nodeFilter = nodeFilter;
        this.nodeFilterClass = nodeFilterClass;
        this.discoveryStrategyConfigs.addAll(discoveryStrategyConfigs);
    }

    public DiscoveryConfig(DiscoveryConfig discoveryConfig) {
        discoveryStrategyConfigs = new ArrayList<>(discoveryConfig.discoveryStrategyConfigs);
        discoveryServiceProvider = discoveryConfig.discoveryServiceProvider;
        nodeFilter = discoveryConfig.nodeFilter;
        nodeFilterClass = discoveryConfig.nodeFilterClass;
    }

    public DiscoveryConfig setDiscoveryServiceProvider(DiscoveryServiceProvider discoveryServiceProvider) {
        this.discoveryServiceProvider = discoveryServiceProvider;
        return this;
    }

    public DiscoveryServiceProvider getDiscoveryServiceProvider() {
        return discoveryServiceProvider;
    }

    public NodeFilter getNodeFilter() {
        return nodeFilter;
    }

    public DiscoveryConfig setNodeFilter(@Nonnull NodeFilter nodeFilter) {
        this.nodeFilter = checkNotNull(nodeFilter, "Node filter cannot be null!");
        this.nodeFilterClass = null;
        return this;
    }

    public String getNodeFilterClass() {
        return nodeFilterClass;
    }

    public DiscoveryConfig setNodeFilterClass(@Nonnull String nodeFilterClass) {
        this.nodeFilterClass = checkHasText(nodeFilterClass, "Node filter class name must contain text");
        this.nodeFilter = null;
        return this;
    }

    public boolean isEnabled() {
        return discoveryStrategyConfigs.size() > 0;
    }

    /**
     * Returns the defined {@link DiscoveryStrategy} configurations.
     * This collection does not include deactivated configurations since those
     * are automatically skipped while reading the configuration file.
     * <p>
     * All returned configurations are expected to be active, this is to remember
     * when building custom {@link com.hazelcast.config.Config} instances.
     *
     * @return all enabled {@link DiscoveryStrategy} configurations
     */
    public Collection<DiscoveryStrategyConfig> getDiscoveryStrategyConfigs() {
        return discoveryStrategyConfigs;
    }

    /**
     * Sets the strategy configurations for this discovery config.
     *
     * @param discoveryStrategyConfigs the strategy configurations
     * @return this configuration
     */
    public DiscoveryConfig setDiscoveryStrategyConfigs(List<DiscoveryStrategyConfig> discoveryStrategyConfigs) {
        this.discoveryStrategyConfigs = discoveryStrategyConfigs == null
                ? new ArrayList<>(1)
                : discoveryStrategyConfigs;
        return this;
    }

    /**
     * Adds an enabled {@link DiscoveryStrategy} configuration.
     * <p>
     * All added configurations are strictly meant to be enabled, this is to
     * remember when building custom {@link com.hazelcast.config.Config} instances.
     *
     * @param discoveryStrategyConfig the {@link DiscoveryStrategyConfig} to add
     * @return this configuration
     */
    public DiscoveryConfig addDiscoveryStrategyConfig(DiscoveryStrategyConfig discoveryStrategyConfig) {
        discoveryStrategyConfigs.add(discoveryStrategyConfig);
        return this;
    }

    @Override
    public String toString() {
        return "DiscoveryConfig{"
                + "discoveryStrategyConfigs=" + discoveryStrategyConfigs
                + ", discoveryServiceProvider=" + discoveryServiceProvider
                + ", nodeFilter=" + nodeFilter
                + ", nodeFilterClass='" + nodeFilterClass + '\''
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.DISCOVERY_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(discoveryStrategyConfigs);
        out.writeObject(discoveryServiceProvider);
        out.writeObject(nodeFilter);
        out.writeString(nodeFilterClass);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        discoveryStrategyConfigs = in.readObject();
        discoveryServiceProvider = in.readObject();
        nodeFilter = in.readObject();
        nodeFilterClass = in.readString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DiscoveryConfig that = (DiscoveryConfig) o;

        return discoveryStrategyConfigs.equals(that.discoveryStrategyConfigs)
            && Objects.equals(discoveryServiceProvider, that.discoveryServiceProvider)
            && Objects.equals(nodeFilterClass, that.nodeFilterClass)
            && Objects.equals(nodeFilter, that.nodeFilter);
    }

    @Override
    public int hashCode() {
        return Objects.hash(discoveryStrategyConfigs, discoveryServiceProvider, nodeFilterClass, nodeFilter);
    }
}
