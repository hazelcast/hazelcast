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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.NodeFilter;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This configuration class describes the top-level config of the discovery
 * SPI and its discovery strategies.
 */
public class DiscoveryConfig implements IdentifiedDataSerializable {

    private List<DiscoveryStrategyConfig> discoveryStrategyConfigs = new ArrayList<DiscoveryStrategyConfig>();
    private DiscoveryServiceProvider discoveryServiceProvider;
    private NodeFilter nodeFilter;
    private String nodeFilterClass;
    private DiscoveryConfig readonly;

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
        discoveryStrategyConfigs = new ArrayList<DiscoveryStrategyConfig>(discoveryConfig.discoveryStrategyConfigs);
        discoveryServiceProvider = discoveryConfig.discoveryServiceProvider;
        nodeFilter = discoveryConfig.nodeFilter;
        nodeFilterClass = discoveryConfig.nodeFilterClass;
        readonly = discoveryConfig.readonly;
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return immutable version of this configuration
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only
     */
    public DiscoveryConfig getAsReadOnly() {
        if (readonly != null) {
            return readonly;
        }
        readonly = new DiscoveryConfigReadOnly(this);
        return readonly;
    }

    public void setDiscoveryServiceProvider(DiscoveryServiceProvider discoveryServiceProvider) {
        this.discoveryServiceProvider = discoveryServiceProvider;
    }

    public DiscoveryServiceProvider getDiscoveryServiceProvider() {
        return discoveryServiceProvider;
    }

    public NodeFilter getNodeFilter() {
        return nodeFilter;
    }

    public void setNodeFilter(NodeFilter nodeFilter) {
        this.nodeFilter = nodeFilter;
    }

    public String getNodeFilterClass() {
        return nodeFilterClass;
    }

    public void setNodeFilterClass(String nodeFilterClass) {
        this.nodeFilterClass = nodeFilterClass;
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
     */
    public void setDiscoveryStrategyConfigs(List<DiscoveryStrategyConfig> discoveryStrategyConfigs) {
        this.discoveryStrategyConfigs = discoveryStrategyConfigs == null
                ? new ArrayList<DiscoveryStrategyConfig>(1)
                : discoveryStrategyConfigs;
    }

    /**
     * Adds an enabled {@link DiscoveryStrategy} configuration.
     * <p>
     * All added configurations are strictly meant to be enabled, this is to
     * remember when building custom {@link com.hazelcast.config.Config} instances.
     *
     * @param discoveryStrategyConfig the {@link DiscoveryStrategyConfig} to add
     */
    public void addDiscoveryStrategyConfig(DiscoveryStrategyConfig discoveryStrategyConfig) {
        discoveryStrategyConfigs.add(discoveryStrategyConfig);
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
        out.writeUTF(nodeFilterClass);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        discoveryStrategyConfigs = in.readObject();
        discoveryServiceProvider = in.readObject();
        nodeFilter = in.readObject();
        nodeFilterClass = in.readUTF();
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DiscoveryConfig that = (DiscoveryConfig) o;

        if (!discoveryStrategyConfigs.equals(that.discoveryStrategyConfigs)) {
            return false;
        }

        if (discoveryServiceProvider != null
                ? !discoveryServiceProvider.equals(that.discoveryServiceProvider)
                : that.discoveryServiceProvider != null) {
            return false;
        }
        if (nodeFilter != null ? !nodeFilter.equals(that.nodeFilter) : that.nodeFilter != null) {
            return false;
        }
        return nodeFilterClass != null ? nodeFilterClass.equals(that.nodeFilterClass) : that.nodeFilterClass == null;
    }

    @Override
    public int hashCode() {
        int result = discoveryStrategyConfigs.hashCode();
        result = 31 * result + (discoveryServiceProvider != null ? discoveryServiceProvider.hashCode() : 0);
        result = 31 * result + (nodeFilter != null ? nodeFilter.hashCode() : 0);
        result = 31 * result + (nodeFilterClass != null ? nodeFilterClass.hashCode() : 0);
        return result;
    }
}
