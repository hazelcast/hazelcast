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

package com.hazelcast.client.config;

import com.hazelcast.config.NamedConfig;
import com.hazelcast.config.PartitioningStrategyConfig;

import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 *  The {@code ClientMapConfig} contains the configuration for the client
 *  regarding {@link com.hazelcast.core.HazelcastInstance#getMap(String)
 *  Map}.
 */
public class ClientMapConfig implements NamedConfig {

    private String name;
    private PartitioningStrategyConfig partitioningStrategyConfig;

    public ClientMapConfig() {
    }

    /**
     * Creates a new {@link ClientMapConfig} with the given configuration name for the
     * {@link com.hazelcast.map.IMap} instance. This could be a pattern or an actual instance name.
     * The name must not be modified after this instance is added to the {@link ClientConfig}.
     *
     * @param name the configuration name for the {@link com.hazelcast.map.IMap} instance.
     */
    public ClientMapConfig(String name) {
        setName(name);
    }

    /**
     * Copy constructor.
     *
     * @param config {@link ClientMapConfig} instance to be copied.
     */
    public ClientMapConfig(ClientMapConfig config) {
        this.name = config.getName();
        this.partitioningStrategyConfig = config.partitioningStrategyConfig != null
                ? new PartitioningStrategyConfig(config.getPartitioningStrategyConfig()) : null;
    }

    /**
     * Returns the configuration name for the {@link com.hazelcast.map.IMap} instance.
     *
     * @return the configuration name for the {@link com.hazelcast.map.IMap} instance.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the configuration name for the {@link com.hazelcast.map.IMap} instance.
     * This could be a pattern or an actual instance name. The name must not be
     * modified after this instance is added to the {@link ClientConfig}.
     *
     * @param name the configuration name for the {@link com.hazelcast.map.IMap} instance.
     * @return configured {@link ClientMapConfig} for chaining.
     */
    public ClientMapConfig setName(String name) {
        this.name = checkNotNull(name, "Configuration name cannot be null.");
        return this;
    }

    /**
     * Returns the {@link PartitioningStrategyConfig} instance of this {@link ClientMapConfig}.
     *
     * @return the {@link PartitioningStrategyConfig} instance of this {@link ClientMapConfig}.
     */
    public PartitioningStrategyConfig getPartitioningStrategyConfig() {
        return partitioningStrategyConfig;
    }

    /**
     * Sets the {@link PartitioningStrategyConfig} instance for this {@link ClientMapConfig}.
     *
     * @param partitioningStrategyConfig the {@link PartitioningStrategyConfig} instance for this {@link ClientMapConfig}.
     * @return configured {@link ClientMapConfig} for chaining.
     */
    public ClientMapConfig setPartitioningStrategyConfig(PartitioningStrategyConfig partitioningStrategyConfig) {
        this.partitioningStrategyConfig = partitioningStrategyConfig;
        return this;
    }

    @Override
    public String toString() {
        return "ClientMapConfig{"
                + "name='" + name + '\''
                + ", partitioningStrategyConfig=" + partitioningStrategyConfig
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClientMapConfig that = (ClientMapConfig) o;
        return name.equals(that.name) &&
                Objects.equals(partitioningStrategyConfig, that.partitioningStrategyConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, partitioningStrategyConfig);
    }
}
