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

package com.hazelcast.jet.config;

import com.hazelcast.config.Config;

import java.util.Properties;

/**
 * Configuration object for a Jet instance.
 */
public class JetConfig {

    /**
     * The default port number for the cluster auto-discovery mechanism's
     * multicast communication.
     */
    public static final int DEFAULT_JET_MULTICAST_PORT = 54326;


    private Config hazelcastConfig = defaultHazelcastConfig();
    private InstanceConfig instanceConfig = new InstanceConfig();
    private EdgeConfig defaultEdgeConfig = new EdgeConfig();
    private Properties properties = new Properties();

    /**
     * Returns the configuration object for the underlying Hazelcast instance.
     */
    public Config getHazelcastConfig() {
        return hazelcastConfig;
    }

    /**
     * Sets the underlying IMDG instance's configuration object.
     */
    public JetConfig setHazelcastConfig(Config config) {
        hazelcastConfig = config;
        return this;
    }

    /**
     * Returns the Jet instance config.
     */
    public InstanceConfig getInstanceConfig() {
        return instanceConfig;
    }

    /**
     * Sets the Jet instance config.
     */
    public JetConfig setInstanceConfig(InstanceConfig instanceConfig) {
        this.instanceConfig = instanceConfig;
        return this;
    }

    /**
     * Returns the Jet-specific configuration properties.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the Jet-specific configuration properties.
     */
    public JetConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Returns the default DAG edge configuration.
     */
    public EdgeConfig getDefaultEdgeConfig() {
        return defaultEdgeConfig;
    }

    /**
     * Sets the configuration object that specifies the defaults to use
     * for a DAG edge configuration.
     */
    public JetConfig setDefaultEdgeConfig(EdgeConfig defaultEdgeConfig) {
        this.defaultEdgeConfig = defaultEdgeConfig;
        return this;
    }

    private static Config defaultHazelcastConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setMulticastPort(DEFAULT_JET_MULTICAST_PORT);
        config.getGroupConfig().setName("jet");
        return config;
    }
}
