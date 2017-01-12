/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet;

import com.hazelcast.config.Config;
import com.hazelcast.jet.config.EdgeConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Javadoc pending
 */
public class JetConfig {

    /**
     * The default value of the {@link #setFlowControlPeriodMs(int) flow-control period}.
     */
    public static final int DEFAULT_FLOW_CONTROL_PERIOD_MS = 100;

    /**
     * The default port number for the cluster auto-discovery mechanism's
     * multicast communication.
     */
    public static final int DEFAULT_JET_MULTICAST_PORT = 54326;

    private Config hazelcastConfig = defaultHazelcastConfig();
    private int threadCount = Runtime.getRuntime().availableProcessors();
    private int flowControlPeriodMs = DEFAULT_FLOW_CONTROL_PERIOD_MS;
    private String workingDirectory;
    private EdgeConfig defaultEdgeConfig = new EdgeConfig();
    private Properties properties = new Properties();

    /**
     * @return the configuration object for the underlying Hazelcast instance
     */
    public Config getHazelcastConfig() {
        return hazelcastConfig;
    }

    /**
     * Sets the underlying Hazelcast instance's configuration object.
     */
    public JetConfig setHazelcastConfig(Config config) {
        hazelcastConfig = config;
        return this;
    }

    /**
     * @return Jet-specific configuration properties
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Sets the Jet-specific configuration properties
     */
    public JetConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Returns the number of cooperative execution threads.
     */
    public int getExecutionThreadCount() {
        return threadCount;
    }

    /**
     * Sets the number of threads each cluster member will use to execute Jet
     * jobs. This refers only to threads executing <em>cooperative</em>
     * processors; each blocking processor is assigned a separate thread.
     */
    public JetConfig setCooperativeThreadCount(int size) {
        this.threadCount = size;
        return this;
    }

    /**
     * @return the working directory used for storing deployed resources
     */
    public String getWorkingDirectory() {
        if (workingDirectory == null) {
            try {
                Path tempDirectory = Files.createTempDirectory("hazelcast-jet");
                tempDirectory.toFile().deleteOnExit();
                workingDirectory = tempDirectory.toString();
            } catch (IOException e) {
                throw rethrow(e);
            }
        }
        return workingDirectory;
    }

    /**
     * Sets the working directory used for storing deployed resources
     */
    public JetConfig setWorkingDirectory(String workingDirectory) {
        this.workingDirectory = workingDirectory;
        return this;
    }

    /**
     * @return the flow-control period in milliseconds
     */
    public int getFlowControlPeriodMs() {
        return flowControlPeriodMs;
    }

    /**
     * While executing a Jet job there is the issue of regulating the rate
     * at which one member of the cluster sends data to another member. The
     * receiver will regularly report to each sender how much more data it
     * is allowed to send over a given DAG edge. This method sets the length
     * (in milliseconds) of the interval between flow-control packets.
     */
    public JetConfig setFlowControlPeriodMs(int flowControlPeriodMs) {
        this.flowControlPeriodMs = flowControlPeriodMs;
        return this;
    }

    /**
     * @return the default DAG edge configuration
     */
    public EdgeConfig getDefaultEdgeConfig() {
        return defaultEdgeConfig;
    }

    /**
     * Sets the configuration object that specifies the defaults to use
     * for DAG edge configuration.
     */
    public JetConfig setDefaultEdgeConfig(EdgeConfig defaultEdgeConfig) {
        this.defaultEdgeConfig = defaultEdgeConfig;
        return this;
    }

    private static Config defaultHazelcastConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setMulticastPort(DEFAULT_JET_MULTICAST_PORT);
        config.getGroupConfig().setName("jet");
        config.getGroupConfig().setPassword("jet-pass");
        return config;
    }

}
