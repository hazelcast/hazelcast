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

package com.hazelcast.spi.discovery.integration;

import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;

/**
 * The <tt>DiscoveryServiceSettings</tt> class is used to pass the necessary
 * configuration to create a {@link DiscoveryService} to the
 * {@link DiscoveryServiceProvider}. This approach is chosen to have an easily
 * extensible way to provide new configuration properties over time.
 */
public final class DiscoveryServiceSettings {
    private DiscoveryNode discoveryNode;
    private ILogger logger;
    private ClassLoader configClassLoader;
    private DiscoveryConfig discoveryConfig;
    private DiscoveryMode discoveryMode;

    public DiscoveryNode getDiscoveryNode() {
        return discoveryNode;
    }

    public DiscoveryServiceSettings setDiscoveryNode(DiscoveryNode discoveryNode) {
        this.discoveryNode = discoveryNode;
        return this;
    }

    public ILogger getLogger() {
        return logger;
    }

    public DiscoveryServiceSettings setLogger(ILogger logger) {
        this.logger = logger;
        return this;
    }

    public ClassLoader getConfigClassLoader() {
        return configClassLoader;
    }

    public DiscoveryServiceSettings setConfigClassLoader(ClassLoader configClassLoader) {
        this.configClassLoader = configClassLoader;
        return this;
    }

    public DiscoveryConfig getDiscoveryConfig() {
        return discoveryConfig;
    }

    public DiscoveryServiceSettings setDiscoveryConfig(DiscoveryConfig discoveryConfig) {
        this.discoveryConfig = discoveryConfig;
        return this;
    }

    public DiscoveryMode getDiscoveryMode() {
        return discoveryMode;
    }

    public DiscoveryServiceSettings setDiscoveryMode(DiscoveryMode discoveryMode) {
        this.discoveryMode = discoveryMode;
        return this;
    }
}
