/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * This configuration class describes a {@link com.hazelcast.spi.discovery.DiscoveryStrategy}
 * based on a parsed XML or configured manually using the config API
 */
public class DiscoveryStrategyConfig {

    private final Map<String, Comparable> properties = new HashMap<String, Comparable>();

    private final String className;
    private final DiscoveryStrategyFactory discoveryStrategyFactory;

    public DiscoveryStrategyConfig(String className) {
        this(className, Collections.<String, Comparable>emptyMap());
    }

    public DiscoveryStrategyConfig(String className, Map<String, Comparable> properties) {
        this.className = className;
        this.properties.putAll(properties);
        this.discoveryStrategyFactory = null;
    }


    public DiscoveryStrategyConfig(DiscoveryStrategyFactory discoveryStrategyFactory) {
        this(discoveryStrategyFactory, Collections.<String, Comparable>emptyMap());
    }

    public DiscoveryStrategyConfig(DiscoveryStrategyFactory discoveryStrategyFactory, Map<String, Comparable> properties) {
        this.className = null;
        this.properties.putAll(properties);
        this.discoveryStrategyFactory = discoveryStrategyFactory;
    }

    public String getClassName() {
        return className;
    }

    public DiscoveryStrategyFactory getDiscoveryStrategyFactory() {
        return discoveryStrategyFactory;
    }

    public void addProperty(String key, Comparable value) {
        properties.put(key, value);
    }

    public void removeProperty(String key) {
        properties.remove(key);
    }

    public Map<String, Comparable> getProperties() {
        return Collections.unmodifiableMap(properties);
    }
}
