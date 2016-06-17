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

package com.hazelcast.jet.config;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.matcher.WildcardConfigPatternMatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Extension of Hazelcast client configuration with Jet specific additions.
 */
public class JetClientConfig extends ClientConfig {

    private static final ILogger LOGGER = Logger.getLogger(JetClientConfig.class);
    private final Map<String, ApplicationConfig> appConfigs = new ConcurrentHashMap<>();
    private ConfigPatternMatcher matcher = new WildcardConfigPatternMatcher();

    /**
     * Constructs an empty config
     */
    public JetClientConfig() {
    }

    /**
     * Gets the configuration for a given application name
     * @param name name of the application
     * @return the configuration for the application
     */
    public ApplicationConfig getApplicationConfig(String name) {
        return JetConfig.lookupConfig(matcher, LOGGER, appConfigs, name);
    }

    /**
     * Sets the configuration for a given application
     *
     * @param config name of the application
     * @return the configuration for the application
     */
    public JetClientConfig addApplicationConfig(ApplicationConfig config) {
        appConfigs.put(config.getName(), config);
        return this;
    }

}
