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

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getBaseName;

/**
 * Extension of Hazelcast member configuration with Jet specific additions.
 */
public class JetConfig extends Config {

    /**
     * Represents default value for any futures in system
     */
    public static final int DEFAULT_SECONDS_TO_AWAIT = 1200;


    private static final ILogger LOGGER = Logger.getLogger(JetConfig.class);

    /**
     * Default port to be used by JET for server listening
     */
    private static final int DEFAULT_PORT = 6701;

    /**
     * Default Input/Output threads count
     */
    private static final int DEFAULT_IO_THREADS_COUNT = 5;

    private final Map<String, ApplicationConfig> appConfigs = new ConcurrentHashMap<>();

    private int ioThreadCount = DEFAULT_IO_THREADS_COUNT;
    private int processingThreadCount;
    private int seconds = DEFAULT_SECONDS_TO_AWAIT;
    private int port = DEFAULT_PORT;

    /**
     * Constructs an empty config
     */
    public JetConfig() {

    }

    /**
     * Gets the configuration for a given application name
     * @param name name of the application
     * @return the configuration for the application
     */
    public ApplicationConfig getApplicationConfig(String name) {
        return lookupConfig(getConfigPatternMatcher(), LOGGER, appConfigs, name);
    }

    /**
     * Sets the configuration for a given application
     * @param config name of the application
     * @return the configuration for the application
     */
    public JetConfig addApplicationConfig(ApplicationConfig config) {
        appConfigs.put(config.getName(), config);
        return this;
    }

    /**
     * Gets the number of processing threads to use
     * @return the number of processing threads to use
     */
    public int getProcessingThreadCount() {
        return processingThreadCount == 0 ? Runtime.getRuntime().availableProcessors() : processingThreadCount;
    }

    /**
     * Sets the number of processing threads to use
     * @param count the number of processing threads
     * @return the current configuration
     */
    public JetConfig setProcessingThreadCount(int count) {
        this.processingThreadCount = count;
        return this;
    }

    /**
     * Gets the number of I/O threads to use
     * @return the number of I/O threads
     */
    public int getIoThreadCount() {
        return ioThreadCount;
    }

    /**
     * Sets the number of I/O threads to use
     * @param count the number of I/O threads
     * @return the current configuration
     */
    public JetConfig setIoThreadCount(int count) {
        this.ioThreadCount = count;
        return this;
    }

    /**
     * Gets the timeout to use when shutting down threads
     * @return the timeout in seconds
     */
    public int getShutdownTimeoutSeconds() {
        return seconds;
    }

    /**
     * Sets the timeout to use when shutting down threads
     * @return the current configuration
     */
    public JetConfig setShutdownTimeoutSeconds(int seconds) {
        this.seconds = seconds;
        return this;
    }

    /**
     * Gets the port which Jet listens on
     * @return the port which Jet listens on
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port which Jet listens on
     * @param port the port which Jet listens on
     * @return the current configuration
     */
    public JetConfig setPort(int port) {
        this.port = port;
        return this;
    }

    static ApplicationConfig lookupConfig(ConfigPatternMatcher matcher, ILogger logger,
                                          Map<String, ApplicationConfig> patterns,
                                          String name) {
        String baseName = getBaseName(name);
        ApplicationConfig candidate = patterns.get(baseName);
        if (candidate != null) {
            return candidate;
        }
        String configPatternKey = matcher.matches(patterns.keySet(), baseName);
        if (configPatternKey != null) {
            return patterns.get(configPatternKey);
        }

        ApplicationConfig defaultConf = patterns.get("default");
        if (defaultConf == null) {
            logger.finest("No configuration found for " + name + ", using system default config!");
            return new ApplicationConfig();
        }
        logger.finest("Using user supplied default config for " + name + ".");
        return defaultConf;
    }
}
