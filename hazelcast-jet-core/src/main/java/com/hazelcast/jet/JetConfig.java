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

package com.hazelcast.jet;

import com.hazelcast.config.Config;

import java.util.Properties;

/**
 * Javadoc pending
 */
public class JetConfig {

    private final Config hazelcastConfig;
    private int threadCount = Runtime.getRuntime().availableProcessors();
    private String resourceDirectory;
    private Properties properties = new Properties();

    /**
     * Javadoc pending
     */
    public JetConfig() {
        this(null);
    }

    /**
     * Javadoc pending
     */
    public JetConfig(Config hazelcastConfig) {
        this.hazelcastConfig = hazelcastConfig;
    }

    /**
     * @return Javadoc pending
     */
    public Config getHazelcastConfig() {
        return hazelcastConfig;
    }

    /**
     * @return Javadoc pending
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * @param properties
     * @return
     */
    public JetConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Javadoc pending
     */
    public JetConfig setExecutionThreadCount(int size) {
        this.threadCount = size;
        return this;
    }

    /**
     * Javadoc pending
     */
    public int getExecutionThreadCount() {
        return threadCount;
    }

    /**
     * @return the deployment directory used for storing deployed resources
     */
    public String getResourceDirectory() {
        return resourceDirectory;
    }

    /**
     * Sets the deployment directory used for storing deployed resources
     */
    public JetConfig setResourceDirectory(String resourceDirectory) {
        this.resourceDirectory = resourceDirectory;
        return this;
    }

}
