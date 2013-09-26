/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.query.impl.IndexFactory;

import java.util.Properties;

/**
 * MapIndexFactory configuration
 */
public final class MapIndexFactoryConfig {
    private String factoryClassName = null;
    private IndexFactory factoryImplementation;
    private Properties properties = new Properties();

    public MapIndexFactoryConfig() {
    }

    /**
     * Returns the name of the MapIndexFactory implementation class
     *
     * @return the name of the class
     */
    public String getFactoryClassName() {
        return factoryClassName;
    }

    /**
     * Sets the name for the MapIndexFactory implementation class
     *
     * @param factoryClassName the name of the MapStoreFactory implementation class to set
     */
    public MapIndexFactoryConfig setFactoryClassName(String factoryClassName) {
        this.factoryClassName = factoryClassName;
        return this;
    }

    /**
     * Sets the map index factory implementation object
     *
     * @param factoryImplementation factory implementation object
     * @return this MapIndexFactoryConfig instance
     */
    public MapIndexFactoryConfig setFactoryImplementation(IndexFactory factoryImplementation) {
        this.factoryImplementation = factoryImplementation;
        return this;
    }

    /**
     * Returns the map index factory implementation object
     *
     * @return map index factory implementation object
     */
    public IndexFactory getFactoryImplementation() {
        return factoryImplementation;
    }

    public MapIndexFactoryConfig setProperty(
            String name,
            String value) {
        properties.put(name, value);
        return this;
    }

    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    public Properties getProperties() {
        return properties;
    }

    public MapIndexFactoryConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MapIndexFactoryConfig{");
        sb.append("factoryClassName='").append(factoryClassName).append('\'');
        sb.append(", properties=").append(properties);
        sb.append('}');
        return sb.toString();
    }
}
