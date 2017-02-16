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

package com.hazelcast.config;

import com.hazelcast.core.RingbufferStore;
import com.hazelcast.core.RingbufferStoreFactory;

import java.util.Properties;

import static com.hazelcast.util.Preconditions.isNotNull;

/**
 * Configuration for the {@link RingbufferStore}.
 */
public class RingbufferStoreConfig {

    private boolean enabled = true;
    private String className;
    private String factoryClassName;
    private Properties properties = new Properties();
    private RingbufferStore storeImplementation;
    private RingbufferStoreFactory factoryImplementation;
    private RingbufferStoreConfigReadOnly readOnly;

    public RingbufferStoreConfig() {
    }

    public RingbufferStoreConfig(RingbufferStoreConfig config) {
        enabled = config.isEnabled();
        className = config.getClassName();
        storeImplementation = config.getStoreImplementation();
        factoryClassName = config.getFactoryClassName();
        factoryImplementation = config.getFactoryImplementation();
        properties.putAll(config.getProperties());
    }

    public RingbufferStore getStoreImplementation() {
        return storeImplementation;
    }

    public RingbufferStoreConfig setStoreImplementation(RingbufferStore storeImplementation) {
        this.storeImplementation = storeImplementation;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public RingbufferStoreConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getClassName() {
        return className;
    }

    public RingbufferStoreConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public RingbufferStoreConfig setProperties(Properties properties) {
        this.properties = isNotNull(properties, "properties");
        return this;
    }

    public String getProperty(String name) {
        return properties.getProperty(name);
    }

    public RingbufferStoreConfig setProperty(String name, String value) {
        properties.put(name, value);
        return this;
    }

    public String getFactoryClassName() {
        return factoryClassName;
    }

    public RingbufferStoreConfig setFactoryClassName(String factoryClassName) {
        this.factoryClassName = factoryClassName;
        return this;
    }

    public RingbufferStoreFactory getFactoryImplementation() {
        return factoryImplementation;
    }

    public RingbufferStoreConfig setFactoryImplementation(RingbufferStoreFactory factoryImplementation) {
        this.factoryImplementation = factoryImplementation;
        return this;
    }

    public String toString() {
        return "RingbufferStoreConfig{"
                + "enabled=" + enabled
                + ", className='" + className + '\''
                + ", properties=" + properties
                + '}';
    }

    /**
     * Gets immutable version of this configuration.
     *
     * @return Immutable version of this configuration.
     * @deprecated this method will be removed in 4.0; it is meant for internal usage only.
     */
    public RingbufferStoreConfigReadOnly getAsReadOnly() {
        if (readOnly == null) {
            readOnly = new RingbufferStoreConfigReadOnly(this);
        }
        return readOnly;
    }

    /**
     * A readonly version of the {@link RingbufferStoreConfig}.
     */
    private static class RingbufferStoreConfigReadOnly extends RingbufferStoreConfig {

        RingbufferStoreConfigReadOnly(RingbufferStoreConfig config) {
            super(config);
        }

        @Override
        public RingbufferStoreConfig setStoreImplementation(RingbufferStore storeImplementation) {
            throw new UnsupportedOperationException("This config is read-only.");
        }

        @Override
        public RingbufferStoreConfig setEnabled(boolean enabled) {
            throw new UnsupportedOperationException("This config is read-only.");
        }

        @Override
        public RingbufferStoreConfig setClassName(String className) {
            throw new UnsupportedOperationException("This config is read-only.");
        }

        @Override
        public RingbufferStoreConfig setProperties(Properties properties) {
            throw new UnsupportedOperationException("This config is read-only.");
        }

        @Override
        public RingbufferStoreConfig setProperty(String name, String value) {
            throw new UnsupportedOperationException("This config is read-only.");
        }

        @Override
        public RingbufferStoreConfig setFactoryClassName(String factoryClassName) {
            throw new UnsupportedOperationException("This config is read-only.");
        }

        @Override
        public RingbufferStoreConfig setFactoryImplementation(RingbufferStoreFactory factoryImplementation) {
            throw new UnsupportedOperationException("This config is read-only.");
        }
    }
}
