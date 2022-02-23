/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import java.util.Properties;

/**
 * Configuration for a single service.
 */
public class ServiceConfig {

    private boolean enabled;

    private String name;

    private String className;

    private Object serviceImpl;

    private Properties properties = new Properties();

    private Object configObject;

    public ServiceConfig() {
    }

    public boolean isEnabled() {
        return enabled;
    }

    public ServiceConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getName() {
        return name;
    }

    public ServiceConfig setName(final String name) {
        this.name = name;
        return this;
    }

    public String getClassName() {
        return className;
    }

    public ServiceConfig setClassName(final String className) {
        this.className = className;
        return this;
    }

    /**
     * @since 3.7
     */
    public Object getImplementation() {
        return serviceImpl;
    }

    /**
     * @since 3.7
     */
    public ServiceConfig setImplementation(final Object serviceImpl) {
        this.serviceImpl = serviceImpl;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public ServiceConfig setProperties(final Properties properties) {
        this.properties = properties;
        return this;
    }

    public ServiceConfig addProperty(String propertyName, String value) {
        properties.setProperty(propertyName, value);
        return this;
    }

    public ServiceConfig setConfigObject(Object configObject) {
        this.configObject = configObject;
        return this;
    }

    public Object getConfigObject() {
        return configObject;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof ServiceConfig)) {
            return false;
        }

        ServiceConfig that = (ServiceConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (name != null ? !name.equals(that.name) : that.name != null) {
            return false;
        }
        if (className != null ? !className.equals(that.className) : that.className != null) {
            return false;
        }
        if (serviceImpl != null ? !serviceImpl.equals(that.serviceImpl) : that.serviceImpl != null) {
            return false;
        }
        if (properties != null ? !properties.equals(that.properties) : that.properties != null) {
            return false;
        }
        return configObject != null ? configObject.equals(that.configObject) : that.configObject == null;
    }

    @Override
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (className != null ? className.hashCode() : 0);
        result = 31 * result + (serviceImpl != null ? serviceImpl.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        result = 31 * result + (configObject != null ? configObject.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ServiceConfig{"
                + "enabled=" + enabled
                + ", name='" + name + '\''
                + ", className='" + className + '\''
                + ", implementation=" + serviceImpl
                + ", properties=" + properties
                + '}';
    }
}
