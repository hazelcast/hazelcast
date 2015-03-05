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

import java.util.Properties;

/**
 * @author mdogan 9/18/12
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

    public Object getServiceImpl() {
        return serviceImpl;
    }

    public ServiceConfig setServiceImpl(final Object serviceImpl) {
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
    public String toString() {
        final StringBuilder sb = new StringBuilder("ServiceConfig{");
        sb.append("enabled=").append(enabled);
        sb.append(", name='").append(name).append('\'');
        sb.append(", className='").append(className).append('\'');
        sb.append(", serviceImpl=").append(serviceImpl);
        sb.append(", properties=").append(properties);
        sb.append('}');
        return sb.toString();
    }
}
