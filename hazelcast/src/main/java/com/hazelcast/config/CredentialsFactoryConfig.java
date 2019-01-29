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

package com.hazelcast.config;

import com.hazelcast.security.ICredentialsFactory;

import java.util.Properties;

/**
 * Contains the configuration for Credentials Factory.
 */
public class CredentialsFactoryConfig {

    private String className;

    private ICredentialsFactory implementation;

    private Properties properties = new Properties();

    public CredentialsFactoryConfig() {
    }

    public CredentialsFactoryConfig(String className) {
        this.className = className;
    }

    public String getClassName() {
        return className;
    }

    public CredentialsFactoryConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    public ICredentialsFactory getImplementation() {
        return implementation;
    }

    public CredentialsFactoryConfig setImplementation(ICredentialsFactory factoryImpl) {
        this.implementation = factoryImpl;
        return this;
    }

    public Properties getProperties() {
        return properties;
    }

    public CredentialsFactoryConfig setProperties(Properties properties) {
        this.properties = properties;
        return this;
    }

    @Override
    public String toString() {
        return "CredentialsFactoryConfig{"
                + "className='" + className + '\''
                + ", implementation=" + implementation
                + ", properties=" + properties
                + '}';
    }

    @SuppressWarnings({"checkstyle:npathcomplexity"})
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CredentialsFactoryConfig that = (CredentialsFactoryConfig) o;

        if (className != null
                ? !className.equals(that.className)
                : that.className != null) {
            return false;
        }
        if (implementation != null
                ? !implementation.equals(that.implementation)
                : that.implementation != null) {
            return false;
        }
        return properties != null
                ? properties.equals(that.properties)
                : that.properties == null;
    }

    @Override
    public int hashCode() {
        int result = className != null ? className.hashCode() : 0;
        result = 31 * result + (implementation != null ? implementation.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        return result;
    }
}
